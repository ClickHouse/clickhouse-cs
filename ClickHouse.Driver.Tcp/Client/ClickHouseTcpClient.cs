using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A high-level client for a ClickHouse server over the native TCP protocol: run queries and stream results,
/// execute statements, and insert columnar data. Build one from a <see cref="ClickHouseTcpClientOptions"/> or a
/// connection string and reuse it — it is safe to share across threads. Operations are serialized onto the
/// underlying connection today (one in flight at a time); a future connection pool lifts that transparently.
///
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </summary>
[Experimental("CHTCP0001")]
public sealed class ClickHouseTcpClient : IAsyncDisposable
{
    // Reading and writing Dynamic (and later JSON) requires the flattened native serialization; the client
    // enables it on every operation so callers never have to know about it. A caller-supplied value wins.
    private const string FlattenedSerializationSetting = "output_format_native_use_flattened_dynamic_and_json_serialization";

    private readonly IConnectionSource source;
    private readonly IReadOnlyDictionary<string, string> baseSettings;
    private readonly int maxSendBufferBytes;

    /// <summary>Creates a client from options.</summary>
    /// <param name="options">The client configuration (endpoint, credentials, timeouts, client-level settings).</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">An option value is invalid (see <see cref="ClickHouseTcpClientOptions"/>).</exception>
    public ClickHouseTcpClient(ClickHouseTcpClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        source = new SingleConnectionSource(options);
        baseSettings = options.CustomSettings;
        maxSendBufferBytes = options.MaxSendBufferBytes;
    }

    /// <summary>Creates a client from a connection string.</summary>
    /// <param name="connectionString">The connection string (keys such as <c>Host</c>, <c>Port</c>, <c>Username</c>, <c>set_&lt;name&gt;</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
    /// <exception cref="ArgumentException">A resulting option value is invalid.</exception>
    public ClickHouseTcpClient(string connectionString)
        : this(ClickHouseTcpClientOptions.FromConnectionString(connectionString))
    {
    }

    /// <summary>Test/pool seam: builds a client over an arbitrary connection source and client-level settings.</summary>
    internal ClickHouseTcpClient(
        IConnectionSource source,
        IReadOnlyDictionary<string, string> baseSettings,
        int maxSendBufferBytes = ClickHouseTcpClientOptions.DefaultMaxSendBufferBytes)
    {
        this.source = source;
        this.baseSettings = baseSettings;
        this.maxSendBufferBytes = maxSendBufferBytes;
    }

    /// <summary>
    /// Runs a query and streams its result as a sequence of <see cref="Block"/>s — the low-level columnar tier,
    /// with no per-row materialization.
    /// </summary>
    /// <remarks>
    /// <b>Blocks are borrowed.</b> Each yielded <see cref="Block"/> is valid only for the current iteration: the
    /// enumerator releases its storage when you advance, stop enumerating, or dispose the enumerator. Do not
    /// dispose a yielded block yourself, and do not retain a block, any of its columns, or an
    /// <see cref="IColumn{T}.Values"/> span past the current iteration — copy out (e.g. <c>Values.ToArray()</c>)
    /// what must outlive the loop body. Enumerate with <c>await foreach</c> (or otherwise dispose the enumerator)
    /// so the underlying connection is returned: reused for the next operation when the response was fully
    /// drained, or discarded and redialed when enumeration stopped mid-response.
    /// </remarks>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of the result's row-bearing blocks, each valid only for its own iteration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    public async IAsyncEnumerable<Block> StreamAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);

        IReadOnlyDictionary<string, string> settings = BuildSettings(options);
        string queryId = options?.QueryId;

        IConnectionLease lease = await source.RentAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // The connection's own enumerator owns each block's storage and, in its finally, returns the
            // connection to Ready or terminates it. We pass the blocks straight through without disposing them.
            await foreach (Block block in lease.Connection
                .QueryAsync(sql, settings, parameters: null, queryId, handlers: null, cancellationToken)
                .ConfigureAwait(false))
            {
                yield return block;
            }
        }
        finally
        {
            // Runs on natural completion, early break / enumerator disposal (which cascades disposal into the
            // inner iterator so its finally runs first), and exceptions. Disposing the lease returns the
            // connection to the source exactly once; the source reuses it if Ready or redials if terminated.
            await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Runs a query and streams its result one row at a time as <c>object[]</c>, each entry the boxed value of a
    /// column in header order (pair with <see cref="Block.ColumnNames"/> — via <see cref="StreamAsync"/> — to
    /// address by name). Each returned array is owned and safe to retain past the enumeration.
    /// </summary>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of result rows.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    public async IAsyncEnumerable<object[]> QueryAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (Block block in StreamAsync(sql, options, cancellationToken).ConfigureAwait(false))
        {
            int columnCount = block.ColumnCount;
            for (int row = 0; row < block.RowCount; row++)
            {
                var values = new object[columnCount];
                for (int column = 0; column < columnCount; column++)
                {
                    values[column] = block[column].GetValue(row);
                }

                yield return values;
            }
        }
    }

    /// <summary>
    /// Runs a statement that produces no result rows (DDL, or DML other than an <c>INSERT ... VALUES</c>) and
    /// returns once the server acknowledges it. Any result blocks the statement happens to produce are drained
    /// and discarded.
    /// </summary>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the statement is acknowledged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    public async ValueTask ExecuteAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        await foreach (Block _ in StreamAsync(sql, options, cancellationToken).ConfigureAwait(false))
        {
            // Draining to completion is the acknowledgement; the connection is released by StreamAsync.
        }
    }

    /// <summary>
    /// Inserts columnar data. The columns are matched to the target's schema <b>by name</b> (order is free, and
    /// a named subset inserts only those columns, the server filling the rest from their defaults); values are
    /// serialized as the target's resolved type. Zero rows is a no-op.
    /// </summary>
    /// <param name="sql">The <c>INSERT INTO … VALUES</c> statement, with no inline <c>VALUES (...)</c> literal.</param>
    /// <param name="columns">The row data, matched to the target columns by name.</param>
    /// <param name="maxRowsPerBlock">A cap on the rows per wire block, applied alongside the internal byte target; null for no row cap.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server acknowledges the insert.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> or <paramref name="columns"/> is null.</exception>
    /// <exception cref="ArgumentException">The columns' row counts differ, names are not unique, do not match the target schema, or a CLR type is not writable as its target type.</exception>
    public async ValueTask InsertAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        int? maxRowsPerBlock = ClickHouseTcpConnection.DefaultMaxRowsPerBlock,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(columns);

        IReadOnlyDictionary<string, string> settings = BuildSettings(options);
        string queryId = options?.QueryId;

        await using IConnectionLease lease = await source.RentAsync(cancellationToken).ConfigureAwait(false);
        await lease.Connection.InsertAsync(
            sql, columns, settings, parameters: null, queryId, maxRowsPerBlock, maxSendBufferBytes, handlers: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Checks connectivity by sending a Ping and awaiting the Pong.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server answers.</returns>
    public async ValueTask PingAsync(CancellationToken cancellationToken = default)
    {
        await using IConnectionLease lease = await source.RentAsync(cancellationToken).ConfigureAwait(false);
        await lease.Connection.PingAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => source.DisposeAsync();

    private IReadOnlyDictionary<string, string> BuildSettings(ClickHouseTcpQueryOptions options)
        => MergeSettings(baseSettings, options?.Settings);

    /// <summary>
    /// Merges the settings for one operation: the client-level custom settings, overlaid by the per-query
    /// settings, with the flattened-serialization setting injected unless a caller already set it at either
    /// level.
    /// </summary>
    /// <param name="clientSettings">The client-level custom settings, or null for none.</param>
    /// <param name="perQuerySettings">The per-query settings that override the client-level ones, or null for none.</param>
    /// <returns>The merged settings to send with the operation.</returns>
    internal static IReadOnlyDictionary<string, string> MergeSettings(
        IReadOnlyDictionary<string, string> clientSettings,
        IReadOnlyDictionary<string, string> perQuerySettings)
    {
        var merged = new Dictionary<string, string>(StringComparer.Ordinal);
        if (clientSettings is not null)
        {
            foreach (KeyValuePair<string, string> entry in clientSettings)
            {
                merged[entry.Key] = entry.Value;
            }
        }

        if (perQuerySettings is not null)
        {
            foreach (KeyValuePair<string, string> entry in perQuerySettings)
            {
                merged[entry.Key] = entry.Value;
            }
        }

        if (!merged.ContainsKey(FlattenedSerializationSetting))
        {
            merged[FlattenedSerializationSetting] = "1";
        }

        return merged;
    }
}
