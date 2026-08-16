using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Parameters;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A high-level client for a ClickHouse server over the native TCP protocol: run queries and stream results,
/// execute statements, and insert data columnwise or row by row. Build one from a <see cref="ClickHouseTcpClientOptions"/> or a
/// connection string and reuse it — it is safe to share across threads, and meant to be shared: it owns a
/// connection pool, so operations run concurrently up to <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/>
/// and queue beyond it.
///
/// <para>
/// <b>Dispose it.</b> A client holds open connections, and while any lifetime or idle limit is set it also runs
/// a timer to close the idle ones — and a running timer keeps the whole pool reachable, so a client dropped
/// without being disposed keeps its sockets open for the life of the process rather than being collected. Build
/// one per endpoint and keep it, rather than one per operation. Disposal closes the idle connections at once
/// and waits up to <see cref="ClickHouseTcpClientOptions.PoolTimeout"/> for the operations still running, after
/// which it aborts them.
/// </para>
///
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </summary>
[Experimental("CHTCP0001")]
public sealed class ClickHouseTcpClient : IClickHouseTcpClient
{
    // Reading and writing Dynamic requires the flattened native serialization; the client enables it on every
    // operation so callers never have to know about it. A caller-supplied value wins.
    private const string FlattenedSerializationSetting = "output_format_native_use_flattened_dynamic_and_json_serialization";

    // How many rows QueryAsync<T> materializes at once. A whole block would otherwise be alive together, and a
    // block is server-sized: 65,409 rows by default, more if a caller raises max_block_size. Bounding it keeps the
    // rows a streaming consumer drops inside gen0 instead of promoting them.
    //
    // Measured flat from 64 to 4096 rows and slower outside that, so this sits mid-plateau rather than at a
    // measured optimum. Erring small on purpose: the cost of a smaller window is a per-window cast and loop setup
    // per column, which did not register even on a two-column row of fixed-width values, while the cost of too
    // large a window is the promotion this is here to avoid, against a gen0 budget that varies by host and GC mode.
    private const int MaterializationWindowRows = 256;

    // Reading JSON requires the server to send it as text rather than one column per path, which is the only
    // encoding this client decodes. Enabled on every operation for the same reason, and likewise overridable.
    // Writes need no setting: the client always writes the String version and the server reads whichever the
    // prefix declares.
    private const string JsonAsStringSetting = "output_format_native_write_json_as_string";

    private static readonly ClickHouseTcpInsertOptions DefaultInsertOptions = new();

    private readonly IConnectionSource source;

    // Per-client, as HTTP's registry is: the client is meant to be a singleton, so the reflection and the compiles
    // are amortized anyway. It does hold its Type keys and compiled delegates strongly, so it pins a collectible
    // AssemblyLoadContext for as long as the client is reachable; scoping it to the client is what makes disposing
    // the client release them, rather than holding them for the process. See PocoTypeRegistry.
    private readonly PocoTypeRegistry pocoTypes = new();

    /// <summary>Creates a client from options.</summary>
    /// <param name="options">The client configuration (endpoint, credentials, timeouts, client-level settings).</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">An option value is invalid (see <see cref="ClickHouseTcpClientOptions"/>).</exception>
    public ClickHouseTcpClient(ClickHouseTcpClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        Options = options.WithOwnedCustomSettings();
        source = new ConnectionPool(Options);
    }

    /// <summary>Creates a client from a connection string.</summary>
    /// <param name="connectionString">The connection string (keys such as <c>Host</c>, <c>Port</c>, <c>Username</c>, <c>set_&lt;name&gt;</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
    /// <exception cref="ArgumentException">A resulting option value is invalid.</exception>
    public ClickHouseTcpClient(string connectionString)
        : this(ClickHouseTcpClientOptions.FromConnectionString(connectionString))
    {
    }

    /// <summary>Test/pool seam: builds a client over an arbitrary connection source.</summary>
    internal ClickHouseTcpClient(IConnectionSource source, ClickHouseTcpClientOptions options = null)
    {
        this.source = source;
        Options = (options ?? new ClickHouseTcpClientOptions()).WithOwnedCustomSettings();
    }

    /// <summary>
    /// The configuration this client was built with, including the client-level settings applied to every
    /// operation. Init-only, so it reflects construction and never changes.
    /// </summary>
    public ClickHouseTcpClientOptions Options { get; }

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
    /// <param name="options">Per-query options (query id, settings, parameters), or null for the client defaults.</param>
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
        IReadOnlyDictionary<string, string> parameters = BuildParameters(sql, options);
        string queryId = options?.QueryId;

        IConnectionLease lease = await source.RentAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // The connection's own enumerator owns each block's storage and, in its finally, returns the
            // connection to Ready or terminates it. We pass the blocks straight through without disposing them.
            await foreach (Block block in lease.Connection
                .QueryAsync(sql, settings, parameters, queryId, handlers: null, cancellationToken)
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
    /// <remarks>
    /// <c>LowCardinality</c> values may be shared within a block. In particular, do not mutate a
    /// <c>LowCardinality(FixedString(N))</c> <c>byte[]</c> in place because another row may reference it.
    /// </remarks>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings, parameters), or null for the client defaults.</param>
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
    /// Runs a query and streams its result one row at a time as <typeparamref name="T"/>, filling each property
    /// from the column of the same name. Matching ignores case, and then underscores, so a <c>user_id</c> column
    /// reaches a <c>UserId</c> property; <c>[ClickHouseTcpColumn]</c> renames a property and
    /// <c>[ClickHouseTcpNotMapped]</c> excludes one. A column no property maps to is skipped, and a property no
    /// column maps to keeps its default.
    /// </summary>
    /// <remarks>
    /// Rows own their values and remain valid after enumeration advances. <c>LowCardinality</c> elements may still
    /// be shared within a block; do not mutate an array-valued property in place. <typeparamref name="T"/> must be
    /// concrete with a public parameterless constructor; every property reached by a result column must have a
    /// public, non-init setter. Mapping is validated on the first block, so an empty result yields nothing without
    /// validating <typeparamref name="T"/>.
    /// </remarks>
    /// <typeparam name="T">The row type.</typeparam>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings, parameters), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of result rows.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped to the result: it has
    /// nothing to map or cannot be constructed, no column maps to a property, or a column cannot be read as its
    /// property's type.</exception>
    public async IAsyncEnumerable<T> QueryAsync<T>(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        PocoReadPlan<T> plan = null;
        long produced = 0;
        await foreach (Block block in StreamAsync(sql, options, cancellationToken).ConfigureAwait(false))
        {
            // The blocks of one result share a header, so the plan is resolved once per result in practice; the
            // check also covers the header changing mid-enumeration, which the cache then serves.
            if (plan is null || !plan.MatchesHeader(block))
            {
                plan = pocoTypes.ReadPlanFor<T>(block, forcedTier: null);
            }

            T[] rows = ArrayPool<T>.Shared.Rent(Math.Min(MaterializationWindowRows, block.RowCount));
            try
            {
                // Strides by the constant rather than by the rented length, so the loop advances even for an empty
                // block and cannot depend on how much the pool handed back.
                for (int start = 0; start < block.RowCount; start += MaterializationWindowRows)
                {
                    // Materializing in one synchronous call is what lets the scatters hold a span: an iterator
                    // method cannot. The rows are handed out afterwards, when no span is live.
                    int count = Math.Min(MaterializationWindowRows, block.RowCount - start);
                    plan.Materialize(block, rows, start, count, produced + start);
                    for (int i = 0; i < count; i++)
                    {
                        yield return rows[i];
                    }
                }

                produced += block.RowCount;
            }
            finally
            {
                // Cleared, so the pool does not keep the rows it just handed to the caller alive.
                ArrayPool<T>.Shared.Return(rows, clearArray: true);
            }
        }
    }

    /// <summary>
    /// Runs a statement that produces no result rows (DDL, or DML other than an <c>INSERT ... VALUES</c>) and
    /// returns once the server acknowledges it. Any result blocks the statement happens to produce are drained
    /// and discarded.
    /// </summary>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings, parameters), or null for the client defaults.</param>
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
    /// <param name="options">Per-insert options (query id, settings, parameters, block sizing), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server acknowledges the insert.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> or <paramref name="columns"/> is null.</exception>
    /// <exception cref="ArgumentException">The columns' row counts differ, names are not unique, do not match the target schema, or a CLR type is not writable as its target type.</exception>
    public async ValueTask InsertAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(columns);

        IReadOnlyDictionary<string, string> settings = BuildSettings(options);
        IReadOnlyDictionary<string, string> parameters = BuildParameters(sql, options);

        await using IConnectionLease lease = await source.RentAsync(cancellationToken).ConfigureAwait(false);
        await lease.Connection.InsertAsync(
            sql,
            columns,
            settings,
            parameters,
            options?.QueryId,
            ResolveMaxRowsPerBlock(options),
            Options.MaxSendBufferBytes,
            handlers: null,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask InsertRowsAsync<T>(
        string sql,
        IReadOnlyList<T> rows,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(rows);

        // Prevent column lists from being mistaken for POCO rows.
        if (typeof(IColumn).IsAssignableFrom(typeof(T)))
        {
            throw new ArgumentException(
                $"An insert of {typeof(T).Name} rows would map that type's properties to columns. To insert columnar data, call InsertAsync with an IReadOnlyList<IColumn> — materialize the sequence (for example with ToList()) if it is not already a list.",
                nameof(rows));
        }

        IReadOnlyDictionary<string, string> settings = BuildSettings(options);
        IReadOnlyDictionary<string, string> parameters = BuildParameters(sql, options);

        using var buffer = PocoRowBuffer<T>.Create(rows, nameof(rows), cancellationToken);

        await using IConnectionLease lease = await source.RentAsync(cancellationToken).ConfigureAwait(false);
        await lease.Connection.InsertAsync(
            sql,
            buffer.Count,
            schema => pocoTypes.WritePlanFor<T>(schema).BuildColumns(buffer.Rows, buffer.Count),
            settings,
            parameters,
            options?.QueryId,
            ResolveMaxRowsPerBlock(options),
            Options.MaxSendBufferBytes,
            handlers: null,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask InsertRowsAsync(
        string sql,
        IReadOnlyList<object[]> rows,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(rows);

        IReadOnlyDictionary<string, string> settings = BuildSettings(options);
        IReadOnlyDictionary<string, string> parameters = BuildParameters(sql, options);
        using var buffer = PocoRowBuffer<object[]>.Create(rows, nameof(rows), cancellationToken);

        await using IConnectionLease lease = await source.RentAsync(cancellationToken).ConfigureAwait(false);
        await lease.Connection.InsertAsync(
            sql,
            buffer.Count,
            schema => UntypedRowColumns.Build(schema, buffer.Rows, buffer.Count),
            settings,
            parameters,
            options?.QueryId,
            ResolveMaxRowsPerBlock(options),
            Options.MaxSendBufferBytes,
            handlers: null,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The row cap for an insert: the caller's value, or the default when they passed no options at all.
    /// </summary>
    /// <param name="options">The per-insert options, or null for the client defaults.</param>
    /// <returns>The rows-per-block cap, or null to write a single block.</returns>
    /// <remarks>
    /// Reads through a default instance rather than coalescing, because null is a meaningful value here: an
    /// explicit <see cref="ClickHouseTcpInsertOptions.MaxRowsPerBlock"/> of null asks for one block, so a
    /// <c>?? Default</c> would silently re-enable splitting.
    /// </remarks>
    internal static int? ResolveMaxRowsPerBlock(ClickHouseTcpInsertOptions options)
        => (options ?? DefaultInsertOptions).MaxRowsPerBlock;

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
        => MergeSettings(Options.CustomSettings, options?.Settings);

    /// <summary>
    /// Resolves each bound parameter to the wire text for the Query packet's parameter list.
    /// </summary>
    /// <param name="sql">The SQL text, scanned for the <c>{name:Type}</c> placeholders that give the types.</param>
    /// <param name="options">The per-query options carrying the parameters, or null for none.</param>
    /// <returns>The formatted parameters by name, or null when none are bound.</returns>
    /// <remarks>
    /// The type each value is formatted as comes from, in order: the parameter's own
    /// <see cref="ClickHouseTcpParameter.ClickHouseType"/>, the query's <c>{name:Type}</c> placeholder, then the
    /// value's CLR type. The last rung only carries a parameter the query does not name, because a query that
    /// does name it must declare the type for the server to read.
    /// </remarks>
    internal static IReadOnlyDictionary<string, string> BuildParameters(string sql, ClickHouseTcpQueryOptions options)
    {
        ClickHouseTcpParameterCollection parameters = options?.Parameters;
        if (parameters is null || parameters.Count == 0)
        {
            return null;
        }

        Dictionary<string, string> hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);
        var formatted = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (ClickHouseTcpParameter parameter in parameters)
        {
            object value = parameter.Value;
            string typeName = parameter.ClickHouseType;
            if (string.IsNullOrWhiteSpace(typeName) && !hints.TryGetValue(parameter.Name, out typeName))
            {
                // Inference reads the sequence to find its element type, and formatting reads it again. A
                // sequence that can only be read once (a LINQ chain, an iterator with side effects) would come
                // up empty the second time, so take a copy before the first read.
                value = Materialize(value);
                typeName = ParameterTypeInference.Infer(value, parameter.Name);
            }

            formatted[parameter.Name] = TcpParameterFormatter.Format(value, typeName, parameter.Name);
        }

        return formatted;
    }

    /// <summary>Copies a sequence that may only be readable once, so it can be read twice.</summary>
    /// <param name="value">The parameter value.</param>
    /// <returns>The value, or a copy of it when it is a sequence with no known count.</returns>
    /// <remarks>
    /// A string is a sequence but is read as one value, and anything with a count (an array, a list, a
    /// dictionary) is already re-readable, so neither is copied.
    /// </remarks>
    private static object Materialize(object value)
    {
        if (value is string || value is System.Collections.ICollection || value is not System.Collections.IEnumerable sequence)
        {
            return value;
        }

        var copy = new List<object>();
        foreach (object element in sequence)
        {
            copy.Add(element);
        }

        return copy;
    }

    /// <summary>
    /// Merges the settings for one operation: the client-level custom settings, overlaid by the per-query
    /// settings, with the flattened-serialization and JSON-as-string settings injected unless a caller already set
    /// them at either level.
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
                // Per-query settings are user-provided and, unlike client CustomSettings, not validated at
                // construction. An empty name would collide with the empty key that terminates the wire settings
                // list, and a null value cannot be written — reject both rather than corrupt the request.
                if (string.IsNullOrEmpty(entry.Key))
                {
                    throw new ArgumentException("A query setting name must not be null or empty.", nameof(perQuerySettings));
                }

                if (entry.Value is null)
                {
                    throw new ArgumentException($"Query setting '{entry.Key}' has a null value; use an empty string for a flag-style setting.", nameof(perQuerySettings));
                }

                merged[entry.Key] = entry.Value;
            }
        }

        if (!merged.ContainsKey(FlattenedSerializationSetting))
        {
            merged[FlattenedSerializationSetting] = "1";
        }

        if (!merged.ContainsKey(JsonAsStringSetting))
        {
            merged[JsonAsStringSetting] = "1";
        }

        return merged;
    }
}
