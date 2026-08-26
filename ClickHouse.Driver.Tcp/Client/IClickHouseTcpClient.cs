using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The contract of a ClickHouse client that speaks the native TCP protocol: run queries and stream results,
/// execute statements, and insert columnar data. <see cref="ClickHouseTcpClient"/> is the implementation; code
/// against this interface to substitute a test double.
///
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </summary>
[Experimental("CHTCP0001")]
public interface IClickHouseTcpClient : IAsyncDisposable
{
    /// <summary>
    /// The configuration this client was built with, including the client-level settings applied to every
    /// operation.
    /// </summary>
    ClickHouseTcpClientOptions Options { get; }

    /// <summary>
    /// Runs a query and streams its result as a sequence of <see cref="Block"/>s — the low-level columnar tier,
    /// with no per-row materialization.
    /// </summary>
    /// <remarks>
    /// <b>Blocks are borrowed.</b> Each yielded <see cref="Block"/> is valid only for the current iteration, and
    /// the consumer must not dispose one or retain it, its columns, or an <see cref="IColumn{T}.Values"/> span
    /// past that point — copy out what must outlive the loop body. Enumerate with <c>await foreach</c> (or
    /// otherwise dispose the enumerator) so the underlying connection is released. An implementation must honor
    /// this contract.
    /// </remarks>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of the result's row-bearing blocks, each valid only for its own iteration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    IAsyncEnumerable<Block> StreamAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs a query and streams its result one row at a time as <c>object[]</c>, each entry the boxed value of a
    /// column in header order. Each returned array is owned and safe to retain past the enumeration.
    /// </summary>
    /// <remarks>
    /// <c>LowCardinality</c> values may be shared within a block; array-valued entries must not be mutated in place.
    /// </remarks>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of result rows.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    IAsyncEnumerable<object[]> QueryAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs a query and streams its result one row at a time as <typeparamref name="T"/>, filling each property
    /// from the column of the same name (ignoring case, and then underscores). A column no property maps to is
    /// skipped, and a property no column maps to keeps its default.
    /// </summary>
    /// <remarks>
    /// Rows remain valid after enumeration advances. Element instances may still be shared where the column's
    /// representation does; see <see cref="ClickHouseTcpClient.QueryAsync{T}"/>.
    /// </remarks>
    /// <typeparam name="T">The row type.</typeparam>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of result rows.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be mapped to the result.</exception>
    IAsyncEnumerable<T> QueryAsync<T>(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Runs a statement that produces no result rows (DDL, or DML other than an <c>INSERT ... VALUES</c>) and
    /// returns once the server acknowledges it. Any result blocks are drained and discarded.
    /// </summary>
    /// <param name="sql">The SQL text.</param>
    /// <param name="options">Per-query options (query id, settings), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the statement is acknowledged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> is null.</exception>
    ValueTask ExecuteAsync(
        string sql,
        ClickHouseTcpQueryOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts columnar data. The columns are matched to the target's schema <b>by name</b> (order is free, and a
    /// named subset inserts only those columns, the server filling the rest from their defaults); values are
    /// serialized as the target's resolved type. Zero rows is a no-op.
    /// </summary>
    /// <param name="sql">The <c>INSERT INTO … VALUES</c> statement, with no inline <c>VALUES (...)</c> literal.</param>
    /// <param name="columns">The row data, matched to the target columns by name.</param>
    /// <param name="options">Per-insert options (query id, settings, block sizing), or null for the client defaults.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server acknowledges the insert.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> or <paramref name="columns"/> is null.</exception>
    /// <exception cref="ArgumentException">The columns' row counts differ, names are not unique, do not match the target schema, or a CLR type is not writable as its target type.</exception>
    ValueTask InsertAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        ClickHouseTcpInsertOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Checks connectivity by sending a Ping and awaiting the Pong.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server answers.</returns>
    ValueTask PingAsync(CancellationToken cancellationToken = default);
}
