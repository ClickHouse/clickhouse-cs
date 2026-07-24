using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// A single raw connection to a ClickHouse server over the native TCP protocol. Owns the socket, the buffered
/// reader/writer, and the post-handshake state (the negotiated protocol version and server identity).
///
/// <para>
/// Enforces the connection lifecycle — Handshaking → Ready, cycling Ready → ReadingResponse → Ready per
/// exchange, or → Terminated on failure — and the one-in-flight-operation rule: the native protocol has no
/// multiplexing, so a connection carries exactly one request/response at a time. A connection is deliberately
/// <b>not</b> thread-safe; the owner (a pool or a session) must guarantee single-caller access, including for
/// disposal and teardown. Active operations observe cancellation through their I/O token and terminate the
/// connection only after the cancelled I/O has unwound. Any transport or protocol failure terminates the
/// connection, and a terminated connection is never reused.
/// </para>
/// </summary>
internal sealed class ClickHouseTcpConnection : IDisposable, IAsyncDisposable
{
    // The setting a query uses to override the session timezone; its value becomes the presentation timezone
    // for timezone-less DateTime/DateTime64 result columns.
    private const string SessionTimezoneSetting = "session_timezone";

    private readonly Socket socket;
    private readonly Stream stream;
    private readonly ClickHouseBinaryReader reader;
    private readonly ClickHouseBinaryWriter writer;
    private ServerHandshake server;
    private ClientMetadata clientMetadata;
    private TcpConnectionState state;

    /// <summary>
    /// Initializes a connection over an established transport, in the Handshaking state. Production callers
    /// go through <see cref="ConnectAsync"/>; the raw stream/socket seam exists so the handshake and dispatch
    /// logic can be exercised over a scripted stream without a real socket (<paramref name="socket"/> null).
    /// </summary>
    /// <param name="stream">The duplex transport stream (a network stream in production).</param>
    /// <param name="socket">The underlying socket, closed on termination; null when the stream owns teardown.</param>
    internal ClickHouseTcpConnection(Stream stream, Socket socket)
    {
        this.stream = stream;
        this.socket = socket;
        reader = new ClickHouseBinaryReader(stream);
        writer = new ClickHouseBinaryWriter(stream);
        state = TcpConnectionState.Handshaking;
    }

    /// <summary>The current lifecycle state.</summary>
    public TcpConnectionState State => state;

    /// <summary>
    /// Builds the context passed to the codec registry when reading an operation's blocks, carrying the
    /// session timezone so a timezone-bearing column whose type string omits an explicit timezone resolves
    /// against it. A query's <c>session_timezone</c> setting takes precedence over the handshake default. No
    /// connection state is mutated: the context is a value threaded through the read path for one operation.
    /// </summary>
    private ResolveContext ReadContextFor(IReadOnlyDictionary<string, string> settings)
        => new() { ServerTimezone = SessionTimezoneFrom(settings) ?? server.Timezone };

    // The session_timezone value from a query's settings, or null when the query does not set it (so the
    // handshake timezone stands). An empty value is treated as unset.
    private static string SessionTimezoneFrom(IReadOnlyDictionary<string, string> settings)
        => settings is not null && settings.TryGetValue(SessionTimezoneSetting, out string timezone) && !string.IsNullOrEmpty(timezone)
            ? timezone
            : null;

    /// <summary>The server identity and protocol details decoded during the handshake.</summary>
    public ServerHandshake Server => server;

    /// <summary>The protocol version negotiated with the server, and the authority for version-gated fields.</summary>
    public NegotiatedProtocol Protocol => server.Negotiated;

    /// <summary>
    /// Opens a connection: dials the socket, runs the handshake, and returns a connection in the Ready state.
    /// The socket is configured with <c>TCP_NODELAY</c> so message-boundary flushes leave promptly. On any
    /// failure the socket is closed and the exception propagates; no half-open connection is returned.
    ///
    /// <para>
    /// A connect <i>timeout</i> is the caller's responsibility: the OS-level TCP connect can hang far longer
    /// than desired against a host that silently drops packets. Pass a token from a linked
    /// <see cref="System.Threading.CancellationTokenSource"/> with a deadline (the pool/options layer supplies this).
    /// </para>
    /// </summary>
    /// <param name="host">The server host name or address.</param>
    /// <param name="port">The server's native-protocol port (typically 9000).</param>
    /// <param name="handshake">The client-supplied handshake values (identity and credentials).</param>
    /// <param name="cancellationToken">A token to observe for cancellation (and to bound the connect).</param>
    /// <returns>A connected, handshaken connection ready to accept a request.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> or <paramref name="handshake"/> is null.</exception>
    /// <exception cref="SocketException">The socket could not connect to the server.</exception>
    /// <exception cref="ClickHouseServerException">The server rejected the handshake (e.g. authentication failure).</exception>
    /// <exception cref="ClickHouseProtocolException">The server's handshake reply was neither Hello nor Exception.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static async ValueTask<ClickHouseTcpConnection> ConnectAsync(
        string host,
        int port,
        ClientHandshakeParameters handshake,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(handshake);

        // The wire is little-endian and columns are read/written as raw reinterpreted bytes with no byte-swapping,
        // so refuse a big-endian host up front rather than silently mis-decoding every value. .NET has no
        // big-endian runtime target today; this is a guard against a future one, checked once per connect.
        if (!BitConverter.IsLittleEndian)
        {
            throw new PlatformNotSupportedException(
                "The ClickHouse native-protocol client requires a little-endian host: column values are transferred as raw little-endian bytes without byte-swapping.");
        }

        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        // HandshakeAsync terminates the connection (closing this socket) on any failure, so a throw here needs
        // no extra cleanup.
        var connection = new ClickHouseTcpConnection(new NetworkStream(socket, ownsSocket: false), socket);
        await connection.HandshakeAsync(handshake, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>
    /// Sends a Ping and awaits the reply. Returns when the server answers with Pong. A server Exception is
    /// decoded and thrown, leaving the connection reusable (the exception is a complete response). Any other
    /// packet, or a transport failure, terminates the connection.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when Pong is received.</returns>
    /// <exception cref="InvalidOperationException">The connection is busy with another operation.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    /// <exception cref="ClickHouseServerException">The server replied with an Exception.</exception>
    /// <exception cref="ClickHouseProtocolException">The server replied with something other than Pong or Exception.</exception>
    public async ValueTask PingAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        BeginOperation();

        ServerPacketType reply;
        try
        {
            writer.WriteClientPacketType(ClientPacketType.Ping);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            // A Ping is only ever sent on an idle connection, never mid-query, so no Progress or other
            // interleaved packet can precede the reply — unlike a query response, which the read loop drains.
            // A single read therefore suffices; anything but Pong or a (complete) Exception is a violation.
            reply = await reader.ReadServerPacketTypeAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // The failed/cancelled I/O has unwound, but the stream position is unknown; discard the connection.
            Terminate();
            throw;
        }

        switch (reply)
        {
            case ServerPacketType.Pong:
                state = TcpConnectionState.Ready;
                return;

            case ServerPacketType.Exception:
                ClickHouseServerException exception;
                try
                {
                    exception = await ClickHouseServerException.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    Terminate();
                    throw;
                }

                // The Exception is itself a complete response and leaves the stream at a packet boundary, so
                // the connection stays usable.
                state = TcpConnectionState.Ready;
                throw exception;

            default:
                Terminate();
                throw new ClickHouseProtocolException(
                    $"Unexpected packet type {reply} ({(ulong)reply}) in response to Ping; expected Pong or Exception.");
        }
    }

    /// <summary>
    /// Runs a query and streams its result as a sequence of <see cref="Block"/>s. Sends the Query and the
    /// empty end-of-input marker, then drains the response, yielding each row-bearing Data block. The
    /// interleaved metadata packets (Progress, ProfileInfo, ProfileEvents, Log, TableColumns, Totals,
    /// Extremes) are always consumed to keep the stream aligned; supply <paramref name="handlers"/> to
    /// observe them, otherwise their contents are discarded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Blocks are borrowed.</b> Each yielded <see cref="Block"/> is valid only for the current iteration of
    /// the enumeration. The enumerator owns the block's storage and
    /// releases it automatically when you advance to the next block, stop enumerating, or dispose the
    /// enumerator. So do <b>not</b> dispose a yielded block yourself, and do <b>not</b> retain a block, any of
    /// its columns, or an <see cref="IColumn{T}.Values"/> span past the current iteration. To keep data beyond
    /// the loop body, copy it out while iterating (for example <c>((IColumn&lt;ulong&gt;)block[0]).Values.ToArray()</c>).
    /// </para>
    /// <example>
    /// <code>
    /// await foreach (Block block in connection.QueryAsync("SELECT number FROM system.numbers LIMIT 10"))
    /// {
    ///     // Read or copy within the loop body; the block is released once the loop advances.
    ///     foreach (ulong value in ((IColumn&lt;ulong&gt;)block[0]).Values)
    ///     {
    ///         // ...
    ///     }
    /// }
    /// </code>
    /// </example>
    /// </remarks>
    /// <param name="sql">The SQL text.</param>
    /// <param name="settings">Per-query settings as textual values, or null for none.</param>
    /// <param name="parameters">Query parameter values in SQL representation, or null for none.</param>
    /// <param name="queryId">The query id, or null to let the server assign one.</param>
    /// <param name="handlers">Optional callbacks for the interleaved metadata packets, or null to discard them.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of the result's row-bearing blocks, each valid only for its own iteration.</returns>
    /// <exception cref="InvalidOperationException">The connection is busy with another operation.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    /// <exception cref="ClickHouseServerException">The server reported an error while executing the query.</exception>
    /// <exception cref="ClickHouseProtocolException">The server sent an unexpected packet.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    internal async IAsyncEnumerable<Block> QueryAsync(
        string sql,
        IReadOnlyDictionary<string, string> settings = null,
        IReadOnlyDictionary<string, string> parameters = null,
        string queryId = null,
        MetadataHandlers handlers = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);

        // A pre-cancelled query must not transition the connection out of Ready or write anything, leaving it reusable for the next operation.
        cancellationToken.ThrowIfCancellationRequested();
        BeginOperation();

        ResolveContext readContext = ReadContextFor(settings);
        NegotiatedProtocol negotiated = server.Negotiated;
        ClickHouseServerException pending = null;
        Block current = null;
        bool completed = false;

        // Encode the request into the write buffer before any of it reaches the socket. A failure here is a
        // client-side error (e.g. parameters on a protocol revision that predates them): nothing has been sent,
        // so discard the partial packet and leave the connection Ready and reusable rather than terminating it.
        try
        {
            Query.Write(writer, negotiated, clientMetadata, queryId, sql, settings, parameters);
            writer.WriteClientPacketType(ClientPacketType.Data);
            BlockWriter.WriteEmptyBlock(writer);
        }
        catch
        {
            writer.Reset();
            state = TcpConnectionState.Ready;
            throw;
        }

        try
        {
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            while (true)
            {
                // Resuming here means the consumer has advanced past the previously yielded block, so its
                // borrowed (possibly pooled) buffers can be released before we read the next packet.
                if (current is not null)
                {
                    current.Dispose();
                    current = null;
                }

                ServerPacketType packet = await reader.ReadServerPacketTypeAsync(cancellationToken).ConfigureAwait(false);

                if (packet == ServerPacketType.EndOfStream)
                {
                    completed = true;
                    break;
                }

                if (packet == ServerPacketType.Exception)
                {
                    pending = await ClickHouseServerException.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
                    completed = true;
                    break;
                }

                if (packet == ServerPacketType.Data)
                {
                    Block block = await BlockReader.ReadBlockAsync(reader, negotiated, ColumnCodecRegistry.Default, readContext, cancellationToken).ConfigureAwait(false);
                    if (block.RowCount != 0)
                    {
                        // Held as the current block so it is released when the consumer advances or stops.
                        current = block;
                        yield return block;
                    }
                    else
                    {
                        block.Dispose();
                    }
                }
                else
                {
                    // Everything else is interleaved metadata: consumed to stay stream-aligned, surfaced to the
                    // handlers when set. An unexpected packet throws from here.
                    await ConsumeMetadataAsync(packet, negotiated, readContext, handlers, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            // Release the last yielded block (still current) on end-of-stream, early disposal, or error.
            current?.Dispose();

            if (completed)
            {
                state = TcpConnectionState.Ready;
            }
            else
            {
                Terminate();
            }
        }

        if (pending is not null)
        {
            throw pending;
        }
    }

    /// <summary>
    /// The default cap on the rows per wire block (1,000,000), applied alongside the byte target
    /// (<see cref="BlockWriter.DefaultFlushThresholdBytes"/>) so a block closes at whichever it reaches first.
    /// The byte target is the primary, width-invariant bound; this row cap keeps very narrow rows from producing
    /// an unbounded row count in a single block.
    /// </summary>
    public const int DefaultMaxRowsPerBlock = 1_000_000;

    /// <summary>
    /// Runs an INSERT, streaming <paramref name="columns"/> as the row data and returning once the server
    /// acknowledges it.
    /// </summary>
    /// <remarks>
    /// Columns are matched to the target's schema <b>by name</b>: order is free, and naming a subset of the
    /// table's columns in the statement (<c>INSERT INTO t (a, c) VALUES</c>) inserts only those, with the server
    /// filling the rest from their defaults. Values are serialized as the target's resolved type, not the type
    /// the column declares. Zero rows is a no-op INSERT. A mismatch (wrong names, or a CLR type the target
    /// cannot accept) writes nothing and leaves the connection usable before throwing. Large inserts are split
    /// into wire blocks sized to an internal byte target, additionally capped at <paramref name="maxRowsPerBlock"/>
    /// rows per block when set (a block closes at whichever limit it reaches first).
    /// </remarks>
    /// <param name="sql">The <c>INSERT INTO … VALUES</c> statement, with no inline <c>VALUES (...)</c> literal.</param>
    /// <param name="columns">The row data, matched to the target columns by name.</param>
    /// <param name="settings">Per-query settings as textual values, or null for none.</param>
    /// <param name="parameters">Query parameter values in SQL representation, or null for none.</param>
    /// <param name="queryId">The query id, or null to let the server assign one.</param>
    /// <param name="maxRowsPerBlock">A cap on the rows per wire block, applied alongside the internal byte target
    /// (a block closes at whichever it reaches first). Defaults to <see cref="DefaultMaxRowsPerBlock"/>; pass null
    /// for no row cap (byte target only).</param>
    /// <param name="handlers">Optional callbacks for the metadata the server interleaves into the insert
    /// acknowledgement (notably <see cref="MetadataHandlers.OnProgress"/> for rows written and
    /// <see cref="MetadataHandlers.OnProfileEvents"/>), or null to discard it.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server acknowledges the insert with end-of-stream.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> or <paramref name="columns"/> is null.</exception>
    /// <exception cref="ArgumentException">The columns hold differing row counts or duplicate names, their names
    /// do not match the target schema, or a column's CLR type is not writable as its target type.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxRowsPerBlock"/> is zero or negative.</exception>
    /// <exception cref="InvalidOperationException">The connection is busy with another operation.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    /// <exception cref="ClickHouseServerException">The server reported an error while executing the insert.</exception>
    /// <exception cref="ClickHouseProtocolException">The server sent an unexpected packet, or no schema block.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    internal async ValueTask InsertAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        IReadOnlyDictionary<string, string> settings = null,
        IReadOnlyDictionary<string, string> parameters = null,
        string queryId = null,
        int? maxRowsPerBlock = DefaultMaxRowsPerBlock,
        MetadataHandlers handlers = null,
        CancellationToken cancellationToken = default)
    {
        ValidateInsertArguments(sql, columns, maxRowsPerBlock, out int rowCount);

        // Bail on cancellation before claiming the connection, so a pre-cancelled call leaves it idle.
        cancellationToken.ThrowIfCancellationRequested();
        BeginOperation();

        NegotiatedProtocol negotiated = server.Negotiated;
        ClickHouseServerException pending = null;
        bool completed = false;
        string mismatchError = null;
        try
        {
            // The empty end-of-input block must follow the Query: the server waits for it before sending the
            // schema block, so omitting it deadlocks.
            Query.Write(writer, negotiated, clientMetadata, queryId, sql, settings, parameters);
            writer.WriteClientPacketType(ClientPacketType.Data);
            BlockWriter.WriteEmptyBlock(writer);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            // Drain metadata until the schema block (the first Data packet) or a terminal packet. An insert
            // reads only the zero-row schema and acknowledgement blocks, so no display timezone is needed.
            (Block schema, ClickHouseServerException error) = await ReadToNextDataBlockAsync(negotiated, ResolveContext.ForWrite, handlers, cancellationToken).ConfigureAwait(false);
            if (schema is null)
            {
                if (error is null)
                {
                    // Clean end-of-stream with no schema: the server never opened the row-stream phase (e.g.
                    // inline VALUES, or INSERT … SELECT). That breaks the INSERT contract, so terminate rather
                    // than pool a spent connection.
                    throw new ClickHouseProtocolException("The server ended the INSERT response without sending a schema block.");
                }

                // Server Exception instead of the schema: the stream is at a packet boundary, so rethrow once
                // the state is back to Ready.
                pending = error;
                completed = true;
            }
            else
            {
                // Align the caller's columns to the schema by name. No columns is the explicit no-op insert; a
                // mismatch leaves plan null (so only the terminator goes out) and defers the throw until Ready.
                InsertColumn[] plan;
                using (schema)
                {
                    plan = columns.Count == 0
                        ? null
                        : BuildInsertPlan(columns, schema, validateWritable: rowCount > 0, out mismatchError);
                }

                await StreamInsertRowsAsync(plan, rowCount, maxRowsPerBlock, negotiated, cancellationToken).ConfigureAwait(false);

                // Rethrow any server error once the state is back to Ready.
                pending = await DrainToEndOfStreamAsync(negotiated, ResolveContext.ForWrite, handlers, cancellationToken).ConfigureAwait(false);
                completed = true;
            }
        }
        finally
        {
            if (completed)
            {
                state = TcpConnectionState.Ready;
            }
            else
            {
                Terminate();
            }
        }

        if (pending is not null)
        {
            throw pending;
        }

        if (mismatchError is not null)
        {
            throw new ArgumentException(mismatchError, nameof(columns));
        }
    }

    /// <summary>
    /// Validates the arguments that need no server round-trip — non-null inputs, a positive
    /// <paramref name="maxRowsPerBlock"/>, a consistent row count, and unique column names — and reports that row
    /// count. Runs before the connection is claimed, so a malformed call leaves it idle. Name-to-schema
    /// alignment and per-column writability need the sample block and are checked after the query round-trip.
    /// </summary>
    /// <param name="rowCount">Set to the row count every column must share (zero when there are no columns).</param>
    private static void ValidateInsertArguments(
        string sql,
        IReadOnlyList<IColumn> columns,
        int? maxRowsPerBlock,
        out int rowCount)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(columns);
        if (maxRowsPerBlock is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRowsPerBlock), maxRowsPerBlock, "The rows-per-block cap must be positive.");
        }

        rowCount = columns.Count == 0 ? 0 : columns[0].RowCount;
        for (int i = 1; i < columns.Count; i++)
        {
            if (columns[i].RowCount != rowCount)
            {
                throw new ArgumentException(
                    $"All columns must hold the same number of rows; column 0 has {rowCount} but column {i} has {columns[i].RowCount}.",
                    nameof(columns));
            }
        }

        var names = new HashSet<string>(columns.Count, StringComparer.Ordinal);
        foreach (IColumn column in columns)
        {
            if (!names.Add(column.Name))
            {
                throw new ArgumentException(
                    $"Column '{column.Name}' is supplied more than once; column names must be unique.",
                    nameof(columns));
            }
        }
    }

    /// <summary>
    /// Writes the INSERT row stream: the rows as bounded wire blocks (each read straight from the columns'
    /// spans), then the empty terminator that closes it. A null <paramref name="plan"/> or zero
    /// <paramref name="rowCount"/> writes only the terminator — a no-op insert. Trims the writer's pooled buffer
    /// once everything is flushed.
    /// </summary>
    /// <param name="plan">The per-column write plan in schema order, or null to write only the terminator.</param>
    private async ValueTask StreamInsertRowsAsync(
        InsertColumn[] plan,
        int rowCount,
        int? maxRowsPerBlock,
        NegotiatedProtocol negotiated,
        CancellationToken cancellationToken)
    {
        if (rowCount > 0 && plan is not null)
        {
            // Split into wire blocks by row count alone (each column is written straight from its ergonomic form,
            // so there is no intermediate dense buffer to build first). The between-column flush backstop bounds
            // peak client memory while a block streams.
            foreach ((int start, int length) in PlanInsertBlocks(rowCount, maxRowsPerBlock))
            {
                writer.WriteClientPacketType(ClientPacketType.Data);
                await BlockWriter.WriteDataBlockAsync(
                    writer, negotiated, plan, start, length, BlockWriter.DefaultFlushThresholdBytes, cancellationToken).ConfigureAwait(false);
                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        writer.WriteClientPacketType(ClientPacketType.Data);
        BlockWriter.WriteEmptyBlock(writer);
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

        // Return the pooled buffer to baseline so an idle connection doesn't retain a large insert's peak size.
        writer.TrimBuffer();
    }

    /// <summary>
    /// Reads and releases blocks until the response ends, returning the server
    /// <see cref="ClickHouseServerException"/> that terminated it, or null on a clean end-of-stream. Either way
    /// the stream is left at a packet boundary, so the connection stays usable.
    /// </summary>
    /// <param name="negotiated">The negotiated protocol.</param>
    /// <param name="context">The codec-resolution context (timezone) for decoding blocks.</param>
    /// <param name="handlers">Optional metadata callbacks for the interleaved packets, or null to discard them.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The parked server exception, or null if the stream ended cleanly.</returns>
    private async ValueTask<ClickHouseServerException> DrainToEndOfStreamAsync(
        NegotiatedProtocol negotiated,
        ResolveContext context,
        MetadataHandlers handlers,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            (Block block, ClickHouseServerException error) = await ReadToNextDataBlockAsync(negotiated, context, handlers, cancellationToken).ConfigureAwait(false);
            if (block is null)
            {
                return error;
            }

            block.Dispose();
        }
    }

    /// <summary>
    /// Splits <paramref name="rowCount"/> rows into contiguous wire-block ranges of at most
    /// <paramref name="maxRowsPerBlock"/> rows each — the block geometry is bounded by row count alone. With no
    /// <paramref name="maxRowsPerBlock"/> the whole insert is a single block; peak client memory while it is
    /// written is instead bounded by the between-column flush backstop
    /// (<see cref="BlockWriter.DefaultFlushThresholdBytes"/>), not by splitting the block.
    /// </summary>
    /// <param name="rowCount">The number of rows to split (assumed greater than zero).</param>
    /// <param name="maxRowsPerBlock">The cap on the rows per block, or null for a single block.</param>
    /// <returns>The (start, length) row range of each wire block, in order.</returns>
    internal static List<(int Start, int Length)> PlanInsertBlocks(int rowCount, int? maxRowsPerBlock)
    {
        var blocks = new List<(int Start, int Length)>();
        int step = maxRowsPerBlock is int cap && cap > 0 ? cap : rowCount;
        for (int start = 0; start < rowCount; start += step)
        {
            blocks.Add((start, Math.Min(step, rowCount - start)));
        }

        return blocks;
    }

    /// <summary>
    /// Aligns the caller's columns to the schema by name, returning one descriptor per schema column (in schema
    /// order) that pairs the target's name, resolved type, and codec with the caller's values — the caller's own
    /// type string is ignored. With <paramref name="validateWritable"/> set, a value column whose CLR type the
    /// target cannot serialize is rejected. Returns null and sets <paramref name="error"/> on any mismatch.
    /// </summary>
    /// <param name="columns">The caller's value columns; names are unique (validated earlier).</param>
    /// <param name="schema">The server's sample block describing the target columns.</param>
    /// <param name="validateWritable">Whether to confirm each value column is writable as its target type.</param>
    /// <param name="error">Set to a human-readable message on mismatch; null on success.</param>
    /// <returns>The per-column write plan in schema order, or null when <paramref name="error"/> is set.</returns>
    private static InsertColumn[] BuildInsertPlan(IReadOnlyList<IColumn> columns, Block schema, bool validateWritable, out string error)
    {
        error = null;

        var byName = new Dictionary<string, IColumn>(columns.Count, StringComparer.Ordinal);
        foreach (IColumn column in columns)
        {
            byName[column.Name] = column;
        }

        // Align by name: every schema column must be supplied and every supplied column must exist in the schema.
        // Both directions are reported so a wrong column set is actionable, not just a count mismatch.
        var plan = new InsertColumn[schema.ColumnCount];
        List<string> missing = null;
        int matched = 0;
        for (int i = 0; i < schema.ColumnCount; i++)
        {
            IColumn schemaColumn = schema[i];
            if (byName.TryGetValue(schemaColumn.Name, out IColumn value))
            {
                matched++;
                plan[i] = new InsertColumn(schemaColumn.Name, schemaColumn.TypeName, codec: null, value);
            }
            else
            {
                (missing ??= new List<string>()).Add(schemaColumn.Name);
            }
        }

        if (missing is not null || matched != columns.Count)
        {
            error = DescribeSchemaMismatch(columns, schema, missing);
            return null;
        }

        // Names line up one-to-one; resolve each target type to its codec (the target type, not the caller's) and
        // confirm the value column is writable as it. Writing a value uses its own instant, so no server timezone
        // is needed to resolve the codec.
        for (int i = 0; i < plan.Length; i++)
        {
            InsertColumn slot = plan[i];
            IColumnCodec codec;
            try
            {
                codec = ColumnCodecRegistry.Default.Resolve(slot.TypeName, ResolveContext.ForWrite);
            }
            catch (Exception ex) when (ex is NotSupportedException or FormatException)
            {
                error = $"The target column '{slot.Name}' has type '{slot.TypeName}', which this client cannot serialize: {ex.Message}";
                return null;
            }

            if (validateWritable && !codec.CanWrite(slot.Values))
            {
                error = $"Column '{slot.Name}' was given a value column of type {slot.Values.GetType()}, whose CLR element type the target type '{slot.TypeName}' does not accept.";
                return null;
            }

            plan[i] = new InsertColumn(slot.Name, slot.TypeName, codec, slot.Values);
        }

        return plan;
    }

    /// <summary>Composes a message naming the columns the caller failed to supply and the ones it supplied in excess.</summary>
    private static string DescribeSchemaMismatch(IReadOnlyList<IColumn> columns, Block schema, List<string> missing)
    {
        var schemaNames = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < schema.ColumnCount; i++)
        {
            schemaNames.Add(schema[i].Name);
        }

        List<string> unexpected = null;
        foreach (IColumn column in columns)
        {
            if (!schemaNames.Contains(column.Name))
            {
                (unexpected ??= new List<string>()).Add(column.Name);
            }
        }

        var parts = new List<string>(2);
        if (missing is not null)
        {
            parts.Add($"missing column(s) the target requires: {string.Join(", ", missing)}");
        }

        if (unexpected is not null)
        {
            parts.Add($"column(s) not in the target: {string.Join(", ", unexpected)}");
        }

        return $"The insert columns do not match the target schema — {string.Join("; ", parts)}. Columns are matched to the target by name.";
    }

    /// <summary>
    /// Reads a metadata block, lends it to the handler if one is set (borrowed only for the duration of the
    /// call), then releases its storage. A throwing handler propagates after the block has been released.
    /// </summary>
    private static async ValueTask ReadMetadataBlockAsync(
        ClickHouseBinaryReader reader,
        NegotiatedProtocol negotiated,
        ResolveContext context,
        Action<Block> handler,
        CancellationToken cancellationToken)
    {
        Block block = await BlockReader.ReadBlockAsync(reader, negotiated, ColumnCodecRegistry.Default, context, cancellationToken).ConfigureAwait(false);
        try
        {
            handler?.Invoke(block);
        }
        finally
        {
            block.Dispose();
        }
    }

    /// <summary>
    /// Terminates the connection after any active I/O has unwound: marks the state final, closes the transport,
    /// then releases the reader and writer's pooled buffers. Idempotent, but not safe to call concurrently with
    /// another operation. Once terminated a connection is never reused.
    /// </summary>
    public void Terminate()
    {
        if (state == TcpConnectionState.Terminated)
        {
            return;
        }

        state = TcpConnectionState.Terminated;
        try
        {
            // NetworkStream does not own the socket, so close the socket first to abort pending network I/O.
            socket?.Dispose();
        }
        finally
        {
            try
            {
                stream.Dispose();
            }
            finally
            {
                try
                {
                    reader.Dispose();
                }
                finally
                {
                    writer.Dispose();
                }
            }
        }
    }

    /// <inheritdoc/>
    public void Dispose() => Terminate();

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Terminate();
        return default;
    }

    /// <summary>
    /// Runs the handshake exchange over the transport and transitions Handshaking → Ready. Any failure
    /// (protocol violation, transport error, cancellation) terminates the connection before propagating, so
    /// the "any failure ⇒ Terminated, never reused" contract holds regardless of who invokes the handshake.
    /// </summary>
    /// <param name="handshake">The client-supplied handshake values.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the handshake succeeds.</returns>
    internal async ValueTask HandshakeAsync(ClientHandshakeParameters handshake, CancellationToken cancellationToken)
    {
        switch (state)
        {
            case TcpConnectionState.Handshaking:
                break;

            case TcpConnectionState.Terminated:
                throw new ObjectDisposedException(nameof(ClickHouseTcpConnection), "The connection has been terminated and cannot be reused.");

            default:
                throw new InvalidOperationException($"The connection cannot start a handshake while in state {state}.");
        }

        try
        {
            server = await Handshake.PerformAsync(reader, writer, handshake, cancellationToken).ConfigureAwait(false);

            // Retain only the non-secret query metadata; the full parameters (and the plaintext password they
            // carry) go out of scope with this method rather than living for the connection's lifetime.
            clientMetadata = ClientMetadata.FromHandshake(handshake);
        }
        catch
        {
            Terminate();
            throw;
        }

        state = TcpConnectionState.Ready;
    }

    /// <summary>
    /// Reads packets, consuming interleaved metadata, until the next Data block or a terminal packet. Returns
    /// the decoded block (the caller owns and must dispose it), or a null block when the stream ended — with the
    /// server Exception attached when the end was an <see cref="ServerPacketType.Exception"/>, or a null
    /// exception on a clean <see cref="ServerPacketType.EndOfStream"/>. A read failure propagates; the caller
    /// terminates the connection.
    /// </summary>
    /// <param name="negotiated">The negotiated protocol, for version-gated fields.</param>
    /// <param name="context">The codec-resolution context (timezone) for decoding blocks.</param>
    /// <param name="handlers">Optional metadata callbacks for the interleaved packets, or null to discard them.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The next Data block, or a null block plus the parked terminal exception (if any).</returns>
    private async ValueTask<(Block block, ClickHouseServerException error)> ReadToNextDataBlockAsync(
        NegotiatedProtocol negotiated,
        ResolveContext context,
        MetadataHandlers handlers,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            ServerPacketType packet = await reader.ReadServerPacketTypeAsync(cancellationToken).ConfigureAwait(false);
            switch (packet)
            {
                case ServerPacketType.EndOfStream:
                    return (null, null);

                case ServerPacketType.Exception:
                    return (null, await ClickHouseServerException.ReadAsync(reader, cancellationToken).ConfigureAwait(false));

                case ServerPacketType.Data:
                    return (await BlockReader.ReadBlockAsync(reader, negotiated, ColumnCodecRegistry.Default, context, cancellationToken).ConfigureAwait(false), null);

                default:
                    await ConsumeMetadataAsync(packet, negotiated, context, handlers, cancellationToken).ConfigureAwait(false);
                    break;
            }
        }
    }

    /// <summary>
    /// Consumes one interleaved metadata packet to keep the stream aligned, handing it to the matching callback
    /// in <paramref name="handlers"/> when one is set and discarding it otherwise. Shared by the query and insert
    /// response drains. Any packet type not valid mid-response at this protocol target is a violation.
    /// </summary>
    /// <param name="packet">The packet type just read (never Data, Exception, or EndOfStream).</param>
    /// <param name="negotiated">The negotiated protocol, for version-gated fields.</param>
    /// <param name="context">The codec-resolution context (timezone) for decoding block-bearing packets.</param>
    /// <param name="handlers">Optional metadata callbacks, or null to discard every packet.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <exception cref="ClickHouseProtocolException"><paramref name="packet"/> is not a valid interleaved packet.</exception>
    private async ValueTask ConsumeMetadataAsync(
        ServerPacketType packet,
        NegotiatedProtocol negotiated,
        ResolveContext context,
        MetadataHandlers handlers,
        CancellationToken cancellationToken)
    {
        switch (packet)
        {
            // Block-bearing packets lend the borrowed block to the handler for the call, then release it.
            case ServerPacketType.Totals:
                await ReadMetadataBlockAsync(reader, negotiated, context, handlers?.OnTotals, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.Extremes:
                await ReadMetadataBlockAsync(reader, negotiated, context, handlers?.OnExtremes, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.ProfileEvents:
                await ReadMetadataBlockAsync(reader, negotiated, context, handlers?.OnProfileEvents, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.Log:
                await ReadMetadataBlockAsync(reader, negotiated, context, handlers?.OnLog, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.Progress:
            {
                Progress progress = await Progress.ReadAsync(reader, negotiated, cancellationToken).ConfigureAwait(false);
                handlers?.OnProgress?.Invoke(progress);
                break;
            }

            case ServerPacketType.ProfileInfo:
            {
                ProfileInfo profileInfo = await ProfileInfo.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
                handlers?.OnProfileInfo?.Invoke(profileInfo);
                break;
            }

            case ServerPacketType.TableColumns:
                // Column-defaults metadata the server may send before the schema block; decoded to stay aligned
                // and discarded (no result surface yet).
                await TableColumns.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.PartUUIDs:
                // Valid when part-level deduplication is active; consumed to stay aligned. TODO: surface.
                await PartUUIDs.ConsumeAsync(reader, cancellationToken).ConfigureAwait(false);
                break;

            default:
                // Anything else (e.g. TimezoneUpdate, a read-task request) is not valid interleaved in a query or
                // insert response at this protocol target.
                throw new ClickHouseProtocolException($"Unexpected packet type {packet} ({(ulong)packet}) in server response.");
        }
    }

    /// <summary>
    /// Claims the connection for a single request/response exchange, enforcing the one-in-flight rule.
    /// Transitions Ready → ReadingResponse; rejects a busy or terminated connection.
    /// </summary>
    /// <exception cref="InvalidOperationException">Another operation is already in flight.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    private void BeginOperation()
    {
        switch (state)
        {
            case TcpConnectionState.Ready:
                state = TcpConnectionState.ReadingResponse;
                return;

            case TcpConnectionState.Terminated:
                throw new ObjectDisposedException(nameof(ClickHouseTcpConnection), "The connection has been terminated and cannot be reused.");

            default:
                throw new InvalidOperationException(
                    $"The connection is busy ({state}); a single connection carries one in-flight operation at a time.");
        }
    }
}
