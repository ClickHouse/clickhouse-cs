using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Builds row-oriented columns after the server supplies the INSERT schema. The insert owns the returned columns.
/// </summary>
/// <param name="schema">The server's sample block, naming and typing the target columns. Valid only for the call.</param>
/// <returns>The columns to insert, matched to the target by name.</returns>
internal delegate IReadOnlyList<IColumn> InsertColumnFactory(Block schema);

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
    /// Whether this connection is fit to carry another operation, as far as can be told without sending
    /// anything. Ready, with a transport the peer has not closed and no bytes left over from the last response.
    /// The pool asks at both ends of every lease, so it must stay cheap: one non-blocking poll of the socket.
    /// </summary>
    /// <remarks>
    /// A readable idle socket means one of two things, and neither allows reuse: the peer closed and a zero-byte read
    /// is pending, or bytes the last operation did not consume are waiting, which means this side's idea of the stream
    /// position no longer matches the server's.
    ///
    /// <para>
    /// Under TLS the poll is one step further from the truth, in both directions. Decrypted bytes held inside the
    /// <c>SslStream</c> are invisible to it, because a TLS record is read whole while a caller may ask for less;
    /// and a record that carries no application data, such as a late session ticket, makes the socket readable and
    /// so discards a healthy connection. Neither has been seen against ClickHouse, and both fail safe — the first
    /// is caught by the next read being out of step, the second only costs a reconnect.
    /// </para>
    ///
    /// <para>
    /// This detects only a peer that closed in an orderly way. A connection dropped without a FIN, by a partition or a
    /// machine that lost power, still looks alive here, and the operation sent over it stalls until TCP itself gives
    /// up, which on Linux takes about fifteen minutes. That is inherent to a client-side check, so the pool does not
    /// rely on this alone: it also refuses a connection that has sat idle past <c>IdleTimeout</c>, which covers the
    /// common case of an intermediary dropping a connection nobody was using. Neither catches a drop that strikes a
    /// connection in active use. The answer to that is an idle read deadline rather than a stricter probe, and
    /// <b>that deadline does not exist yet</b>: <c>ReadTimeout</c> is parsed and stored but nothing enforces it, so a
    /// caller's own <see cref="System.Threading.CancellationToken"/> is currently the only bound on such a stall.
    /// </para>
    /// </remarks>
    internal bool IsReusable
    {
        get
        {
            if (state != TcpConnectionState.Ready)
            {
                return false;
            }

            // Bytes already buffered from the socket are invisible to a poll, so check our own buffer first.
            if (reader.BufferedBytes != 0)
            {
                return false;
            }

            // The scripted-stream seam has no socket; there is nothing to poll, so trust the state.
            if (socket is null)
            {
                return true;
            }

            try
            {
                return !socket.Poll(0, SelectMode.SelectRead);
            }
            catch (Exception e) when (e is SocketException or ObjectDisposedException)
            {
                return false;
            }
        }
    }

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
    /// Opens a connection: dials the socket, negotiates TLS when asked, runs the handshake, and returns a
    /// connection in the Ready state. The socket is configured with <c>TCP_NODELAY</c> so message-boundary
    /// flushes leave promptly. On any failure the socket is closed and the exception propagates; no half-open
    /// connection is returned.
    ///
    /// <para>
    /// A connect <i>timeout</i> is the caller's responsibility: the OS-level TCP connect can hang far longer
    /// than desired against a host that silently drops packets. Pass a token from a linked
    /// <see cref="System.Threading.CancellationTokenSource"/> with a deadline (the pool/options layer supplies this).
    /// </para>
    /// </summary>
    /// <param name="host">The server host name or address.</param>
    /// <param name="port">The server's native-protocol port (typically 9000, or 9440 with TLS).</param>
    /// <param name="handshake">The client-supplied handshake values (identity and credentials).</param>
    /// <param name="tls">How to negotiate TLS before the handshake, or null to run in the clear.</param>
    /// <param name="cancellationToken">A token to observe for cancellation (and to bound the connect).</param>
    /// <returns>A connected, handshaken connection ready to accept a request.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> or <paramref name="handshake"/> is null.</exception>
    /// <exception cref="SocketException">The socket could not connect to the server.</exception>
    /// <exception cref="System.Security.Authentication.AuthenticationException">The TLS handshake failed (certificate rejected, or the port is not a TLS port).</exception>
    /// <exception cref="ClickHouseServerException">The server rejected the handshake (e.g. authentication failure).</exception>
    /// <exception cref="ClickHouseProtocolException">The server's handshake reply was neither Hello nor Exception.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static async ValueTask<ClickHouseTcpConnection> ConnectAsync(
        string host,
        int port,
        ClientHandshakeParameters handshake,
        TlsParameters tls,
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
        Stream transport;
        try
        {
            await socket.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);

            // TLS is negotiated here, before a single protocol byte is written: the native client Hello that follows
            // carries the password in plaintext, so it must already be inside the encrypted stream.
            transport = new NetworkStream(socket, ownsSocket: false);
            if (tls is not null)
            {
                transport = await tls.WrapAsync(transport, cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        // HandshakeAsync terminates the connection (closing this socket) on any failure, so a throw here needs
        // no extra cleanup.
        var connection = new ClickHouseTcpConnection(transport, socket);
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
    /// The default cap on the rows per wire block (1,000,000). Block geometry is bounded by row count alone, so
    /// this cap is what splits a large insert into bounded blocks. Peak buffered bytes while a block is written
    /// are bounded separately by the between-column flush backstop
    /// (<see cref="BlockWriter.DefaultFlushThresholdBytes"/>), which flushes mid-block rather than closing it.
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
    /// into wire blocks of at most <paramref name="maxRowsPerBlock"/> rows each — row count is the only bound on
    /// block geometry — or written as a single block when the cap is null.
    /// </remarks>
    /// <param name="sql">The <c>INSERT INTO … VALUES</c> statement, with no inline <c>VALUES (...)</c> literal.</param>
    /// <param name="columns">The row data, matched to the target columns by name.</param>
    /// <param name="settings">Per-query settings as textual values, or null for none.</param>
    /// <param name="parameters">Query parameter values in SQL representation, or null for none.</param>
    /// <param name="queryId">The query id, or null to let the server assign one.</param>
    /// <param name="maxRowsPerBlock">A cap on the rows per wire block — the only bound on block geometry.
    /// Defaults to <see cref="DefaultMaxRowsPerBlock"/>; pass null to write the whole insert as a single block.
    /// Peak buffered bytes are bounded separately by <paramref name="maxSendBufferBytes"/>.</param>
    /// <param name="maxSendBufferBytes">The buffered-byte cap that triggers a between-column flush while a block is
    /// written — the write memory backstop bounding peak client memory during a large insert (a single column
    /// larger than the cap still buffers in full). Independent of the row-based block split. Defaults to
    /// <see cref="BlockWriter.DefaultFlushThresholdBytes"/>.</param>
    /// <param name="handlers">Optional callbacks for the metadata the server interleaves into the insert
    /// acknowledgement (notably <see cref="MetadataHandlers.OnProgress"/> for rows written and
    /// <see cref="MetadataHandlers.OnProfileEvents"/>), or null to discard it.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server acknowledges the insert with end-of-stream.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> or <paramref name="columns"/> is null.</exception>
    /// <exception cref="ArgumentException">The columns hold differing row counts or duplicate names, their names
    /// do not match the target schema, or a column's CLR type is not writable as its target type.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxRowsPerBlock"/> is zero or negative, or <paramref name="maxSendBufferBytes"/> is not positive.</exception>
    /// <exception cref="InvalidOperationException">The connection is busy with another operation.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    /// <exception cref="ClickHouseServerException">The server reported an error while executing the insert.</exception>
    /// <exception cref="ClickHouseProtocolException">The server sent an unexpected packet, or no schema block.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    internal ValueTask InsertAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        IReadOnlyDictionary<string, string> settings = null,
        IReadOnlyDictionary<string, string> parameters = null,
        string queryId = null,
        int? maxRowsPerBlock = DefaultMaxRowsPerBlock,
        int maxSendBufferBytes = BlockWriter.DefaultFlushThresholdBytes,
        MetadataHandlers handlers = null,
        CancellationToken cancellationToken = default)
    {
        ValidateInsertArguments(sql, columns, maxRowsPerBlock, maxSendBufferBytes, out int rowCount);
        return InsertCoreAsync(sql, columns, buildColumns: null, rowCount, settings, parameters, queryId, maxRowsPerBlock, maxSendBufferBytes, handlers, cancellationToken);
    }

    /// <summary>
    /// Runs an INSERT whose columns are built from the server's sample block.
    /// </summary>
    /// <remarks>
    /// The returned columns are disposed after writing. A factory failure sends no rows and leaves the connection
    /// reusable.
    /// </remarks>
    internal ValueTask InsertAsync(
        string sql,
        int rowCount,
        InsertColumnFactory buildColumns,
        IReadOnlyDictionary<string, string> settings = null,
        IReadOnlyDictionary<string, string> parameters = null,
        string queryId = null,
        int? maxRowsPerBlock = DefaultMaxRowsPerBlock,
        int maxSendBufferBytes = BlockWriter.DefaultFlushThresholdBytes,
        MetadataHandlers handlers = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(buildColumns);
        if (rowCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(rowCount), rowCount, "The row count must not be negative.");
        }

        ValidateInsertGeometry(maxRowsPerBlock, maxSendBufferBytes);
        return InsertCoreAsync(sql, columns: null, buildColumns, rowCount, settings, parameters, queryId, maxRowsPerBlock, maxSendBufferBytes, handlers, cancellationToken);
    }

    /// <summary>
    /// Runs the shared INSERT flow. Factory-built columns are owned here; caller-supplied columns are not.
    /// </summary>
    private async ValueTask InsertCoreAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        InsertColumnFactory buildColumns,
        int rowCount,
        IReadOnlyDictionary<string, string> settings,
        IReadOnlyDictionary<string, string> parameters,
        string queryId,
        int? maxRowsPerBlock,
        int maxSendBufferBytes,
        MetadataHandlers handlers,
        CancellationToken cancellationToken)
    {
        // Bail on cancellation before claiming the connection, so a pre-cancelled call leaves it idle.
        cancellationToken.ThrowIfCancellationRequested();
        BeginOperation();

        NegotiatedProtocol negotiated = server.Negotiated;
        // Decode metadata blocks with the operation's session timezone.
        ResolveContext readContext = ReadContextFor(settings);
        ClickHouseServerException pending = null;
        Exception buildFailure = null;
        IReadOnlyList<IColumn> values = null;
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

            // Drain metadata until the schema block (the first Data packet) or a terminal packet.
            (Block schema, ClickHouseServerException error) = await ReadToNextDataBlockAsync(negotiated, readContext, handlers, cancellationToken).ConfigureAwait(false);
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
                InsertColumn[] plan = null;
                using (schema)
                {
                    if (buildColumns is null)
                    {
                        values = columns;
                    }
                    else
                    {
                        // Defer caller errors until the row stream is closed cleanly.
                        try
                        {
                            values = buildColumns(schema);
                        }
                        catch (Exception failure)
                        {
                            buildFailure = failure;
                        }
                    }

                    if (buildFailure is null)
                    {
                        plan = values.Count == 0
                            ? null
                            : BuildInsertPlan(values, schema, validateWritable: rowCount > 0, out mismatchError);
                    }
                }

                await StreamInsertRowsAsync(plan, rowCount, maxRowsPerBlock, maxSendBufferBytes, negotiated, cancellationToken).ConfigureAwait(false);

                // Rethrow any server error once the state is back to Ready.
                pending = await DrainToEndOfStreamAsync(negotiated, readContext, handlers, cancellationToken).ConfigureAwait(false);
                completed = true;
            }
        }
        finally
        {
            // Only the factory's columns are ours to release; a caller's own columns outlive the insert.
            if (buildColumns is not null && values is not null)
            {
                for (int i = 0; i < values.Count; i++)
                {
                    values[i]?.Dispose();
                }
            }

            if (completed)
            {
                state = TcpConnectionState.Ready;
            }
            else
            {
                Terminate();
            }
        }

        // Prefer the build failure over the server's response to the resulting empty insert.
        if (buildFailure is not null)
        {
            ExceptionDispatchInfo.Capture(buildFailure).Throw();
        }

        if (pending is not null)
        {
            throw pending;
        }

        if (mismatchError is not null)
        {
            throw new ArgumentException(mismatchError, buildColumns is null ? nameof(columns) : nameof(buildColumns));
        }
    }

    /// <summary>
    /// Validates caller-supplied columns before claiming the connection.
    /// </summary>
    /// <param name="rowCount">Set to the row count every column must share (zero when there are no columns).</param>
    private static void ValidateInsertArguments(
        string sql,
        IReadOnlyList<IColumn> columns,
        int? maxRowsPerBlock,
        int maxSendBufferBytes,
        out int rowCount)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(columns);
        ValidateInsertGeometry(maxRowsPerBlock, maxSendBufferBytes);

        rowCount = 0;
        for (int i = 0; i < columns.Count; i++)
        {
            // Reject null before reading its row count or name.
            IColumn column = columns[i]
                ?? throw new ArgumentException($"Column at index {i} is null; every supplied column must be non-null.", nameof(columns));

            if (i == 0)
            {
                rowCount = column.RowCount;
            }
            else if (column.RowCount != rowCount)
            {
                throw new ArgumentException(
                    $"All columns must hold the same number of rows; column 0 has {rowCount} but column {i} has {column.RowCount}.",
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

    /// <summary>Validates block limits before claiming the connection.</summary>
    /// <param name="maxRowsPerBlock">The cap on the rows per wire block, or null for a single block.</param>
    /// <param name="maxSendBufferBytes">The buffered-byte cap that triggers a between-column flush.</param>
    private static void ValidateInsertGeometry(int? maxRowsPerBlock, int maxSendBufferBytes)
    {
        if (maxRowsPerBlock is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRowsPerBlock), maxRowsPerBlock, "The rows-per-block cap must be positive.");
        }

        if (maxSendBufferBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxSendBufferBytes), maxSendBufferBytes, "The send-buffer cap must be positive.");
        }
    }

    /// <summary>
    /// Writes the row blocks and terminating empty block. A null plan or zero rows writes only the terminator.
    /// </summary>
    /// <param name="plan">The per-column write plan in schema order, or null to write only the terminator.</param>
    /// <param name="flushThresholdBytes">The buffered-byte cap that triggers a between-column flush while a block is written.</param>
    private async ValueTask StreamInsertRowsAsync(
        InsertColumn[] plan,
        int rowCount,
        int? maxRowsPerBlock,
        int flushThresholdBytes,
        NegotiatedProtocol negotiated,
        CancellationToken cancellationToken)
    {
        if (rowCount > 0 && plan is not null)
        {
            // Row count controls block splitting; the flush threshold bounds buffered output within each block.
            foreach ((int start, int length) in PlanInsertBlocks(rowCount, maxRowsPerBlock))
            {
                writer.WriteClientPacketType(ClientPacketType.Data);
                await BlockWriter.WriteDataBlockAsync(
                    writer, negotiated, plan, start, length, flushThresholdBytes, cancellationToken).ConfigureAwait(false);
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
    /// Drains the response and returns its server error, if any, while leaving the connection reusable.
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
    /// Splits rows into contiguous ranges of at most <paramref name="maxRowsPerBlock"/> rows.
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
    /// Aligns columns to the server schema and resolves the target codecs.
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

        // Report both missing and unexpected columns.
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

        // Resolve target codecs with the sample block's context, including its session timezone.
        for (int i = 0; i < plan.Length; i++)
        {
            InsertColumn slot = plan[i];
            IColumnCodec codec;
            try
            {
                codec = schema.Codecs.Resolve(slot.TypeName, schema.Context);
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
    /// <remarks>
    /// There is deliberately no early return for an already-terminated connection. Every step below is
    /// idempotent, and the buffer release has to run even when the state was set elsewhere — after
    /// <see cref="AbortTransport"/>, this call as the operation unwinds is the only thing that returns those
    /// buffers to the pool.
    /// </remarks>
    public void Terminate()
    {
        state = TcpConnectionState.Terminated;
        try
        {
            // NetworkStream does not own the socket, so close the socket first to abort pending network I/O.
            // Under TLS the stream is an SslStream over that NetworkStream; neither does I/O when disposed, so
            // disposing them after the socket is closed is safe and the ordering still holds.
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

    /// <summary>
    /// Closes the transport under an operation that is still running, marking the connection final but leaving
    /// the reader and writer alone. Unlike <see cref="Terminate"/> this <i>is</i> safe to call concurrently with
    /// an operation, and it is the only teardown that is: it is how the pool frees a connection whose caller
    /// abandoned it, typically parked on a read that will never arrive.
    /// </summary>
    /// <remarks>
    /// The pooled reader and writer buffers are deliberately not returned here: a buffer a pending read or write
    /// still points at must not go back to the pool, or that memory is handed to an unrelated caller while it is
    /// still in use. The operation returns them itself, through the <see cref="Terminate"/> its own unwinding
    /// calls — which closing the socket provokes, and which runs only once the I/O has actually stopped. That
    /// release is exactly-once even against this call, because the reader and writer guard their disposal with
    /// an interlocked flag. If the operation never unwinds at all, two pooled arrays are left to the garbage
    /// collector: an allocation lost, nothing corrupted.
    /// </remarks>
    internal void AbortTransport()
    {
        // Marks the connection unusable so it is never handed out again. Terminate has no early return, so the
        // operation's own call still releases the buffers afterwards despite the state already being final.
        state = TcpConnectionState.Terminated;

        try
        {
            // Socket disposal is thread-safe and aborts the pending I/O, which is the whole point. With no
            // socket (the scripted-stream seam) the stream itself is the transport.
            if (socket is not null)
            {
                socket.Dispose();
            }
            else
            {
                stream.Dispose();
            }
        }
        catch (Exception e) when (e is not OutOfMemoryException and not StackOverflowException)
        {
            // Best effort: this runs during disposal, with nothing left to report a teardown failure to.
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
