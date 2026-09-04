using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Compression;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Compiles the row-to-column mapping once the server supplies the INSERT schema. The insert owns the returned
/// source and disposes it.
/// </summary>
/// <param name="schema">The server's sample block, naming and typing the target columns. Valid only for the call.</param>
/// <returns>The source of the columns to insert, matched to the target by name.</returns>
internal delegate IInsertColumnSource InsertColumnFactory(Block schema);

/// <summary>
/// The columns of a row-oriented INSERT, filled one wire block at a time. Created once per insert, against the
/// server's schema; <see cref="Columns"/> holds the same column objects throughout, and each
/// <see cref="Gather"/> makes the next range of rows their values.
/// </summary>
internal interface IInsertColumnSource : IDisposable
{
    /// <summary>The columns, in the schema's order. Empty of rows until the first <see cref="Gather"/>.</summary>
    IReadOnlyList<IColumn> Columns { get; }

    /// <summary>Makes rows <c>[start, start + length)</c> the values of every column.</summary>
    /// <param name="start">The zero-based first row of the block.</param>
    /// <param name="length">The number of rows in the block.</param>
    void Gather(int start, int length);
}

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
/// connection only after the cancelled I/O has unwound. Any server, transport, or protocol failure terminates
/// the connection, and a terminated connection is never reused.
/// </para>
/// </summary>
internal sealed class ClickHouseTcpConnection : IDisposable, IAsyncDisposable
{
    // The setting a query uses to override the session timezone; its value becomes the presentation timezone
    // for timezone-less DateTime/DateTime64 result columns.
    private const string SessionTimezoneSetting = "session_timezone";

    // How long to spend delivering the Cancel packet before giving up on it. Short and not configurable: the
    // connection is closed either way, so all this bounds is how long a cancelling caller waits on a server that
    // has stopped reading.
    private static readonly TimeSpan CancelSendTimeout = TimeSpan.FromSeconds(2);

    private readonly Socket socket;
    private readonly Stream stream;
    private readonly ClickHouseBinaryReader reader;
    private readonly ClickHouseBinaryWriter writer;

    // Bounds how long the server may stay silent mid-response. Null when the caller's token is the only bound.
    private readonly IdleReadDeadline readDeadline;

    // Null means every query on this connection is uncompressed. Compression is per-query on the wire, but the
    // codec is a client-level option today, so it is fixed for a connection's life; a per-query override would
    // move this to the operation entry points.
    private readonly IClickHouseCompressor compressor;

    // Created on the first compressed block and kept for the connection's life, so their pooled buffers are
    // reused across blocks and queries rather than rented per block.
    private CompressedFrameReader frameReader;
    private CompressedFrameWriter frameWriter;

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
    /// <param name="compressor">Frame codec for this connection's queries, or null to run them uncompressed.</param>
    /// <param name="readTimeout">
    /// How long the server may stay silent mid-response before the operation fails. <see cref="TimeSpan.Zero"/>
    /// leaves the caller's token as the only bound, which is the default for the scripted-stream seam.
    /// </param>
    internal ClickHouseTcpConnection(Stream stream, Socket socket, IClickHouseCompressor compressor = null, TimeSpan readTimeout = default)
    {
        this.stream = stream;
        this.socket = socket;
        this.compressor = compressor;
        readDeadline = readTimeout == TimeSpan.Zero ? null : new IdleReadDeadline(readTimeout);

        // The deadline belongs to this buffer alone: it is the one that reads the socket. The frame decoder's
        // buffer is served from bytes that already arrived through here, so it can never stall on the network.
        reader = new ClickHouseBinaryReader(new ReadBuffer(stream, deadline: readDeadline), ownsBuffer: true);
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
    /// connection in active use; the answer to that is <c>ReadTimeout</c>, the idle deadline every read of an
    /// operation runs under, rather than a stricter probe here.
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

            // Decoded plaintext nobody read is the same fault one layer up: the last response's frames carried
            // more than its blocks declared, so this side's idea of the stream position is wrong.
            if (frameReader is { PendingPlaintext: not 0 })
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
    /// <exception cref="ClickHouseTcpTransportException">The socket could not connect, or the TLS handshake failed (certificate rejected, or the port is not a TLS port). The cause is the inner exception.</exception>
    /// <exception cref="ClickHouseTcpServerException">The server rejected the handshake (e.g. authentication failure).</exception>
    /// <exception cref="ClickHouseTcpProtocolException">The server's handshake reply was neither Hello nor Exception.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    public static async ValueTask<ClickHouseTcpConnection> ConnectAsync(
        string host,
        int port,
        ClientHandshakeParameters handshake,
        TlsParameters tls,
        CancellationToken cancellationToken,
        IClickHouseCompressor compressor = null,
        TimeSpan readTimeout = default)
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

        // Before the socket exists, and outside the try below: this runs the caller's ConfigureTls hook, whose
        // failures are its own rather than the transport's, and it fails fast without spending a connection.
        SslClientAuthenticationOptions tlsOptions = tls?.BuildAuthenticationOptions();

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
                transport = await TlsParameters.WrapAsync(transport, tlsOptions, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception e) when (TransportFailure.IsTransportFailure(e))
        {
            socket.Dispose();
            throw TransportFailure.Connect(host, port, e);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        // HandshakeAsync terminates the connection (closing this socket) on any failure, so a throw here needs
        // no extra cleanup. The handshake itself runs under the caller's connect deadline rather than
        // readTimeout, so the two never stack on the one exchange.
        var connection = new ClickHouseTcpConnection(transport, socket, compressor, readTimeout);
        await connection.HandshakeAsync(handshake, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>
    /// Sends a Ping and awaits the reply. Returns when the server answers with Pong. A server Exception is
    /// decoded and thrown. Any error or unexpected packet terminates the connection.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when Pong is received.</returns>
    /// <exception cref="InvalidOperationException">The connection is busy with another operation.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    /// <exception cref="ClickHouseTcpServerException">The server replied with an Exception.</exception>
    /// <exception cref="ClickHouseTcpProtocolException">The server replied with something other than Pong or Exception.</exception>
    /// <exception cref="ClickHouseTcpTransportException">The connection failed while the ping was in flight.</exception>
    /// <exception cref="TimeoutException">The server stayed silent for longer than the connection's ReadTimeout.</exception>
    public async ValueTask PingAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        BeginOperation();

        CancellationToken io = BeginRead(cancellationToken);
        try
        {
            ServerPacketType reply;
            try
            {
                writer.WriteClientPacketType(ClientPacketType.Ping);
                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

                // A Ping is only ever sent on an idle connection, never mid-query, so no Progress or other
                // interleaved packet can precede the reply — unlike a query response, which the read loop drains.
                // A single read therefore suffices; anything but Pong or a (complete) Exception is a violation.
                reply = await reader.ReadServerPacketTypeAsync(io).ConfigureAwait(false);
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
                    ClickHouseTcpServerException exception;
                    try
                    {
                        exception = await ClickHouseTcpServerException.ReadAsync(reader, io).ConfigureAwait(false);
                    }
                    catch
                    {
                        Terminate();
                        throw;
                    }

                    Terminate();
                    throw exception;

                default:
                    Terminate();
                    throw new ClickHouseTcpProtocolException(
                        $"Unexpected packet type {reply} ({(ulong)reply}) in response to Ping; expected Pong or Exception.");
            }
        }
        finally
        {
            EndRead();
        }
    }

    /// <summary>
    /// Runs a query and streams its result as a sequence of <see cref="Block"/>s. Sends the Query and the
    /// empty end-of-input marker, then drains the response, yielding each row-bearing Data block. The
    /// interleaved metadata packets (Progress, ProfileInfo, ProfileEvents, Log, TableColumns, Totals,
    /// Extremes) are always consumed to keep the stream aligned; supply <paramref name="callbacks"/> to
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
    /// <para>
    /// Dispose the enumerator. Everything this method owns is released from one finally, so a consumer that stops
    /// advancing without disposing keeps the connection, the last block's buffers, and the read deadline's
    /// registration on the caller's token for as long as the caller's token source lives.
    /// </para>
    /// </remarks>
    /// <param name="sql">The SQL text.</param>
    /// <param name="settings">Per-query settings as textual values, or null for none.</param>
    /// <param name="parameters">Query parameter values in SQL representation, or null for none.</param>
    /// <param name="queryId">The query id, or null to let the server assign one.</param>
    /// <param name="telemetry">The client's own metadata observers, run before the caller's, or null.</param>
    /// <param name="callbacks">The caller's callbacks for the interleaved metadata packets, or null to discard them.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>An async stream of the result's row-bearing blocks, each valid only for its own iteration.</returns>
    /// <exception cref="InvalidOperationException">The connection is busy with another operation.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    /// <exception cref="ClickHouseTcpServerException">The server reported an error while executing the query.</exception>
    /// <exception cref="ClickHouseTcpProtocolException">The server sent an unexpected packet.</exception>
    /// <exception cref="ClickHouseTcpTransportException">The connection failed while the response was being read.</exception>
    /// <exception cref="TimeoutException">The server stayed silent for longer than the connection's ReadTimeout.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    internal async IAsyncEnumerable<Block> QueryAsync(
        string sql,
        IReadOnlyDictionary<string, string> settings = null,
        IReadOnlyDictionary<string, string> parameters = null,
        string queryId = null,
        ClickHouseTcpQueryCallbacks telemetry = null,
        ClickHouseTcpQueryCallbacks callbacks = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);

        // A pre-cancelled query must not transition the connection out of Ready or write anything, leaving it reusable for the next operation.
        cancellationToken.ThrowIfCancellationRequested();
        BeginOperation();

        ResolveContext readContext = ReadContextFor(settings);
        NegotiatedProtocol negotiated = server.Negotiated;
        ClickHouseTcpServerException pending = null;
        Block current = null;
        bool responseCompleted = false;
        bool reusable = false;
        bool flushedWholePackets = false;

        // Encode the Query packet into the write buffer before any of it reaches the socket. A failure here is a
        // client-side error (e.g. parameters on a protocol revision that predates them): nothing has been sent,
        // so discard the partial packet and leave the connection Ready and reusable rather than terminating it.
        try
        {
            Query.Write(writer, negotiated, clientMetadata, queryId, sql, settings, parameters, compressor is not null);
        }
        catch
        {
            writer.Reset();
            state = TcpConnectionState.Ready;
            throw;
        }

        CancellationToken io = BeginRead(cancellationToken);
        try
        {
            // The end-of-input marker is written here rather than above, because framing it is not buffer-only
            // work: each frame is flushed as it is emitted. A failure part-way through would leave the Query
            // packet on the wire, so it must terminate the connection instead of returning it to the pool
            // looking reusable — the reusable path above holds only work that cannot have sent anything.
            await WriteEndOfInputBlockAsync(cancellationToken).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            // The whole request is on the wire and the client writes nothing more, so from here every exit
            // short of end-of-stream leaves a query the server is still running and can still be cancelled.
            flushedWholePackets = true;

            while (true)
            {
                // Resuming here means the consumer has advanced past the previously yielded block, so its
                // borrowed (possibly pooled) buffers can be released before we read the next packet.
                if (current is not null)
                {
                    current.Dispose();
                    current = null;
                }

                ServerPacketType packet = await reader.ReadServerPacketTypeAsync(io).ConfigureAwait(false);

                if (packet == ServerPacketType.EndOfStream)
                {
                    responseCompleted = true;
                    reusable = true;
                    break;
                }

                if (packet == ServerPacketType.Exception)
                {
                    pending = await ClickHouseTcpServerException.ReadAsync(reader, io).ConfigureAwait(false);
                    responseCompleted = true;
                    break;
                }

                if (packet == ServerPacketType.Data)
                {
                    Block block = await ReadBlockAsync(ServerPacketType.Data, negotiated, readContext, io).ConfigureAwait(false);
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
                    // callbacks when set. An unexpected packet throws from here.
                    await ConsumeMetadataAsync(packet, negotiated, readContext, telemetry, callbacks, io).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            // First, so that nothing below can throw past it and leave the deadline holding a registration on the
            // caller's token. Nothing below reads from the transport, so none of it needs the deadline.
            EndRead();

            // Release the last yielded block (still current) on end-of-stream, early disposal, or error.
            current?.Dispose();

            if (reusable)
            {
                state = TcpConnectionState.Ready;
            }
            else
            {
                // A response that has not reached a terminal packet may still be running on the server.
                if (!responseCompleted)
                {
                    await TrySendCancelAsync(flushedWholePackets).ConfigureAwait(false);
                }

                Terminate();
            }
        }

        if (pending is not null)
        {
            throw pending;
        }
    }

    /// <summary>
    /// The default cap on the rows per wire block (50,000). Block geometry is bounded by row count alone, so this
    /// cap is what splits a large insert into bounded blocks. Peak buffered bytes while a block is written are
    /// bounded separately by the between-column flush backstop
    /// (<see cref="BlockWriter.DefaultFlushThresholdBytes"/>), which flushes mid-block rather than closing it.
    ///
    /// <para>
    /// A row insert converts one block at a time, so for those the cap also bounds the memory the conversion
    /// holds: one buffer per column, of this many values, rather than one of the whole insert's row count.
    /// </para>
    /// </summary>
    public const int DefaultMaxRowsPerBlock = 50_000;

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
    /// <param name="telemetry">The client's own metadata observers, run before the caller's, or null.</param>
    /// <param name="callbacks">The caller's callbacks for the metadata the server interleaves into the insert
    /// acknowledgement (notably <see cref="ClickHouseTcpQueryCallbacks.OnProgress"/> for rows written and
    /// <see cref="ClickHouseTcpQueryCallbacks.OnProfileEvents"/>), or null to discard it.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the server acknowledges the insert with end-of-stream.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sql"/> or <paramref name="columns"/> is null.</exception>
    /// <exception cref="ArgumentException">The columns hold differing row counts or duplicate names, their names
    /// do not match the target schema, or a column's CLR type is not writable as its target type.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxRowsPerBlock"/> is zero or negative, or <paramref name="maxSendBufferBytes"/> is not positive.</exception>
    /// <exception cref="InvalidOperationException">The connection is busy with another operation.</exception>
    /// <exception cref="ObjectDisposedException">The connection has been terminated.</exception>
    /// <exception cref="ClickHouseTcpServerException">The server reported an error while executing the insert.</exception>
    /// <exception cref="ClickHouseTcpTransportException">The connection failed while the blocks were being sent or the response read.</exception>
    /// <exception cref="ClickHouseTcpProtocolException">The server sent an unexpected packet, or no schema block.</exception>
    /// <exception cref="TimeoutException">The server stayed silent for longer than the connection's ReadTimeout.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    internal ValueTask InsertAsync(
        string sql,
        IReadOnlyList<IColumn> columns,
        IReadOnlyDictionary<string, string> settings = null,
        IReadOnlyDictionary<string, string> parameters = null,
        string queryId = null,
        int? maxRowsPerBlock = DefaultMaxRowsPerBlock,
        int maxSendBufferBytes = BlockWriter.DefaultFlushThresholdBytes,
        ClickHouseTcpQueryCallbacks telemetry = null,
        ClickHouseTcpQueryCallbacks callbacks = null,
        CancellationToken cancellationToken = default)
    {
        ValidateInsertArguments(sql, columns, maxRowsPerBlock, maxSendBufferBytes, out int rowCount);
        return InsertCoreAsync(sql, columns, buildColumns: null, rowCount, settings, parameters, queryId, maxRowsPerBlock, maxSendBufferBytes, telemetry, callbacks, cancellationToken);
    }

    /// <summary>
    /// Runs an INSERT whose columns are built from the server's sample block, one wire block of rows at a time.
    /// </summary>
    /// <remarks>
    /// The source is disposed after writing. A failure while the factory compiles the mapping sends no rows. A
    /// failure while a block is gathered — a value the target cannot take, for one — stops at that block, so the
    /// blocks before it have been sent and the server keeps them. Either way the row stream is closed cleanly and
    /// the connection stays reusable, and the failure is thrown once the server has acknowledged the insert.
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
        ClickHouseTcpQueryCallbacks telemetry = null,
        ClickHouseTcpQueryCallbacks callbacks = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(buildColumns);
        if (rowCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(rowCount), rowCount, "The row count must not be negative.");
        }

        ValidateInsertGeometry(maxRowsPerBlock, maxSendBufferBytes);
        return InsertCoreAsync(sql, columns: null, buildColumns, rowCount, settings, parameters, queryId, maxRowsPerBlock, maxSendBufferBytes, telemetry, callbacks, cancellationToken);
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
        ClickHouseTcpQueryCallbacks telemetry,
        ClickHouseTcpQueryCallbacks callbacks,
        CancellationToken cancellationToken)
    {
        // Bail on cancellation before claiming the connection, so a pre-cancelled call leaves it idle.
        cancellationToken.ThrowIfCancellationRequested();
        BeginOperation();

        NegotiatedProtocol negotiated = server.Negotiated;
        // Decode metadata blocks with the operation's session timezone.
        ResolveContext readContext = ReadContextFor(settings);
        ClickHouseTcpServerException pending = null;
        Exception buildFailure = null;
        IReadOnlyList<IColumn> values = null;
        IInsertColumnSource source = null;
        bool responseCompleted = false;
        bool reusable = false;
        bool flushedWholePackets = false;
        string mismatchError = null;
        CancellationToken io = BeginRead(cancellationToken);
        try
        {
            // The empty end-of-input block must follow the Query: the server waits for it before sending the
            // schema block, so omitting it deadlocks.
            Query.Write(writer, negotiated, clientMetadata, queryId, sql, settings, parameters, compressor is not null);
            await WriteEndOfInputBlockAsync(cancellationToken).ConfigureAwait(false);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            flushedWholePackets = true;

            // Drain metadata until the schema block (the first Data packet) or a terminal packet.
            (Block schema, ClickHouseTcpServerException error) = await ReadToNextDataBlockAsync(negotiated, readContext, telemetry, callbacks, io).ConfigureAwait(false);
            if (schema is null)
            {
                responseCompleted = true;
                if (error is null)
                {
                    // Clean end-of-stream with no schema: the server never opened the row-stream phase (e.g.
                    // inline VALUES, or INSERT … SELECT). That breaks the INSERT contract, so terminate rather
                    // than pool a spent connection.
                    throw new ClickHouseTcpProtocolException("The server ended the INSERT response without sending a schema block.");
                }

                // The Exception packet does not say whether the server accepted the query and returned to its
                // request loop, so leave reusable false and retire the connection in the finally below.
                pending = error;
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
                            source = buildColumns(schema);
                            values = source.Columns;
                        }
                        catch (Exception failure)
                        {
                            buildFailure = failure;
                        }
                    }

                    if (buildFailure is null)
                    {
                        // The plan holds the source's columns, which keep their identity as each block refills
                        // them, so it is built once here rather than per block.
                        plan = values.Count == 0
                            ? null
                            : BuildInsertPlan(values, schema, validateWritable: rowCount > 0, out mismatchError);
                    }
                }

                // A row stream flushes each block as it is built, so an exit part-way through leaves a truncated
                // Data packet on the wire. A Cancel appended to that is read as more block bytes, not as a
                // packet, so the row phase is the one stretch where there is nothing useful to send.
                //
                // The call always runs, even after a factory failure: the row stream has to be closed for the
                // server to finish the insert. A gather failure is deferred the same way, so it still closes
                // cleanly and this stays a whole-packet boundary.
                flushedWholePackets = false;
                Exception gatherFailure = await StreamInsertRowsAsync(plan, source, rowCount, maxRowsPerBlock, maxSendBufferBytes, negotiated, cancellationToken).ConfigureAwait(false);
                flushedWholePackets = true;
                buildFailure ??= gatherFailure;

                // A clean acknowledgement leaves the connection reusable. A server Exception is parked for the
                // caller but retires the connection, because its packet does not prove the server will accept
                // another request.
                pending = await DrainToEndOfStreamAsync(negotiated, readContext, telemetry, callbacks, io).ConfigureAwait(false);
                responseCompleted = true;
                reusable = pending is null;
            }
        }
        finally
        {
            // First, so that nothing below can throw past it and leave the deadline holding a registration on the
            // caller's token. Nothing below reads from the transport, so none of it needs the deadline.
            EndRead();

            // Only the factory's source is ours to release; a caller's own columns outlive the insert.
            source?.Dispose();

            if (reusable)
            {
                state = TcpConnectionState.Ready;
            }
            else
            {
                if (!responseCompleted)
                {
                    await TrySendCancelAsync(flushedWholePackets).ConfigureAwait(false);
                }

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
    /// <param name="source">The row source to fill the plan's columns from per block, or null when the caller
    /// supplied whole columns.</param>
    /// <param name="flushThresholdBytes">The buffered-byte cap that triggers a between-column flush while a block is written.</param>
    /// <returns>The gather failure that stopped the row stream, or null if every block was written.</returns>
    private async ValueTask<Exception> StreamInsertRowsAsync(
        InsertColumn[] plan,
        IInsertColumnSource source,
        int rowCount,
        int? maxRowsPerBlock,
        int flushThresholdBytes,
        NegotiatedProtocol negotiated,
        CancellationToken cancellationToken)
    {
        Exception gatherFailure = null;
        if (rowCount > 0 && plan is not null)
        {
            // Row count controls block splitting; the flush threshold bounds buffered output within each block.
            foreach ((int start, int length) in PlanInsertBlocks(rowCount, maxRowsPerBlock))
            {
                if (source is not null)
                {
                    // The block is the conversion unit: fill the columns with these rows, then write them. A
                    // failure here has not written a byte of the block, so the stream is still at a boundary.
                    try
                    {
                        source.Gather(start, length);
                    }
                    catch (Exception failure)
                    {
                        gatherFailure = failure;
                        break;
                    }
                }

                await WriteDataBlockPacketAsync(
                    negotiated, plan, source is null ? start : 0, length, flushThresholdBytes, cancellationToken).ConfigureAwait(false);
                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        await WriteEndOfInputBlockAsync(cancellationToken).ConfigureAwait(false);
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

        // Return the pooled buffer to baseline so an idle connection doesn't retain a large insert's peak size.
        writer.TrimBuffer();
        return gatherFailure;
    }

    /// <summary>
    /// Reads and releases blocks until the response ends, returning the server
    /// <see cref="ClickHouseTcpServerException"/> that terminated it, or null on a clean end-of-stream. The caller
    /// keeps the connection only after the clean end-of-stream case.
    /// </summary>
    /// <param name="negotiated">The negotiated protocol.</param>
    /// <param name="context">The codec-resolution context (timezone) for decoding blocks.</param>
    /// <param name="telemetry">The client's own metadata observers, run before the caller's, or null.</param>
    /// <param name="callbacks">The caller's metadata callbacks for the interleaved packets, or null to discard them.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The parked server exception, or null if the stream ended cleanly.</returns>
    private async ValueTask<ClickHouseTcpServerException> DrainToEndOfStreamAsync(
        NegotiatedProtocol negotiated,
        ResolveContext context,
        ClickHouseTcpQueryCallbacks telemetry,
        ClickHouseTcpQueryCallbacks callbacks,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            (Block block, ClickHouseTcpServerException error) = await ReadToNextDataBlockAsync(negotiated, context, telemetry, callbacks, cancellationToken).ConfigureAwait(false);
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
        int step = RowsPerBlock(rowCount, maxRowsPerBlock);
        for (int start = 0; start < rowCount; start += step)
        {
            blocks.Add((start, Math.Min(step, rowCount - start)));
        }

        return blocks;
    }

    /// <summary>
    /// Returns the largest block <see cref="PlanInsertBlocks"/> will produce, which is what a row insert has to
    /// size its per-column gather buffers for.
    /// </summary>
    /// <param name="rowCount">The number of rows to insert.</param>
    /// <param name="maxRowsPerBlock">The cap on the rows per block, or null for a single block.</param>
    /// <returns>The rows in the largest block.</returns>
    internal static int RowsPerBlock(int rowCount, int? maxRowsPerBlock)
        => maxRowsPerBlock is int cap && cap > 0 ? Math.Min(cap, rowCount) : rowCount;

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
    /// Reads a block whose name is next on the raw stream. The name belongs to the packet envelope and is never
    /// compressed, so it is read from the raw reader; the body comes from the frame reader when compression is
    /// active <i>and</i> this packet is one whose body the server frames.
    /// <para>
    /// This is the only place in the driver that knows two readers exist. Everything below it — the block
    /// reader, every column codec — is handed one reader and cannot tell which.
    /// </para>
    /// </summary>
    /// <param name="packet">The packet type just read from the envelope, which decides whether the body is framed.</param>
    /// <param name="negotiated">The negotiated protocol, for version-gated header fields.</param>
    /// <param name="context">The resolution context passed to each column's codec factory.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded block.</returns>
    private async ValueTask<Block> ReadBlockAsync(
        ServerPacketType packet,
        NegotiatedProtocol negotiated,
        ResolveContext context,
        CancellationToken cancellationToken)
    {
        string name = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);

        if (compressor is null || !FramedPackets.CarriesFramedBody(packet))
        {
            return await BlockReader.ReadBodyAsync(reader, name, negotiated, ColumnCodecRegistry.Default, context, cancellationToken).ConfigureAwait(false);
        }

        frameReader ??= new CompressedFrameReader(reader);
        Block block = await BlockReader.ReadBodyAsync(frameReader.Reader, name, negotiated, ColumnCodecRegistry.Default, context, cancellationToken).ConfigureAwait(false);

        // A block end coincides with a frame boundary, so anything left decoded means the peer and the column
        // decoders disagree about the body's length. The block is never handed out when that check fails, so
        // return its pooled buffers here.
        try
        {
            frameReader.EndBlock();
        }
        catch
        {
            block.Dispose();
            throw;
        }

        return block;
    }

    /// <summary>
    /// Writes a Data packet carrying the empty end-of-input block. With compression on the server expects the
    /// client's own blocks framed too, this marker included, so it is framed like any other body.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    private async ValueTask WriteEndOfInputBlockAsync(CancellationToken cancellationToken)
    {
        writer.WriteClientPacketType(ClientPacketType.Data);
        writer.WriteString(string.Empty); // table_name: envelope, never framed

        if (compressor is null)
        {
            BlockWriter.WriteEmptyBlockBody(writer);
            return;
        }

        frameWriter ??= new CompressedFrameWriter(writer, compressor);
        BlockWriter.WriteEmptyBlockBody(frameWriter.Writer);
        await frameWriter.EndBlockAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes a Data packet carrying rows <c>[start, start + rowCount)</c>, framed when compression is on.</summary>
    /// <param name="negotiated">The negotiated protocol, gating the <c>has_custom_serialization</c> byte.</param>
    /// <param name="columns">The columns to write, in header order.</param>
    /// <param name="start">The zero-based first row of the range each column contributes.</param>
    /// <param name="rowCount">The number of rows the block holds.</param>
    /// <param name="flushThresholdBytes">The buffered-byte cap that triggers a between-column flush.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    private async ValueTask WriteDataBlockPacketAsync(
        NegotiatedProtocol negotiated,
        IReadOnlyList<InsertColumn> columns,
        int start,
        int rowCount,
        int flushThresholdBytes,
        CancellationToken cancellationToken)
    {
        writer.WriteClientPacketType(ClientPacketType.Data);
        writer.WriteString(string.Empty); // table_name: empty for the INSERT row stream, and never framed

        if (compressor is null)
        {
            await BlockWriter.WriteDataBlockBodyAsync(writer, negotiated, columns, start, rowCount, flushThresholdBytes, cancellationToken).ConfigureAwait(false);
            return;
        }

        frameWriter ??= new CompressedFrameWriter(writer, compressor);
        await BlockWriter.WriteDataBlockBodyAsync(frameWriter.Writer, negotiated, columns, start, rowCount, flushThresholdBytes, cancellationToken).ConfigureAwait(false);
        await frameWriter.EndBlockAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a metadata block, lends it to the handler if one is set (borrowed only for the duration of the
    /// call), then releases its storage. A throwing handler propagates after the block has been released.
    /// </summary>
    private async ValueTask ReadMetadataBlockAsync(
        ServerPacketType packet,
        NegotiatedProtocol negotiated,
        ResolveContext context,
        Action<Block> first,
        Action<Block> second,
        CancellationToken cancellationToken)
    {
        Block block = await ReadBlockAsync(packet, negotiated, context, cancellationToken).ConfigureAwait(false);
        try
        {
            first?.Invoke(block);
            second?.Invoke(block);
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
                    try
                    {
                        writer.Dispose();
                    }
                    finally
                    {
                        // The frame buffers are pooled like the reader's and writer's, and this runs under the
                        // same guarantee: Terminate happens once the I/O that pointed at them has unwound.
                        try
                        {
                            frameReader?.Dispose();
                        }
                        finally
                        {
                            frameWriter?.Dispose();
                        }
                    }
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
    /// <param name="telemetry">The client's own metadata observers, run before the caller's, or null.</param>
    /// <param name="callbacks">The caller's metadata callbacks for the interleaved packets, or null to discard them.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The next Data block, or a null block plus the parked terminal exception (if any).</returns>
    private async ValueTask<(Block block, ClickHouseTcpServerException error)> ReadToNextDataBlockAsync(
        NegotiatedProtocol negotiated,
        ResolveContext context,
        ClickHouseTcpQueryCallbacks telemetry,
        ClickHouseTcpQueryCallbacks callbacks,
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
                    return (null, await ClickHouseTcpServerException.ReadAsync(reader, cancellationToken).ConfigureAwait(false));

                case ServerPacketType.Data:
                    return (await ReadBlockAsync(ServerPacketType.Data, negotiated, context, cancellationToken).ConfigureAwait(false), null);

                default:
                    await ConsumeMetadataAsync(packet, negotiated, context, telemetry, callbacks, cancellationToken).ConfigureAwait(false);
                    break;
            }
        }
    }

    /// <summary>
    /// Consumes one interleaved metadata packet to keep the stream aligned, handing it to the matching callback
    /// to each set callback in turn, the client's own first, and discarding it otherwise. Shared by the query and insert
    /// response drains. Any packet type not valid mid-response at this protocol target is a violation.
    /// </summary>
    /// <param name="packet">The packet type just read (never Data, Exception, or EndOfStream).</param>
    /// <param name="negotiated">The negotiated protocol, for version-gated fields.</param>
    /// <param name="context">The codec-resolution context (timezone) for decoding block-bearing packets.</param>
    /// <param name="telemetry">The client's own metadata observers, run before the caller's, or null.</param>
    /// <param name="callbacks">The caller's metadata callbacks, or null to discard every packet.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <exception cref="ClickHouseTcpProtocolException"><paramref name="packet"/> is not a valid interleaved packet.</exception>
    private async ValueTask ConsumeMetadataAsync(
        ServerPacketType packet,
        NegotiatedProtocol negotiated,
        ResolveContext context,
        ClickHouseTcpQueryCallbacks telemetry,
        ClickHouseTcpQueryCallbacks callbacks,
        CancellationToken cancellationToken)
    {
        switch (packet)
        {
            // Block-bearing packets lend the borrowed block to each set handler for the call, then release it.
            case ServerPacketType.Totals:
                await ReadMetadataBlockAsync(ServerPacketType.Totals, negotiated, context, telemetry?.OnTotals, callbacks?.OnTotals, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.Extremes:
                await ReadMetadataBlockAsync(ServerPacketType.Extremes, negotiated, context, telemetry?.OnExtremes, callbacks?.OnExtremes, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.ProfileEvents:
                await ReadMetadataBlockAsync(ServerPacketType.ProfileEvents, negotiated, context, telemetry?.OnProfileEvents, callbacks?.OnProfileEvents, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.Log:
                await ReadMetadataBlockAsync(ServerPacketType.Log, negotiated, context, telemetry?.OnLog, callbacks?.OnLog, cancellationToken).ConfigureAwait(false);
                break;

            case ServerPacketType.Progress:
            {
                ClickHouseTcpProgress progress = await ClickHouseTcpProgress.ReadAsync(reader, negotiated, cancellationToken).ConfigureAwait(false);
                telemetry?.OnProgress?.Invoke(progress);
                callbacks?.OnProgress?.Invoke(progress);
                break;
            }

            case ServerPacketType.ProfileInfo:
            {
                ClickHouseTcpProfileInfo profileInfo = await ClickHouseTcpProfileInfo.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
                telemetry?.OnProfileInfo?.Invoke(profileInfo);
                callbacks?.OnProfileInfo?.Invoke(profileInfo);
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
                throw new ClickHouseTcpProtocolException($"Unexpected packet type {packet} ({(ulong)packet}) in server response.");
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

    /// <summary>
    /// Opens the idle read deadline over an operation's token, and returns the token the operation must pass to
    /// every <b>read</b> it makes. Pair with <see cref="EndRead"/> in a finally.
    /// </summary>
    /// <remarks>
    /// Writes keep the caller's token, deliberately. A deadline that elapses just as a read completes cannot be
    /// recalled — disarming does not stop a timer callback already running — so giving this token to a write
    /// would let it fail as cancelled for a token the caller never cancelled.
    /// </remarks>
    /// <param name="cancellationToken">The caller's token for this operation.</param>
    /// <returns>The token to use for the operation's reads.</returns>
    private CancellationToken BeginRead(CancellationToken cancellationToken)
        => readDeadline?.Begin(cancellationToken) ?? cancellationToken;

    /// <summary>Closes the idle read deadline opened by <see cref="BeginRead"/>.</summary>
    private void EndRead() => readDeadline?.End();

    /// <summary>
    /// Tells the server to stop the query this connection is running, so it does not keep working and writing
    /// into a socket nobody will read. Best effort: the connection is closed next whatever happens here, so a
    /// failure to deliver the packet must not replace the failure that brought us here.
    /// </summary>
    /// <remarks>
    /// The server reads this between the blocks it sends, so a result large enough to fill the socket blocks it in
    /// a write where it reads nothing, and the close that follows stops the query instead. Verified against a real
    /// server: a slow result ends with QUERY_WAS_CANCELLED_BY_CLIENT, a saturating one with a broken pipe. Both
    /// stop it, so this is what turns a silent abandonment into an explicit one rather than the only way out.
    /// </remarks>
    /// <param name="flushedWholePackets">
    /// Whether everything the client has flushed ended a packet. False leaves the Cancel unsent, because a
    /// server part-way through reading a packet takes the next byte as more of that packet, not as a new one.
    /// Only a caller that has completed a flush may pass true: a flush that fails part-way through leaves bytes
    /// on the wire that <c>writer.Reset()</c> cannot take back.
    /// </param>
    /// <returns>A task that completes once the packet has been sent, or given up on.</returns>
    private async ValueTask TrySendCancelAsync(bool flushedWholePackets)
    {
        // Terminated means the socket is already gone, from a concurrent AbortTransport. Checked rather than
        // left to the flush to discover, which only fails safely because a disposed writer holds an empty array.
        if (!flushedWholePackets || state == TcpConnectionState.Terminated)
        {
            return;
        }

        try
        {
            // Discard anything the interrupted operation left buffered, so Cancel is the whole of what goes out.
            writer.Reset();
            writer.WriteClientPacketType(ClientPacketType.Cancel);

            // Not the operation's token: it is usually the cancelled one that brought us here, and flushing on it
            // would send nothing. The separate deadline stops a server that has also stopped reading from holding
            // the caller here — one byte, so it only elapses against a peer whose receive window is shut. It runs
            // before the pool lease is given back, so it is also how long the next caller can wait for the slot.
            using var deadline = new CancellationTokenSource(CancelSendTimeout);
            await writer.FlushAsync(deadline.Token).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not (OutOfMemoryException or StackOverflowException))
        {
            // A connection too broken to carry one byte needs no cancelling.
        }
    }
}
