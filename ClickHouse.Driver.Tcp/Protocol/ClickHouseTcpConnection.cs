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

                switch (packet)
                {
                    case ServerPacketType.Data:
                    {
                        Block block = await BlockReader.ReadBlockAsync(reader, negotiated, ColumnCodecRegistry.Default, cancellationToken).ConfigureAwait(false);
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

                        break;
                    }
                    // The remaining metadata packets are always decoded to stay stream-aligned; each is handed
                    // to its handler when one is set, otherwise discarded. The block-bearing ones lend the block
                    // to the handler for the duration of the call and release it immediately after.
                    case ServerPacketType.Totals:
                        await ReadMetadataBlockAsync(reader, negotiated, handlers?.OnTotals, cancellationToken).ConfigureAwait(false);
                        break;

                    case ServerPacketType.Extremes:
                        await ReadMetadataBlockAsync(reader, negotiated, handlers?.OnExtremes, cancellationToken).ConfigureAwait(false);
                        break;

                    case ServerPacketType.ProfileEvents:
                        await ReadMetadataBlockAsync(reader, negotiated, handlers?.OnProfileEvents, cancellationToken).ConfigureAwait(false);
                        break;

                    case ServerPacketType.Log:
                        await ReadMetadataBlockAsync(reader, negotiated, handlers?.OnLog, cancellationToken).ConfigureAwait(false);
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
                        // A column-defaults description the server may send; the client has no use for it yet, so
                        // it is decoded to stay stream-aligned and discarded (an internal concern, not surfaced).
                        await TableColumns.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
                        break;

                    case ServerPacketType.PartUUIDs:
                        // Valid mid-query when part-level deduplication is active. Consumed to stay stream-aligned;
                        // the decoded UUIDs have no result surface yet. TODO: surface.
                        await PartUUIDs.ConsumeAsync(reader, cancellationToken).ConfigureAwait(false);
                        break;

                    default:
                        // Only the packets above are valid in a query response at this protocol target; anything
                        // else (e.g. a read-task request) is a violation here.
                        throw new ClickHouseProtocolException($"Unexpected packet type {packet} ({(ulong)packet}) in query response.");
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
    /// Reads a metadata block, lends it to the handler if one is set (borrowed only for the duration of the
    /// call), then releases its storage. A throwing handler propagates after the block has been released.
    /// </summary>
    private static async ValueTask ReadMetadataBlockAsync(
        ClickHouseBinaryReader reader,
        NegotiatedProtocol negotiated,
        Action<Block> handler,
        CancellationToken cancellationToken)
    {
        Block block = await BlockReader.ReadBlockAsync(reader, negotiated, ColumnCodecRegistry.Default, cancellationToken).ConfigureAwait(false);
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
