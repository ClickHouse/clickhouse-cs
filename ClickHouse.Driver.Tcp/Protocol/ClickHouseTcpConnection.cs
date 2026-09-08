using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

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
