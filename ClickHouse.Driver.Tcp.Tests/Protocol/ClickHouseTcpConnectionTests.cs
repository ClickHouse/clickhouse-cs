using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

// These cover the connection state machine and Ping dispatch over a scripted server stream — the paths a
// live server can't be made to produce on demand (a protocol-violating reply to Ping, an Exception mid-ping,
// operating before handshake). Connect + handshake + ping against a real server is covered end to end by the
// integration suite.
[TestFixture]
public class ClickHouseTcpConnectionTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly ClientHandshakeParameters Handshake = new()
    {
        Username = "default",
    };

    [Test]
    public async Task PingAsync_ServerRepliesException_ThrowsButStaysReusable()
    {
        byte[] exception = await BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Exception);
            w.WriteInt32(516); // AUTHENTICATION_FAILED, arbitrary here
            w.WriteString("DB::Exception");
            w.WriteString("something went wrong");
            w.WriteString("stack trace");
            w.WriteBool(false); // has_nested
        });
        byte[] script = Concat(await ServerHelloBytesAsync(54476), exception);
        var transport = new ScriptedDuplexStream(script);
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);

        var thrown = Assert.ThrowsAsync<ClickHouseServerException>(async () => await connection.PingAsync(None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Code, Is.EqualTo(516));
            Assert.That(thrown.Message, Is.EqualTo("something went wrong"));

            // A cleanly-decoded Exception is a complete response, so the connection remains usable.
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task PingAsync_ServerRepliesUnexpectedPacket_ThrowsProtocolAndTerminates()
    {
        // Data (1) is a valid server packet, but never a valid reply to Ping.
        byte[] script = Concat(await ServerHelloBytesAsync(54476), PacketBytes(ServerPacketType.Data));
        var transport = new ScriptedDuplexStream(script);
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await connection.PingAsync(None));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task PingAsync_TransportEndsBeforeReply_TerminatesConnection()
    {
        // Handshake succeeds, then the stream ends where Pong should be.
        byte[] script = await ServerHelloBytesAsync(54476);
        var transport = new ScriptedDuplexStream(script);
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);

        Assert.ThrowsAsync<EndOfStreamException>(async () => await connection.PingAsync(None));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task PingAsync_ExceptionBodyTruncated_TerminatesConnection()
    {
        // The server announces an Exception but the body is cut off, so decoding it fails partway. A failure
        // reading the reply leaves the stream desynced, so the connection must terminate.
        byte[] script = Concat(await ServerHelloBytesAsync(54476), PacketBytes(ServerPacketType.Exception));
        var transport = new ScriptedDuplexStream(script);
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);

        Assert.ThrowsAsync<EndOfStreamException>(async () => await connection.PingAsync(None));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task HandshakeAsync_ServerRepliesUnexpectedPacket_ThrowsProtocol()
    {
        // Pong is a valid server packet but never a valid handshake reply (only Hello or Exception are).
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(PacketBytes(ServerPacketType.Pong)), socket: null);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () => await connection.HandshakeAsync(Handshake, None));
    }

    [Test]
    public void HandshakeAsync_AfterTerminate_ThrowsObjectDisposed()
    {
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(Array.Empty<byte>()), socket: null);
        connection.Terminate();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await connection.HandshakeAsync(Handshake, None));
    }

    [Test]
    public async Task HandshakeAsync_ServerBelowMinimumProtocol_ThrowsNotSupportedAndTerminates()
    {
        // A revision below the floor the write path assumes: the settings triples we always send would desync
        // it, so the handshake must refuse rather than proceed. The revision still sits above the timezone,
        // display_name and version_patch gates, so those fields are present and must be written.
        byte[] script = await BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Hello);
            w.WriteString("ClickHouse");
            w.WriteVarUInt(21);
            w.WriteVarUInt(3);
            w.WriteVarUInt(NegotiatedProtocol.MinimumTcpProtocolVersion - 1);
            w.WriteString("UTC");                // timezone
            w.WriteString("clickhouse-server");  // display_name
            w.WriteVarUInt(0);                   // version_patch
        });
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(script), socket: null);

        Assert.ThrowsAsync<NotSupportedException>(async () => await connection.HandshakeAsync(Handshake, None));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task PingAsync_TokenAlreadyCancelled_ThrowsWithoutClaimingConnection()
    {
        byte[] script = Concat(await ServerHelloBytesAsync(54476), PacketBytes(ServerPacketType.Pong));
        var transport = new ScriptedDuplexStream(script);
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);
        byte[] writtenBeforePing = transport.Written;

        Assert.ThrowsAsync<OperationCanceledException>(async () => await connection.PingAsync(new CancellationToken(canceled: true)));
        Assert.Multiple(() =>
        {
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(transport.Written, Is.EqualTo(writtenBeforePing));
        });

        // No bytes were sent, so the idle connection remains aligned and can service the next operation.
        await connection.PingAsync(None);
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task PingAsync_CancelledWhileAwaitingPong_TerminatesConnection()
    {
        // Handshake completes, the Ping is sent, then the read for Pong blocks (as against a slow server).
        byte[] script = await ServerHelloBytesAsync(54476);
        var transport = new ScriptedDuplexStream(script, blockWhenExhausted: true);
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);

        using var cts = new CancellationTokenSource();
        ValueTask ping = connection.PingAsync(cts.Token);
        await cts.CancelAsync();

        // CatchAsync (not ThrowsAsync) so the derived TaskCanceledException from the blocked read matches.
        Assert.CatchAsync<OperationCanceledException>(async () => await ping);

        // Cancelling mid-read leaves the reply half-consumed, so the connection is broken and discarded rather
        // than returned to the pool. (A future bounded-drain-and-reuse path could relax this.)
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task PingAsync_AfterTerminate_ThrowsObjectDisposed()
    {
        byte[] script = Concat(await ServerHelloBytesAsync(54476), PacketBytes(ServerPacketType.Pong));
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(script), socket: null);
        await connection.HandshakeAsync(Handshake, None);
        connection.Terminate();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await connection.PingAsync(None));
    }

    [Test]
    public async Task PingAsync_BeforeHandshake_ThrowsInvalidOperation()
    {
        // Still Handshaking (no HandshakeAsync call): the one-in-flight guard rejects the operation.
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(Array.Empty<byte>()), socket: null);

        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Handshaking));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await connection.PingAsync(None));
    }

    [Test]
    public void Terminate_CalledTwice_IsIdempotent()
    {
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(Array.Empty<byte>()), socket: null);

        connection.Terminate();
        Assert.DoesNotThrow(() => connection.Terminate());
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public void ConnectAsync_NullHost_ThrowsArgumentNull()
        => Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await ClickHouseTcpConnection.ConnectAsync(null, 9000, Handshake, None));

    [Test]
    public void ConnectAsync_NullHandshake_ThrowsArgumentNull()
        => Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await ClickHouseTcpConnection.ConnectAsync("localhost", 9000, null, None));

    private static byte[] PacketBytes(ServerPacketType type) => BytesAsync(w => w.WriteVarUInt((ulong)type)).GetAwaiter().GetResult();

    // A ServerHello packet (type code + body) reporting the given revision. All gated fields at/below 54460
    // (timezone, display_name, version_patch) are present, matching a modern server negotiating down to 54460.
    private static Task<byte[]> ServerHelloBytesAsync(int revision) => BytesAsync(w =>
    {
        w.WriteVarUInt((ulong)ServerPacketType.Hello);
        w.WriteString("ClickHouse");
        w.WriteVarUInt(25);
        w.WriteVarUInt(8);
        w.WriteVarUInt((ulong)revision);
        w.WriteString("UTC");
        w.WriteString("clickhouse-server");
        w.WriteVarUInt(0);
    });

    private static async Task<byte[]> BytesAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static byte[] Concat(byte[] first, byte[] second)
    {
        byte[] result = new byte[first.Length + second.Length];
        Buffer.BlockCopy(first, 0, result, 0, first.Length);
        Buffer.BlockCopy(second, 0, result, first.Length, second.Length);
        return result;
    }
}
