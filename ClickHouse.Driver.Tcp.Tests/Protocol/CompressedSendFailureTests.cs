using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

/// <summary>
/// What happens when sending a compressed request fails part-way. Framing the end-of-input marker is not
/// buffer-only work — each frame is flushed as it is emitted — so a failure there can leave the Query packet on
/// the wire. Such a connection must terminate rather than return to the pool looking reusable, or the next
/// caller to lease it reads the previous query's response.
/// </summary>
[TestFixture]
public class CompressedSendFailureTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task QueryAsync_Compressed_WhenTheFramedEndOfInputMarkerFailsToSend_TerminatesTheConnection()
    {
        var transport = new FailOnDemandStream(await FakeConnectionFactory.ServerHelloBytesAsync(None));
        var connection = new ClickHouseTcpConnection(transport, socket: null, Lz4Compressor.Default);
        await connection.HandshakeAsync(new ClientHandshakeParameters { Username = "default" }, None);
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready), "guard: the handshake must have completed");

        transport.FailWrites = true;

        Assert.ThrowsAsync<ClickHouseTcpTransportException>(async () =>
        {
            await foreach (Block _ in connection.QueryAsync("SELECT 1", cancellationToken: None))
            {
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
            Assert.That(connection.IsReusable, Is.False);
        });
    }

    [Test]
    public async Task InsertAsync_Compressed_WhenTheFramedEndOfInputMarkerFailsToSend_TerminatesTheConnection()
    {
        var transport = new FailOnDemandStream(await FakeConnectionFactory.ServerHelloBytesAsync(None));
        var connection = new ClickHouseTcpConnection(transport, socket: null, Lz4Compressor.Default);
        await connection.HandshakeAsync(new ClientHandshakeParameters { Username = "default" }, None);

        transport.FailWrites = true;

        Assert.ThrowsAsync<ClickHouseTcpTransportException>(
            async () => await connection.InsertAsync("INSERT INTO t VALUES", Array.Empty<IColumn>(), cancellationToken: None));

        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    /// <summary>Serves a fixed read script, and fails writes once armed. Reads keep working, so only the send path breaks.</summary>
    private sealed class FailOnDemandStream : Stream
    {
        private readonly MemoryStream script;

        public FailOnDemandStream(byte[] readScript) => script = new MemoryStream(readScript);

        public bool FailWrites { get; set; }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => script.ReadAsync(buffer, cancellationToken);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => script.ReadAsync(buffer, offset, count, cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => script.Read(buffer, offset, count);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => FailWrites
                ? ValueTask.FromException(new IOException("simulated send failure"))
                : ValueTask.CompletedTask;

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).AsTask();

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (FailWrites)
            {
                throw new IOException("simulated send failure");
            }
        }

        public override void Flush()
        {
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            script.Dispose();
            base.Dispose(disposing);
        }
    }
}
