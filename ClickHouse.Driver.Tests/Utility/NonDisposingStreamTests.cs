using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Utility;

[TestFixture]
public class NonDisposingStreamTests
{
    private static readonly byte[] Payload = Encoding.UTF8.GetBytes("clickhouse");

    private sealed class DisposeCountingStream : MemoryStream
    {
        public DisposeCountingStream(byte[] buffer)
            : base(buffer, writable: false)
        {
        }

        public int DisposeCount { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                DisposeCount++;
            base.Dispose(disposing);
        }
    }

    private sealed class TimeoutCapableStream : MemoryStream
    {
        public override bool CanTimeout => true;

        public override int ReadTimeout { get; set; } = 1000;

        public override int WriteTimeout { get; set; } = 2000;
    }

    [Test]
    public void Constructor_WithNullInnerStream_ShouldThrowArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new NonDisposingStream(null));
    }

    [Test]
    public void Dispose_WithWrappedStream_ShouldLeaveWrappedStreamOpen()
    {
        using var inner = new DisposeCountingStream(Payload);

        new NonDisposingStream(inner).Dispose();

        Assert.That(inner.DisposeCount, Is.Zero);
        Assert.That(inner.CanRead, Is.True);
    }

    [Test]
    public void Close_WithWrappedStream_ShouldLeaveWrappedStreamOpen()
    {
        using var inner = new DisposeCountingStream(Payload);

        new NonDisposingStream(inner).Close();

        Assert.That(inner.DisposeCount, Is.Zero);
        Assert.That(inner.CanRead, Is.True);
    }

    [Test]
    public async Task DisposeAsync_WithWrappedStream_ShouldLeaveWrappedStreamOpen()
    {
        using var inner = new DisposeCountingStream(Payload);

        await new NonDisposingStream(inner).DisposeAsync();

        Assert.That(inner.DisposeCount, Is.Zero);
        Assert.That(inner.CanRead, Is.True);
    }

    [Test]
    public void Properties_ShouldDelegateToWrappedStream()
    {
        using var inner = new MemoryStream(Payload, writable: false);
        var wrapper = new NonDisposingStream(inner);

        Assert.That(wrapper.CanRead, Is.EqualTo(inner.CanRead));
        Assert.That(wrapper.CanSeek, Is.EqualTo(inner.CanSeek));
        Assert.That(wrapper.CanWrite, Is.EqualTo(inner.CanWrite));
        Assert.That(wrapper.Length, Is.EqualTo(Payload.Length));

        wrapper.Position = 4;
        Assert.That(inner.Position, Is.EqualTo(4));
        Assert.That(wrapper.Position, Is.EqualTo(4));

        Assert.That(wrapper.Seek(0, SeekOrigin.Begin), Is.Zero);
        Assert.That(inner.Position, Is.Zero);
    }

    [Test]
    public async Task Reads_ShouldDelegateToWrappedStreamAndAdvanceIt()
    {
        using var inner = new MemoryStream(Payload, writable: false);
        var wrapper = new NonDisposingStream(inner);

        var buffer = new byte[4];
        Assert.That(wrapper.Read(buffer, 0, buffer.Length), Is.EqualTo(4));
        Assert.That(buffer, Is.EqualTo(new[] { Payload[0], Payload[1], Payload[2], Payload[3] }));

        Assert.That(await wrapper.ReadAsync(buffer, 0, 2), Is.EqualTo(2));
        Assert.That(wrapper.ReadByte(), Is.EqualTo(Payload[6]));
        Assert.That(inner.Position, Is.EqualTo(7));

        using var destination = new MemoryStream();
        await wrapper.CopyToAsync(destination);
        Assert.That(destination.ToArray(), Is.EqualTo(Payload[7..]));
    }

    [Test]
    public void Writes_ShouldDelegateToWrappedStream()
    {
        using var inner = new MemoryStream();
        var wrapper = new NonDisposingStream(inner);

        wrapper.Write(Payload, 0, Payload.Length);
        wrapper.WriteByte((byte)'!');
        wrapper.Flush();

        Assert.That(inner.ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("clickhouse!")));
    }

    [Test]
    public async Task SpanAndMemoryReads_ShouldDelegateToWrappedStreamAndAdvanceIt()
    {
        using var inner = new MemoryStream(Payload, writable: false);
        var wrapper = new NonDisposingStream(inner);

        var buffer = new byte[5];
        Assert.That(wrapper.Read(buffer.AsSpan(0, 4)), Is.EqualTo(4));
        Assert.That(buffer[..4], Is.EqualTo(Payload[..4]));

        Assert.That(await wrapper.ReadAsync(buffer.AsMemory(0, 5)), Is.EqualTo(5));
        Assert.That(buffer, Is.EqualTo(Payload[4..9]));
        Assert.That(inner.Position, Is.EqualTo(9));
    }

    [Test]
    public async Task SpanAndMemoryWrites_ShouldDelegateToWrappedStream()
    {
        using var inner = new MemoryStream();
        var wrapper = new NonDisposingStream(inner);

        wrapper.Write(Payload.AsSpan(0, 5));
        await wrapper.WriteAsync(Payload, 5, Payload.Length - 5);
        await wrapper.WriteAsync(new ReadOnlyMemory<byte>(new[] { (byte)'!' }));
        await wrapper.FlushAsync();

        Assert.That(inner.ToArray(), Is.EqualTo(Encoding.UTF8.GetBytes("clickhouse!")));
    }

    [Test]
    public void CopyTo_WithExplicitBufferSize_ShouldDelegateToWrappedStream()
    {
        using var inner = new MemoryStream(Payload, writable: false);
        var wrapper = new NonDisposingStream(inner);

        using var destination = new MemoryStream();
        wrapper.CopyTo(destination, 4);

        Assert.That(destination.ToArray(), Is.EqualTo(Payload));
        Assert.That(inner.Position, Is.EqualTo(Payload.Length));
    }

    [Test]
    public void SetLength_ShouldDelegateToWrappedStream()
    {
        using var inner = new MemoryStream();
        var wrapper = new NonDisposingStream(inner);

        wrapper.SetLength(3);

        Assert.That(inner.Length, Is.EqualTo(3));
        Assert.That(wrapper.Length, Is.EqualTo(3));
    }

    [Test]
    public void TimeoutMembers_ShouldDelegateToWrappedStream()
    {
        using var timeoutCapable = new TimeoutCapableStream();
        var wrapper = new NonDisposingStream(timeoutCapable);

        Assert.That(wrapper.CanTimeout, Is.True);
        Assert.That(wrapper.ReadTimeout, Is.EqualTo(1000));
        Assert.That(wrapper.WriteTimeout, Is.EqualTo(2000));

        wrapper.ReadTimeout = 250;
        wrapper.WriteTimeout = 500;

        Assert.That(timeoutCapable.ReadTimeout, Is.EqualTo(250));
        Assert.That(timeoutCapable.WriteTimeout, Is.EqualTo(500));
    }

    [Test]
    public void CanTimeout_WithNonTimeoutCapableWrappedStream_ShouldBeFalse()
    {
        using var inner = new MemoryStream(Payload, writable: false);

        Assert.That(new NonDisposingStream(inner).CanTimeout, Is.False);
    }
}
