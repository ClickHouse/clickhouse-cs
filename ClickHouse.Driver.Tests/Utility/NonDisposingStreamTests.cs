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
}
