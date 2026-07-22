using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using ClickHouse.Driver.Compression;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public class PooledWriteBufferStreamTests
{
    // Deterministic pseudo-random payload so failures are reproducible.
    private static byte[] MakePayload(int length)
    {
        var data = new byte[length];
        for (int i = 0; i < length; i++)
            data[i] = (byte)((i * 31 + 7) & 0xFF);
        return data;
    }

    // Sizes chosen to straddle the buffer boundary (empty, sub-buffer, exact, over, multi-buffer).
    private static readonly int[] PayloadSizes = { 0, 1, 63, 64, 65, 200, 256, 257, 1024 };

    [Test]
    public void Write_ThenGZipRoundTrip_PreservesBytes([ValueSource(nameof(PayloadSizes))] int length)
    {
        var payload = MakePayload(length);
        using var destination = new MemoryStream();

        using (var buffered = new PooledWriteBufferStream(new GZipStream(destination, CompressionLevel.Fastest, leaveOpen: true), bufferSize: 64))
        {
            buffered.Write(payload, 0, payload.Length);
        }

        CollectionAssert.AreEqual(payload, Decompress(destination.ToArray()));
    }

    [Test]
    public void WriteByte_ThenGZipRoundTrip_PreservesBytes()
    {
        var payload = MakePayload(300); // spans multiple 64-byte buffer fills
        using var destination = new MemoryStream();

        using (var buffered = new PooledWriteBufferStream(new GZipStream(destination, CompressionLevel.Fastest, leaveOpen: true), bufferSize: 64))
        {
            foreach (var b in payload)
                buffered.WriteByte(b);
        }

        CollectionAssert.AreEqual(payload, Decompress(destination.ToArray()));
    }

    [Test]
    public void Write_LargerThanBuffer_PreservesBytes()
    {
        // A single write far larger than the buffer must hit the direct-forward fast path.
        var payload = MakePayload(10_000);
        using var destination = new MemoryStream();

        using (var buffered = new PooledWriteBufferStream(new GZipStream(destination, CompressionLevel.Fastest, leaveOpen: true), bufferSize: 256))
        {
            buffered.Write(payload, 0, payload.Length);
        }

        CollectionAssert.AreEqual(payload, Decompress(destination.ToArray()));
    }

    [Test]
    public void Write_SmallThenLarge_FlushesPendingBeforeDirectForward()
    {
        using var destination = new MemoryStream();
        var head = MakePayload(10);
        var big = MakePayload(5000);

        using (var buffered = new PooledWriteBufferStream(destination, bufferSize: 256, leaveOpen: true))
        {
            buffered.Write(head, 0, head.Length);   // stays buffered
            buffered.Write(big, 0, big.Length);      // triggers flush of head, then direct forward
        }

        var expected = head.Concat(big).ToArray();
        CollectionAssert.AreEqual(expected, destination.ToArray());
    }

    [Test]
    public void Write_CoalescesSmallWrites_ReducesInnerWriteCalls()
    {
        var counter = new WriteCountingStream(new MemoryStream());

        using (var buffered = new PooledWriteBufferStream(counter, bufferSize: 256, leaveOpen: true))
        {
            for (int i = 0; i < 256; i++)
                buffered.WriteByte((byte)i); // 256 one-byte writes fill exactly one buffer
        }

        // Without buffering this would be 256 inner writes; buffered it is a single flush.
        Assert.That(counter.WriteCalls, Is.EqualTo(1));
    }

    [Test]
    public void Flush_PushesBufferedBytesToInner()
    {
        var counter = new WriteCountingStream(new MemoryStream());
        using var buffered = new PooledWriteBufferStream(counter, bufferSize: 256, leaveOpen: true);

        buffered.WriteByte(1);
        buffered.WriteByte(2);
        Assert.That(counter.WriteCalls, Is.EqualTo(0), "bytes should still be buffered");

        buffered.Flush();
        Assert.That(counter.WriteCalls, Is.EqualTo(1), "flush should forward buffered bytes once");
    }

    [Test]
    public void Write_NullArray_Throws()
    {
        using var buffered = new PooledWriteBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        Assert.Throws<ArgumentNullException>(() => buffered.Write(null, 0, 0));
    }

    [Test]
    public void Dispose_WithLeaveOpenFalse_DisposesInnerStream()
    {
        var inner = new MemoryStream();
        var buffered = new PooledWriteBufferStream(inner, bufferSize: 64, leaveOpen: false);
        buffered.Dispose();

        Assert.That(inner.CanWrite, Is.False, "inner stream should be disposed");
    }

    [Test]
    public void Dispose_WithLeaveOpenTrue_LeavesInnerStreamOpen()
    {
        using var inner = new MemoryStream();
        var buffered = new PooledWriteBufferStream(inner, bufferSize: 64, leaveOpen: true);
        buffered.Dispose();

        Assert.That(inner.CanWrite, Is.True, "inner stream should remain usable");
    }

    [Test]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        // BinaryWriter disposes the underlying stream, then the caller's `using` disposes it
        // again. Returning the pooled buffer twice would corrupt the pool, so dispose must be
        // idempotent.
        var buffered = new PooledWriteBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        buffered.Dispose();
        Assert.DoesNotThrow(() => buffered.Dispose());
    }

    [Test]
    public void Write_AfterDispose_Throws()
    {
        var buffered = new PooledWriteBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        buffered.Dispose();
        Assert.Throws<ObjectDisposedException>(() => buffered.WriteByte(1));
    }

    [Test]
    public void Constructor_NullInner_Throws()
        => Assert.Throws<ArgumentNullException>(() => new PooledWriteBufferStream(null, bufferSize: 64));

    [Test]
    public void Constructor_NonPositiveBufferSize_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new PooledWriteBufferStream(new MemoryStream(), bufferSize: 0));

    [Test]
    public void Capabilities_AreWriteOnly()
    {
        using var buffered = new PooledWriteBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        Assert.Multiple(() =>
        {
            Assert.That(buffered.CanWrite, Is.True);
            Assert.That(buffered.CanRead, Is.False);
            Assert.That(buffered.CanSeek, Is.False);
        });
    }

    [Test]
    public void UnsupportedOperations_Throw()
    {
        using var buffered = new PooledWriteBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        Assert.Multiple(() =>
        {
            Assert.Throws<NotSupportedException>(() => buffered.Read(new byte[1], 0, 1));
            Assert.Throws<NotSupportedException>(() => buffered.Seek(0, SeekOrigin.Begin));
            Assert.Throws<NotSupportedException>(() => buffered.SetLength(1));
            Assert.Throws<NotSupportedException>(() => _ = buffered.Length);
            Assert.Throws<NotSupportedException>(() => _ = buffered.Position);
            Assert.Throws<NotSupportedException>(() => buffered.Position = 0);
        });
    }

    private static byte[] Decompress(byte[] compressed)
    {
        using var source = new MemoryStream(compressed);
        using var gzip = new GZipStream(source, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return output.ToArray();
    }

    private sealed class WriteCountingStream : Stream
    {
        private readonly Stream inner;

        public WriteCountingStream(Stream inner) => this.inner = inner;

        public int WriteCalls { get; private set; }

        public override void Write(byte[] buffer, int offset, int count)
        {
            WriteCalls++;
            inner.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            WriteCalls++;
            inner.Write(buffer);
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => inner.Position = value; }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
