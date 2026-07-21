using System;
using System.Collections.Generic;
using System.IO;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Tests.Formats;

[TestFixture]
public class PooledReadBufferStreamTests
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
    public void Read_PreservesBytes_AcrossBufferBoundaries([ValueSource(nameof(PayloadSizes))] int length)
    {
        var payload = MakePayload(length);
        using var source = new MemoryStream(payload);
        using var buffered = new PooledReadBufferStream(source, bufferSize: 64, leaveOpen: true);

        // Read in 7-byte chunks so requests straddle the 64-byte buffer refills.
        CollectionAssert.AreEqual(payload, ReadAll(buffered, chunk: 7));
    }

    [Test]
    public void ReadByte_PreservesBytes_AcrossBufferBoundaries()
    {
        var payload = MakePayload(300); // spans multiple 64-byte refills
        using var source = new MemoryStream(payload);
        using var buffered = new PooledReadBufferStream(source, bufferSize: 64, leaveOpen: true);

        var result = new List<byte>();
        int b;
        while ((b = buffered.ReadByte()) != -1)
            result.Add((byte)b);

        CollectionAssert.AreEqual(payload, result);
    }

    [Test]
    public void Read_RequestLargerThanBuffer_ReadsThrough()
    {
        // A request at least as large as the buffer must hit the direct-read fast path.
        var payload = MakePayload(10_000);
        using var source = new MemoryStream(payload);
        using var buffered = new PooledReadBufferStream(source, bufferSize: 256, leaveOpen: true);

        CollectionAssert.AreEqual(payload, ReadAll(buffered, chunk: 10_000));
    }

    [Test]
    public void Read_MixedReadByteAndBlockRead_PreservesBytes()
    {
        var payload = MakePayload(200);
        using var source = new MemoryStream(payload);
        using var buffered = new PooledReadBufferStream(source, bufferSize: 64, leaveOpen: true);

        var result = new List<byte>();
        result.Add((byte)buffered.ReadByte()); // consume one, leaving the buffer mid-block
        var block = new byte[50];
        int n = buffered.Read(block, 0, block.Length);
        result.AddRange(block.AsSpan(0, n).ToArray());
        result.AddRange(ReadAll(buffered, chunk: 7));

        CollectionAssert.AreEqual(payload, result);
    }

    [Test]
    public void Read_AtEndOfStream_ReturnsZeroAndMinusOne()
    {
        using var source = new MemoryStream(Array.Empty<byte>());
        using var buffered = new PooledReadBufferStream(source, bufferSize: 64, leaveOpen: true);

        Assert.That(buffered.Read(new byte[4], 0, 4), Is.EqualTo(0));
        Assert.That(buffered.ReadByte(), Is.EqualTo(-1));
    }

    [Test]
    public void Capabilities_AreReadOnly()
    {
        using var buffered = new PooledReadBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        Assert.Multiple(() =>
        {
            Assert.That(buffered.CanRead, Is.True);
            Assert.That(buffered.CanWrite, Is.False);
            Assert.That(buffered.CanSeek, Is.False);
        });
    }

    [Test]
    public void UnsupportedOperations_Throw()
    {
        using var buffered = new PooledReadBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        Assert.Multiple(() =>
        {
            Assert.Throws<NotSupportedException>(() => buffered.Write(new byte[1], 0, 1));
            Assert.Throws<NotSupportedException>(() => buffered.Seek(0, SeekOrigin.Begin));
            Assert.Throws<NotSupportedException>(() => buffered.SetLength(1));
            Assert.Throws<NotSupportedException>(() => _ = buffered.Length);
            Assert.Throws<NotSupportedException>(() => _ = buffered.Position);
            Assert.Throws<NotSupportedException>(() => buffered.Position = 0);
        });
    }

    [Test]
    public void Read_NullArray_Throws()
    {
        using var buffered = new PooledReadBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        Assert.Throws<ArgumentNullException>(() => buffered.Read(null, 0, 0));
    }

    [Test]
    public void Read_InvalidOffsetOrCount_Throws()
    {
        using var buffered = new PooledReadBufferStream(new MemoryStream(MakePayload(8)), bufferSize: 64, leaveOpen: true);
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => buffered.Read(new byte[4], -1, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => buffered.Read(new byte[4], 0, -1));
            Assert.Throws<ArgumentException>(() => buffered.Read(new byte[4], 2, 5)); // offset + count > length
        });
    }

    [Test]
    public void Constructor_NullInner_Throws()
        => Assert.Throws<ArgumentNullException>(() => new PooledReadBufferStream(null, bufferSize: 64));

    [Test]
    public void Constructor_NonPositiveBufferSize_Throws()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new PooledReadBufferStream(new MemoryStream(), bufferSize: 0));

    [Test]
    public void Dispose_WithLeaveOpenFalse_DisposesInnerStream()
    {
        var inner = new MemoryStream(MakePayload(8));
        var buffered = new PooledReadBufferStream(inner, bufferSize: 64, leaveOpen: false);
        buffered.Dispose();

        Assert.That(inner.CanRead, Is.False, "inner stream should be disposed");
    }

    [Test]
    public void Dispose_WithLeaveOpenTrue_LeavesInnerStreamOpen()
    {
        using var inner = new MemoryStream(MakePayload(8));
        var buffered = new PooledReadBufferStream(inner, bufferSize: 64, leaveOpen: true);
        buffered.Dispose();

        Assert.That(inner.CanRead, Is.True, "inner stream should remain usable");
    }

    [Test]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        // The reader disposes this explicitly and a `using` may dispose it again; returning the pooled
        // buffer twice would corrupt the pool, so dispose must be idempotent.
        var buffered = new PooledReadBufferStream(new MemoryStream(), bufferSize: 64, leaveOpen: true);
        buffered.Dispose();
        Assert.DoesNotThrow(() => buffered.Dispose());
    }

    [Test]
    public void Read_AfterDispose_Throws()
    {
        var buffered = new PooledReadBufferStream(new MemoryStream(MakePayload(8)), bufferSize: 64, leaveOpen: true);
        buffered.Dispose();
        Assert.Throws<ObjectDisposedException>(() => buffered.ReadByte());
    }

    private static byte[] ReadAll(Stream stream, int chunk)
    {
        using var output = new MemoryStream();
        var buffer = new byte[chunk];
        int n;
        while ((n = stream.Read(buffer, 0, buffer.Length)) > 0)
            output.Write(buffer, 0, n);
        return output.ToArray();
    }
}
