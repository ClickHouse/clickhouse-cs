using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class ReadBufferTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static byte[] Pattern(int length)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = (byte)((i * 37) + 11);
        }

        return data;
    }

    private static ReadBuffer Over(byte[] data, int capacity = 64, int maxChunk = int.MaxValue)
        => new(new ChunkedStream(data, maxChunk), capacity);

    // Returns the actual backing capacity for a given requested capacity (pooling may round it up).
    private static int ActualCapacity(int requested)
    {
        using var probe = new ReadBuffer(new MemoryStream(), requested);
        return probe.Capacity;
    }

    // ---- Construction & validation ------------------------------------------------------------

    [Test]
    public void Constructor_NullStream_Throws()
        => Assert.Throws<ArgumentNullException>(() => new ReadBuffer(null!));

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(31)]
    [TestCase(-1)]
    public void Constructor_CapacityBelowMinimum_Throws(int capacity)
        => Assert.Throws<ArgumentOutOfRangeException>(() => new ReadBuffer(new MemoryStream(), capacity));

    [Test]
    public void Constructor_MinimumCapacity_IsAccepted()
    {
        using var buffer = new ReadBuffer(new MemoryStream(), ReadBuffer.MaxContiguous);
        Assert.That(buffer.Capacity, Is.GreaterThanOrEqualTo(ReadBuffer.MaxContiguous));
        Assert.That(buffer.Buffered, Is.EqualTo(0));
    }

    [Test]
    public void MaxContiguous_IsThirtyTwo() => Assert.That(ReadBuffer.MaxContiguous, Is.EqualTo(32));

    // ---- EnsureAsync --------------------------------------------------------------------------

    [Test]
    public async Task EnsureAsync_Zero_IsNoOpEvenOnEmptyStream()
    {
        using var buffer = new ReadBuffer(new MemoryStream(Array.Empty<byte>()), 64);
        await buffer.EnsureAsync(0, None);
        Assert.That(buffer.Buffered, Is.EqualTo(0));
    }

    [Test]
    public void EnsureAsync_BeyondCapacity_Throws()
    {
        using var buffer = Over(Pattern(256));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await buffer.EnsureAsync(buffer.Capacity + 1, None));
    }

    [Test]
    public async Task EnsureAsync_ExactCapacity_FillsWholeBuffer()
    {
        using var buffer = Over(Pattern(256), capacity: 64);
        await buffer.EnsureAsync(buffer.Capacity, None);
        Assert.That(buffer.Buffered, Is.EqualTo(buffer.Capacity));
    }

    [Test]
    public async Task EnsureAsync_AlreadySatisfied_DoesNotRead()
    {
        var stream = new ChunkedStream(Pattern(256));
        using var buffer = new ReadBuffer(stream, 64);
        await buffer.EnsureAsync(10, None);
        int reads = stream.ReadCount;
        int count = buffer.Buffered;

        await buffer.EnsureAsync(count, None); // already have this many

        Assert.That(stream.ReadCount, Is.EqualTo(reads), "no additional stream read expected");
        Assert.That(buffer.Buffered, Is.EqualTo(count));
    }

    [Test]
    public async Task EnsureAsync_PartialStreamReads_LoopUntilSatisfied()
    {
        // maxChunk = 1 forces one read per byte.
        var stream = new ChunkedStream(Pattern(64), maxChunk: 1);
        using var buffer = new ReadBuffer(stream, 64);

        await buffer.EnsureAsync(20, None);

        Assert.That(buffer.Buffered, Is.GreaterThanOrEqualTo(20));
        Assert.That(stream.ReadCount, Is.EqualTo(20));
    }

    [Test]
    public void EnsureAsync_StreamEndsEarly_Throws()
    {
        using var buffer = new ReadBuffer(new MemoryStream(new byte[] { 1, 2, 3 }), 64);
        Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.EnsureAsync(8, None));
    }

    [Test]
    public void EnsureAsync_CancelledToken_Throws()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        using var buffer = new ReadBuffer(new ChunkedStream(Pattern(64), honorCancellation: true), 64);
        Assert.CatchAsync<OperationCanceledException>(async () => await buffer.EnsureAsync(8, cts.Token));
    }

    // ---- ReadByte -----------------------------------------------------------------------------

    [Test]
    public async Task ReadByte_SequentialThroughManyRefills_ReturnsEveryByteInOrder()
    {
        byte[] source = Pattern(500);
        using var buffer = Over(source, capacity: 40, maxChunk: 13);
        for (int i = 0; i < source.Length; i++)
        {
            await buffer.EnsureAsync(1, None);
            Assert.That(buffer.ReadByte(), Is.EqualTo(source[i]), $"byte {i}");
        }
    }

    [Test]
    public async Task ReadByte_DecrementsCount()
    {
        using var buffer = Over(Pattern(64));
        await buffer.EnsureAsync(4, None);
        int before = buffer.Buffered;
        buffer.ReadByte();
        Assert.That(buffer.Buffered, Is.EqualTo(before - 1));
    }

    // ---- ReadSpan -----------------------------------------------------------------------------

    [Test]
    public async Task ReadSpan_Contiguous_ReturnsExactBytes([Range(1, 32)] int n)
    {
        byte[] source = Pattern(n);
        using var buffer = new ReadBuffer(new MemoryStream(source), 64);
        await buffer.EnsureAsync(n, None);
        CollectionAssert.AreEqual(source, buffer.ReadSpan(n).ToArray());
    }

    [Test]
    public async Task ReadSpan_AboveMaxContiguous_Throws()
    {
        using var buffer = Over(Pattern(256), capacity: 64);
        await buffer.EnsureAsync(ReadBuffer.MaxContiguous, None);
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.ReadSpan(ReadBuffer.MaxContiguous + 1));
    }

    [TestCase(2)]
    [TestCase(3)]
    [TestCase(8)]
    [TestCase(16)]
    [TestCase(31)]
    [TestCase(32)]
    public async Task ReadSpan_RequestNearEnd_CompactsToFrontAndReturnsContiguousBytes(int n)
    {
        int capacity = ActualCapacity(64);
        byte[] source = Pattern(capacity + n);
        using var buffer = new ReadBuffer(new MemoryStream(source), capacity);

        // Consume down to n-1 unconsumed bytes with the head near the end, so the next n-byte request cannot
        // fit at the current head and forces a compaction to the front.
        await buffer.EnsureAsync(capacity, None);
        for (int i = 0; i < capacity - (n - 1); i++)
        {
            buffer.ReadByte();
        }

        await buffer.EnsureAsync(n, None);
        byte[] actual = buffer.ReadSpan(n).ToArray();
        CollectionAssert.AreEqual(source.AsSpan(capacity - (n - 1), n).ToArray(), actual);
    }

    [Test]
    public async Task ReadSpan_ConsumingToCapacity_ResetsAndContinues()
    {
        int capacity = ActualCapacity(64);
        byte[] source = Pattern(capacity + 8);
        using var buffer = new ReadBuffer(new MemoryStream(source), capacity);

        await buffer.EnsureAsync(capacity, None);
        await buffer.ReadIntoAsync(new byte[capacity - 4], None); // consume all but the last 4
        buffer.ReadSpan(4).ToArray();                             // consumes exactly to the end → head resets to 0
        Assert.That(buffer.Buffered, Is.EqualTo(0));

        await buffer.EnsureAsync(8, None);            // continues from the stream after the reset
        CollectionAssert.AreEqual(source.AsSpan(capacity, 8).ToArray(), buffer.ReadSpan(8).ToArray());
    }

    // ---- ReadIntoAsync ------------------------------------------------------------------------

    [Test]
    public async Task ReadIntoAsync_EmptyDestination_IsNoOp()
    {
        using var buffer = new ReadBuffer(new MemoryStream(Array.Empty<byte>()), 64);
        await buffer.ReadIntoAsync(Memory<byte>.Empty, None);
        Assert.That(buffer.Buffered, Is.EqualTo(0));
    }

    [Test]
    public async Task ReadIntoAsync_ServedEntirelyFromBuffer()
    {
        byte[] source = Pattern(64);
        using var buffer = new ReadBuffer(new MemoryStream(source), 64);
        await buffer.EnsureAsync(40, None);

        byte[] destination = new byte[20];
        await buffer.ReadIntoAsync(destination, None);
        CollectionAssert.AreEqual(source.AsSpan(0, 20).ToArray(), destination);
    }

    [Test]
    public async Task ReadIntoAsync_ServedFromBuffer_AfterCompaction()
    {
        int capacity = ActualCapacity(64);
        byte[] source = Pattern(capacity * 2);
        using var buffer = new ReadBuffer(new MemoryStream(source), capacity);

        await buffer.EnsureAsync(capacity, None);
        await buffer.ReadIntoAsync(new byte[capacity - 8], None); // head near the end, 8 bytes left
        await buffer.EnsureAsync(24, None);                       // refill compacts the 8 bytes to the front

        byte[] destination = new byte[24];          // drains a single contiguous run of 24 buffered bytes
        await buffer.ReadIntoAsync(destination, None);
        CollectionAssert.AreEqual(source.AsSpan(capacity - 8, 24).ToArray(), destination);
    }

    [Test]
    public async Task ReadIntoAsync_PartialFromBufferThenStream()
    {
        byte[] source = Pattern(2000);
        using var buffer = Over(source, capacity: 64, maxChunk: 50);
        await buffer.EnsureAsync(10, None);

        byte[] destination = new byte[1500];
        await buffer.ReadIntoAsync(destination, None);
        CollectionAssert.AreEqual(source.AsSpan(0, 1500).ToArray(), destination);
    }

    [Test]
    public async Task ReadIntoAsync_EmptyBuffer_AllFromStream()
    {
        byte[] source = Pattern(1000);
        using var buffer = new ReadBuffer(new MemoryStream(source), 64);

        byte[] destination = new byte[1000];
        await buffer.ReadIntoAsync(destination, None);
        CollectionAssert.AreEqual(source, destination);
    }

    [Test]
    public async Task ReadIntoAsync_LargerThanCapacity()
    {
        byte[] source = Pattern(10000);
        using var buffer = Over(source, capacity: 64, maxChunk: 37);
        byte[] destination = new byte[10000];
        await buffer.ReadIntoAsync(destination, None);
        CollectionAssert.AreEqual(source, destination);
    }

    [Test]
    public void ReadIntoAsync_StreamEndsEarly_Throws()
    {
        using var buffer = new ReadBuffer(new MemoryStream(new byte[] { 1, 2, 3 }), 64);
        Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadIntoAsync(new byte[8], None));
    }

    [Test]
    public async Task ReadIntoAsync_ContinuityAfterDirectRead()
    {
        byte[] source = Pattern(4000);
        using var buffer = Over(source, capacity: 64, maxChunk: 100);

        await buffer.EnsureAsync(5, None);
        byte[] first = new byte[2000];
        await buffer.ReadIntoAsync(first, None);          // drains buffer + direct stream read
        CollectionAssert.AreEqual(source.AsSpan(0, 2000).ToArray(), first);

        // Buffer must be reusable and continue exactly where the direct read left off.
        await buffer.EnsureAsync(8, None);
        CollectionAssert.AreEqual(source.AsSpan(2000, 8).ToArray(), buffer.ReadSpan(8).ToArray());
    }

    [Test]
    public void ReadIntoAsync_CancelledToken_Throws()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        using var buffer = new ReadBuffer(new ChunkedStream(Pattern(64), honorCancellation: true), 64);
        Assert.CatchAsync<OperationCanceledException>(async () => await buffer.ReadIntoAsync(new byte[8], cts.Token));
    }

    // ---- Mixed / stress ------------------------------------------------------------------------

    [Test]
    public async Task Repeated_MisalignedReads_ManyCompactions_StayCorrect()
    {
        const int count = 200;
        byte[] source = Pattern(1 + (8 * count) + 4);
        using var buffer = Over(source, capacity: 40, maxChunk: 40);

        await buffer.EnsureAsync(1, None);
        buffer.ReadByte();

        for (int k = 0; k < count; k++)
        {
            await buffer.EnsureAsync(8, None);
            ulong actual = BinaryPrimitives.ReadUInt64LittleEndian(buffer.ReadSpan(8));
            ulong expected = BinaryPrimitives.ReadUInt64LittleEndian(source.AsSpan(1 + (8 * k), 8));
            Assert.That(actual, Is.EqualTo(expected), $"value {k}");
        }
    }

    [TestCase(48, 7)]
    [TestCase(32, 1)]
    [TestCase(64, 1000)]
    [TestCase(100, 3)]
    public async Task Interleaved_Operations_StayCorrect(int capacity, int maxChunk)
    {
        byte[] source = Pattern(12000);
        using var buffer = Over(source, capacity, maxChunk);
        var rng = new Random(20260707);
        int pos = 0;

        while (pos < source.Length)
        {
            int remaining = source.Length - pos;
            switch (rng.Next(3))
            {
                case 0:
                    await buffer.EnsureAsync(1, None);
                    Assert.That(buffer.ReadByte(), Is.EqualTo(source[pos]), $"byte at {pos}");
                    pos += 1;
                    break;

                case 1:
                    int n = Math.Min(rng.Next(1, ReadBuffer.MaxContiguous + 1), remaining);
                    await buffer.EnsureAsync(n, None);
                    Assert.That(buffer.ReadSpan(n).SequenceEqual(source.AsSpan(pos, n)), Is.True, $"span at {pos} len {n}");
                    pos += n;
                    break;

                default:
                    int m = Math.Min(rng.Next(1, 300), remaining);
                    byte[] destination = new byte[m];
                    await buffer.ReadIntoAsync(destination, None);
                    Assert.That(destination.AsSpan().SequenceEqual(source.AsSpan(pos, m)), Is.True, $"block at {pos} len {m}");
                    pos += m;
                    break;
            }
        }

        Assert.That(pos, Is.EqualTo(source.Length));
    }

    // ---- Dispose ------------------------------------------------------------------------------

    [Test]
    public void Dispose_Twice_DoesNotThrow()
    {
        var buffer = new ReadBuffer(new MemoryStream(Pattern(64)), 64);
        buffer.Dispose();
        Assert.DoesNotThrow(() => buffer.Dispose());
    }

    // ---- Count tracking -----------------------------------------------------------------------

    [Test]
    public async Task Count_TracksAcrossOperations()
    {
        byte[] source = Pattern(200);
        using var buffer = Over(source, capacity: 64, maxChunk: 64);

        Assert.That(buffer.Buffered, Is.EqualTo(0));
        await buffer.EnsureAsync(10, None);
        int afterEnsure = buffer.Buffered;
        Assert.That(afterEnsure, Is.GreaterThanOrEqualTo(10));

        buffer.ReadByte();
        Assert.That(buffer.Buffered, Is.EqualTo(afterEnsure - 1));

        buffer.ReadSpan(4).ToArray();
        Assert.That(buffer.Buffered, Is.EqualTo(afterEnsure - 5));
    }
}
