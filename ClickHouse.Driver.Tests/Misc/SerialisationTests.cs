using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Misc;

[TestFixture]
public class SerialisationTests
{
    // Json type is excluded because it has mode-dependent serialization behavior
    // that doesn't fit the simple binary round-trip model. Json is tested separately.
    public static IEnumerable<TestCaseData> NonJsonCases => TestCases.GetDataTypeSamples()
        .Where(sample => !sample.ClickHouseType.StartsWith("Json"))
        .Select(sample => new TestCaseData(sample.ExampleValue, sample.ClickHouseType)
        { TestName = $"ShouldRoundtripSerialisation({sample.ExampleExpression}, {sample.ClickHouseType})" });

    [Test]
    [TestCaseSource(nameof(NonJsonCases))]
    public void ShouldRoundtripSerialisation(object original, string clickHouseType)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);
        type.Write(writer, original);
        stream.Seek(0, SeekOrigin.Begin);
        var read = type.Read(reader);
        TestUtilities.AssertEqual(original, read);
        Assert.That(stream.Position, Is.EqualTo(stream.Length), "Read underflow");
    }

    [Test]
    public void BinaryReaderShouldThrowOnOverflow()
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);

        writer.Write((short)1);
        stream.Seek(0, SeekOrigin.Begin);
        Assert.Throws<EndOfStreamException>(() => reader.ReadInt64());
    }

    // FixedString read chooses a stackalloc scratch buffer for Length <= 256 and an ArrayPool
    // rental above that; exercise both branches (16, the 256 boundary, and 257/1024 above it).
    [Test]
    [TestCase(16)]
    [TestCase(256)]
    [TestCase(257)]
    [TestCase(1024)]
    public void FixedStringRead_AcrossStackallocAndPoolBranches_ShouldRoundtrip(int length)
    {
        var type = TypeConverter.ParseClickHouseType($"FixedString({length})", TypeSettings.Default);
        // Exactly `length` single-byte characters, so no zero-padding is added on write and the
        // decoded string matches the original verbatim.
        var original = new string('x', length);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        using var reader = new ExtendedBinaryReader(stream);
        type.Write(writer, original);
        stream.Seek(0, SeekOrigin.Begin);
        var read = (string)type.Read(reader);

        Assert.That(read, Is.EqualTo(original));
        Assert.That(stream.Position, Is.EqualTo(stream.Length), "Read underflow");
    }

    [Test]
    public void ReadBytesSpan_WithExactLengthStream_ShouldFillBuffer()
    {
        using var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });
        using var reader = new ExtendedBinaryReader(stream);

        Span<byte> buffer = stackalloc byte[5];
        reader.ReadBytes(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3, 4, 5 }));
        Assert.That(stream.Position, Is.EqualTo(stream.Length));
    }

    [Test]
    public void ReadBytesSpan_WhenStreamTooShort_ShouldThrowEndOfStream()
    {
        using var stream = new MemoryStream(new byte[] { 1, 2, 3 });
        using var reader = new ExtendedBinaryReader(stream);

        var buffer = new byte[8];
        Assert.Throws<EndOfStreamException>(() => reader.ReadBytes(buffer));
    }

    // PeekChar buffers a read-ahead byte in PeekableStreamWrapper; a subsequent span read must emit
    // that peeked byte first (not drop or duplicate it) before reading the remainder from the stream.
    [Test]
    public void ReadBytesSpan_AfterPeek_ShouldEmitPeekedByteFirst()
    {
        using var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });
        using var reader = new ExtendedBinaryReader(stream);

        Assert.That(reader.PeekChar(), Is.EqualTo(1));

        Span<byte> buffer = stackalloc byte[5];
        reader.ReadBytes(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3, 4, 5 }));
        Assert.That(stream.Position, Is.EqualTo(stream.Length));
    }

    // An empty span read while a byte is buffered from Peek must be a no-op: it must not write the
    // peeked byte into the zero-length buffer (which would throw) nor consume it from the stream.
    [Test]
    public void PeekableStreamWrapperReadSpan_WithEmptyBufferAfterPeek_ShouldReturnZeroWithoutConsuming()
    {
        using var stream = new MemoryStream(new byte[] { 7, 8, 9 });
        using var wrapper = new PeekableStreamWrapper(stream);

        Assert.That(wrapper.Peek(), Is.EqualTo(7));
        Assert.That(wrapper.Read(Span<byte>.Empty), Is.EqualTo(0));

        // Peeked byte is still pending and read out intact afterwards.
        Assert.That(wrapper.ReadByte(), Is.EqualTo(7));
    }

    // Peeking an empty stream buffers a -1 sentinel; a following span read must surface end-of-stream
    // as a zero-length read rather than writing 0xFF (the truncated sentinel) into the buffer.
    [Test]
    public void PeekableStreamWrapperReadSpan_AfterPeekPastEndOfStream_ShouldReturnZero()
    {
        using var stream = new MemoryStream(Array.Empty<byte>());
        using var wrapper = new PeekableStreamWrapper(stream);

        Assert.That(wrapper.Peek(), Is.EqualTo(-1));

        var buffer = new byte[4];
        Assert.That(wrapper.Read(buffer.AsSpan()), Is.EqualTo(0));
    }

    // Locks in the read-side sign semantics after the switch to
    // new BigInteger(span, isUnsigned: !Signed): an all-ones little-endian buffer must decode as the
    // unsigned max (2^bits - 1) for unsigned types and as -1 for signed types.
    [Test]
    [TestCase("UInt128", 16)]
    [TestCase("UInt256", 32)]
    public void UnsignedBigIntegerRead_WithAllOnes_ShouldDecodeAsUnsignedMax(string clickHouseType, int size)
    {
        var value = ReadRawBigInteger(clickHouseType, Enumerable.Repeat((byte)0xFF, size).ToArray());
        Assert.That(value, Is.EqualTo((BigInteger.One << (size * 8)) - 1));
    }

    [Test]
    [TestCase("Int128", 16)]
    [TestCase("Int256", 32)]
    public void SignedBigIntegerRead_WithAllOnes_ShouldDecodeAsNegativeOne(string clickHouseType, int size)
    {
        var value = ReadRawBigInteger(clickHouseType, Enumerable.Repeat((byte)0xFF, size).ToArray());
        Assert.That(value, Is.EqualTo(BigInteger.MinusOne));
    }

    [Test]
    [TestCase("Int128", 16)]
    [TestCase("Int256", 32)]
    public void SignedBigIntegerRead_WithHighBitSet_ShouldDecodeAsMinValue(string clickHouseType, int size)
    {
        // Little-endian two's-complement min: only the most-significant byte's high bit set.
        var bytes = new byte[size];
        bytes[size - 1] = 0x80;
        var value = ReadRawBigInteger(clickHouseType, bytes);
        Assert.That(value, Is.EqualTo(-(BigInteger.One << ((size * 8) - 1))));
    }

    // Write-side mirror of the read tests above, after the switch to
    // BigInteger.TryWriteBytes(span, out written, isUnsigned: !Signed): the emitted bytes are the
    // value's little-endian two's-complement form, sign-extended (0xFF) or zero-extended to the full
    // column width. Boundary values are the ones the fill and the unsigned sign-byte handling can get
    // wrong.
    public static IEnumerable<TestCaseData> BigIntegerWriteBoundaryCases()
    {
        foreach (var size in new[] { 16, 32 })
        {
            var bits = size * 8;
            var signedType = size == 16 ? "Int128" : "Int256";
            var unsignedType = size == 16 ? "UInt128" : "UInt256";

            yield return WriteCase(signedType, BigInteger.Zero, new byte[size]);
            yield return WriteCase(signedType, BigInteger.One, Bytes(size, (0, 0x01)));
            yield return WriteCase(signedType, BigInteger.MinusOne, Filled(size, 0xFF));
            // -2 is the shortest negative that needs the 0xFF fill past its single written byte.
            yield return WriteCase(signedType, -BigInteger.One - 1, Filled(size, 0xFF, (0, 0xFE)));
            yield return WriteCase(signedType, (BigInteger.One << (bits - 1)) - 1, Filled(size, 0xFF, (size - 1, 0x7F)));
            yield return WriteCase(signedType, -(BigInteger.One << (bits - 1)), Bytes(size, (size - 1, 0x80)));

            yield return WriteCase(unsignedType, BigInteger.Zero, new byte[size]);
            yield return WriteCase(unsignedType, BigInteger.One, Bytes(size, (0, 0x01)));
            // The unsigned maximum is the case the old code handled by trimming BigInteger's
            // trailing sign byte and TryWriteBytes handles with isUnsigned.
            yield return WriteCase(unsignedType, (BigInteger.One << bits) - 1, Filled(size, 0xFF));
            yield return WriteCase(unsignedType, BigInteger.One << (bits - 1), Bytes(size, (size - 1, 0x80)));
        }
    }

    [Test]
    [TestCaseSource(nameof(BigIntegerWriteBoundaryCases))]
    public void BigIntegerWrite_AtBoundaryValues_ShouldEmitTwosComplementBytes(string clickHouseType, BigInteger value, byte[] expected)
    {
        Assert.That(WriteRawBigInteger(clickHouseType, value), Is.EqualTo(expected));
    }

    [Test]
    [TestCase("UInt128")]
    [TestCase("UInt256")]
    public void BigIntegerWrite_WithNegativeValueOnUnsignedType_ShouldThrowArgumentException(string clickHouseType)
    {
        Assert.Throws<ArgumentException>(() => WriteRawBigInteger(clickHouseType, BigInteger.MinusOne));
    }

    [Test]
    [TestCase("Int128", 16)]
    [TestCase("Int256", 32)]
    [TestCase("UInt128", 16)]
    [TestCase("UInt256", 32)]
    public void BigIntegerWrite_WithValueWiderThanTheColumn_ShouldThrowOverflowException(string clickHouseType, int size)
    {
        // One past the widest value the column holds: 2^bits for the unsigned types, 2^(bits-1) for
        // the signed ones. Both need one byte more than the column is wide.
        var bits = size * 8;
        var tooWide = clickHouseType[0] == 'U' ? BigInteger.One << bits : BigInteger.One << (bits - 1);

        var exception = Assert.Throws<OverflowException>(() => WriteRawBigInteger(clickHouseType, tooWide));
        Assert.That(exception.Message, Is.EqualTo($"Got {size + 1} bytes, {size} expected"));
    }

    // The negative side of the same boundary: one below the signed minimum. It is the shape where the
    // overflow check and the 0xFF sign-extension fill meet.
    [Test]
    [TestCase("Int128", 16)]
    [TestCase("Int256", 32)]
    public void BigIntegerWrite_WithValueBelowTheSignedMinimum_ShouldThrowOverflowException(string clickHouseType, int size)
    {
        var tooNegative = -(BigInteger.One << ((size * 8) - 1)) - 1;

        var exception = Assert.Throws<OverflowException>(() => WriteRawBigInteger(clickHouseType, tooNegative));
        Assert.That(exception.Message, Is.EqualTo($"Got {size + 1} bytes, {size} expected"));
    }

    [Test]
    [TestCase("Int128")]
    [TestCase("Int256")]
    [TestCase("UInt128")]
    [TestCase("UInt256")]
    public void BigIntegerWrite_OfManyValues_ShouldNotAllocate(string clickHouseType)
    {
        const int Count = 5000;
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);
        // Boxed once outside the measurement: the box belongs to the caller, the scratch buffers this
        // test guards against belonged to the write itself.
        var value = (object)((BigInteger.One << 100) - 12345);

        using var stream = new MemoryStream((Count * 32) + (16 * 1024));
        using var writer = new ExtendedBinaryWriter(stream);

        // Warm up the JIT and let the writer's buffer settle so only the writes are measured.
        type.Write(writer, value);
        writer.Flush();
        stream.Position = 0;

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Count; i++)
            type.Write(writer, value);
        writer.Flush();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.That(allocated, Is.Zero, $"Write should use a stack buffer; allocated {allocated} bytes for {Count} values");
    }

    private static TestCaseData WriteCase(string clickHouseType, BigInteger value, byte[] expected) =>
        new TestCaseData(clickHouseType, value, expected)
            .SetName($"BigIntegerWrite_{clickHouseType}_{value}_ShouldEmitTwosComplementBytes");

    private static byte[] Bytes(int size, params (int Index, byte Value)[] set) => Filled(size, 0x00, set);

    private static byte[] Filled(int size, byte fill, params (int Index, byte Value)[] set)
    {
        var bytes = new byte[size];
        bytes.AsSpan().Fill(fill);
        foreach (var (index, value) in set)
            bytes[index] = value;
        return bytes;
    }

    private static BigInteger ReadRawBigInteger(string clickHouseType, byte[] littleEndianBytes)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);
        using var stream = new MemoryStream(littleEndianBytes);
        using var reader = new ExtendedBinaryReader(stream);
        return (BigInteger)type.Read(reader);
    }

    private static byte[] WriteRawBigInteger(string clickHouseType, BigInteger value)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        type.Write(writer, value);
        writer.Flush();
        return stream.ToArray();
    }
}
