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
    public void FixedStringReadShouldRoundtripAcrossStackallocAndPoolBranches(int length)
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
    public void ReadBytesSpanShouldFillBufferExactly()
    {
        using var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });
        using var reader = new ExtendedBinaryReader(stream);

        Span<byte> buffer = stackalloc byte[5];
        reader.ReadBytes(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3, 4, 5 }));
        Assert.That(stream.Position, Is.EqualTo(stream.Length));
    }

    [Test]
    public void ReadBytesSpanShouldThrowWhenStreamTooShort()
    {
        using var stream = new MemoryStream(new byte[] { 1, 2, 3 });
        using var reader = new ExtendedBinaryReader(stream);

        var buffer = new byte[8];
        Assert.Throws<EndOfStreamException>(() => reader.ReadBytes(buffer));
    }

    // Locks in the read-side sign semantics after the switch to
    // new BigInteger(span, isUnsigned: !Signed): an all-ones little-endian buffer must decode as the
    // unsigned max (2^bits - 1) for unsigned types and as -1 for signed types.
    [Test]
    [TestCase("UInt128", 16)]
    [TestCase("UInt256", 32)]
    public void UnsignedBigIntegerReadShouldDecodeAllOnesAsUnsignedMax(string clickHouseType, int size)
    {
        var value = ReadRawBigInteger(clickHouseType, Enumerable.Repeat((byte)0xFF, size).ToArray());
        Assert.That(value, Is.EqualTo((BigInteger.One << (size * 8)) - 1));
    }

    [Test]
    [TestCase("Int128", 16)]
    [TestCase("Int256", 32)]
    public void SignedBigIntegerReadShouldDecodeAllOnesAsNegativeOne(string clickHouseType, int size)
    {
        var value = ReadRawBigInteger(clickHouseType, Enumerable.Repeat((byte)0xFF, size).ToArray());
        Assert.That(value, Is.EqualTo(BigInteger.MinusOne));
    }

    [Test]
    [TestCase("Int128", 16)]
    [TestCase("Int256", 32)]
    public void SignedBigIntegerReadShouldDecodeHighBitAsMinValue(string clickHouseType, int size)
    {
        // Little-endian two's-complement min: only the most-significant byte's high bit set.
        var bytes = new byte[size];
        bytes[size - 1] = 0x80;
        var value = ReadRawBigInteger(clickHouseType, bytes);
        Assert.That(value, Is.EqualTo(-(BigInteger.One << ((size * 8) - 1))));
    }

    private static BigInteger ReadRawBigInteger(string clickHouseType, byte[] littleEndianBytes)
    {
        var type = TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default);
        using var stream = new MemoryStream(littleEndianBytes);
        using var reader = new ExtendedBinaryReader(stream);
        return (BigInteger)type.Read(reader);
    }
}
