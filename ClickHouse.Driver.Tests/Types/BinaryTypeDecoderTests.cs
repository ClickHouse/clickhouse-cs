using System.IO;
using ClickHouse.Driver;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class BinaryTypeDecoderTests
{
    private static ClickHouseType Decode(byte[] byteCode, TypeSettings settings)
    {
        using var stream = new MemoryStream(byteCode);
        using var reader = new ExtendedBinaryReader(stream);
        return BinaryTypeDecoder.FromByteCode(reader, settings);
    }

    // Every stateless, parameterless type code should decode to the exact same shared instance
    // on repeated calls (the allocation optimisation), and to the expected runtime type.
    private static readonly (byte ByteCode, System.Type Expected)[] StatelessTypeCases =
    [
        (BinaryTypeIndex.Nothing, typeof(NothingType)),
        (BinaryTypeIndex.UInt8, typeof(UInt8Type)),
        (BinaryTypeIndex.UInt16, typeof(UInt16Type)),
        (BinaryTypeIndex.UInt32, typeof(UInt32Type)),
        (BinaryTypeIndex.UInt64, typeof(UInt64Type)),
        (BinaryTypeIndex.UInt128, typeof(UInt128Type)),
        (BinaryTypeIndex.UInt256, typeof(UInt256Type)),
        (BinaryTypeIndex.Int8, typeof(Int8Type)),
        (BinaryTypeIndex.Int16, typeof(Int16Type)),
        (BinaryTypeIndex.Int32, typeof(Int32Type)),
        (BinaryTypeIndex.Int64, typeof(Int64Type)),
        (BinaryTypeIndex.Int128, typeof(Int128Type)),
        (BinaryTypeIndex.Int256, typeof(Int256Type)),
        (BinaryTypeIndex.Float32, typeof(Float32Type)),
        (BinaryTypeIndex.Float64, typeof(Float64Type)),
        (BinaryTypeIndex.BFloat16, typeof(BFloat16Type)),
        (BinaryTypeIndex.Date, typeof(DateType)),
        (BinaryTypeIndex.Date32, typeof(Date32Type)),
        (BinaryTypeIndex.UUID, typeof(UuidType)),
        (BinaryTypeIndex.IPv4, typeof(IPv4Type)),
        (BinaryTypeIndex.IPv6, typeof(IPv6Type)),
        (BinaryTypeIndex.Bool, typeof(BooleanType)),
        (BinaryTypeIndex.Time, typeof(TimeType)),
    ];

    [TestCaseSource(nameof(StatelessTypeCases))]
    public void FromByteCode_StatelessType_ReturnsCachedSingletonInstance((byte ByteCode, System.Type Expected) testCase)
    {
        var first = Decode([testCase.ByteCode], TypeSettings.Default);
        var second = Decode([testCase.ByteCode], TypeSettings.Default);

        Assert.That(first, Is.TypeOf(testCase.Expected));
        Assert.That(second, Is.SameAs(first), "Stateless type should decode to a shared singleton instance");
    }

    [Test]
    public void FromByteCode_String_ReturnsSharedInstancePerReadAsByteArrayVariant()
    {
        var stringSettings = TypeSettings.Default with { readStringsAsByteArrays = false };
        var byteArraySettings = TypeSettings.Default with { readStringsAsByteArrays = true };

        var stringA = (StringType)Decode([BinaryTypeIndex.String], stringSettings);
        var stringB = (StringType)Decode([BinaryTypeIndex.String], stringSettings);
        var bytesA = (StringType)Decode([BinaryTypeIndex.String], byteArraySettings);
        var bytesB = (StringType)Decode([BinaryTypeIndex.String], byteArraySettings);

        // Same variant -> shared instance
        Assert.That(stringB, Is.SameAs(stringA));
        Assert.That(bytesB, Is.SameAs(bytesA));

        // Different variant -> distinct instance with the correct flag
        Assert.That(bytesA, Is.Not.SameAs(stringA));
        Assert.That(stringA.ReadAsByteArray, Is.False);
        Assert.That(bytesA.ReadAsByteArray, Is.True);
    }

    [Test]
    public void FromByteCode_DateTimeUtc_ReturnsFreshInstance()
    {
        // DateTimeType carries a mutable TimeZone, so it must NOT be shared.
        var first = Decode([BinaryTypeIndex.DateTimeUTC], TypeSettings.Default);
        var second = Decode([BinaryTypeIndex.DateTimeUTC], TypeSettings.Default);

        Assert.That(first, Is.TypeOf<DateTimeType>());
        Assert.That(second, Is.Not.SameAs(first));
    }
}
