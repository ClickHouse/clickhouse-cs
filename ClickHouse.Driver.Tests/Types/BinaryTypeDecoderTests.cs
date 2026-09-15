using System.Collections.Generic;
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

    // Decoding a type header must consume exactly its own bytes: on the Dynamic/Variant/JSON read
    // path the value (and the next column) follows immediately in the same stream.
    private static ClickHouseType DecodeWholeHeader(byte[] byteCode, TypeSettings settings)
    {
        using var stream = new MemoryStream(byteCode);
        using var reader = new ExtendedBinaryReader(stream);
        var type = BinaryTypeDecoder.FromByteCode(reader, settings);
        Assert.That(stream.Position, Is.EqualTo(byteCode.Length), "Type header decoding consumed the wrong number of bytes");
        return type;
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

    // Aggregate function type headers as a real server writes them, captured from the RowBinary
    // output of a Dynamic column. They are not reachable through hand-written SQL for every
    // parameter shape, and only a byte-level test can pin how much of the stream they consume.
    private static readonly byte[] SimpleAggregateFunctionSumHeader =
    [
        BinaryTypeIndex.SimpleAggregateFunction,
        0x03, (byte)'s', (byte)'u', (byte)'m',
        0x00,                          // no parameters
        0x01,                          // one argument type
        BinaryTypeIndex.UInt64,
    ];

    private static readonly byte[] SimpleAggregateFunctionGroupArrayArray300Header =
    [
        BinaryTypeIndex.SimpleAggregateFunction,
        0x0F, (byte)'g', (byte)'r', (byte)'o', (byte)'u', (byte)'p', (byte)'A', (byte)'r', (byte)'r',
        (byte)'a', (byte)'y', (byte)'A', (byte)'r', (byte)'r', (byte)'a', (byte)'y',
        0x01,                          // one parameter
        0x01, 0xAC, 0x02,              // UInt64 parameter, varint 300
        0x01,                          // one argument type
        BinaryTypeIndex.Array, BinaryTypeIndex.UInt32,
    ];

    private static readonly byte[] AggregateFunctionSumHeader =
    [
        BinaryTypeIndex.AggregateFunction,
        0x00,                          // serialization version
        0x03, (byte)'s', (byte)'u', (byte)'m',
        0x00,                          // no parameters
        0x01,                          // one argument type
        BinaryTypeIndex.UInt64,
    ];

    private static readonly byte[] AggregateFunctionQuantilesHeader =
    [
        BinaryTypeIndex.AggregateFunction,
        0x00,                          // serialization version
        0x09, (byte)'q', (byte)'u', (byte)'a', (byte)'n', (byte)'t', (byte)'i', (byte)'l', (byte)'e', (byte)'s',
        0x02,                          // two parameters
        0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE0, 0x3F, // Float64 0.5
        0x07, 0xCD, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xEC, 0x3F, // Float64 0.9
        0x01,                          // one argument type
        BinaryTypeIndex.UInt64,
    ];

    // AggregateFunction(1, uniqExact, UInt64), as the server writes it: the leading varint is the
    // aggregate function's state serialization version, and a non-zero version leaves the rest of the
    // header layout unchanged.
    private static readonly byte[] AggregateFunctionUniqExactVersion1Header =
    [
        BinaryTypeIndex.AggregateFunction,
        0x01,                          // serialization version 1
        0x09, (byte)'u', (byte)'n', (byte)'i', (byte)'q', (byte)'E', (byte)'x', (byte)'a', (byte)'c', (byte)'t',
        0x00,                          // no parameters
        0x01,                          // one argument type
        BinaryTypeIndex.UInt64,
    ];

    [Test]
    public void FromByteCode_SimpleAggregateFunction_DecodesFunctionNameAndStorageType()
    {
        var type = (SimpleAggregateFunctionType)DecodeWholeHeader(SimpleAggregateFunctionSumHeader, TypeSettings.Default);

        Assert.That(type.AggregateFunction, Is.EqualTo("sum"));
        Assert.That(type.UnderlyingType, Is.TypeOf<UInt64Type>());
        Assert.That(type.ToString(), Is.EqualTo("SimpleAggregateFunction(sum, UInt64)"));
    }

    [Test]
    public void FromByteCode_SimpleAggregateFunctionWithFunctionParameter_SkipsParameterAndDecodesStorageType()
    {
        var type = (SimpleAggregateFunctionType)DecodeWholeHeader(SimpleAggregateFunctionGroupArrayArray300Header, TypeSettings.Default);

        Assert.That(type.AggregateFunction, Is.EqualTo("groupArrayArray"));
        Assert.That(type.UnderlyingType, Is.TypeOf<ArrayType>());
        Assert.That(((ArrayType)type.UnderlyingType).UnderlyingType, Is.TypeOf<UInt32Type>());
    }

    [Test]
    public void FromByteCode_SimpleAggregateFunction_PassesTypeSettingsToStorageType()
    {
        byte[] byteCode =
        [
            BinaryTypeIndex.SimpleAggregateFunction,
            0x07, (byte)'a', (byte)'n', (byte)'y', (byte)'L', (byte)'a', (byte)'s', (byte)'t',
            0x00,
            0x01,
            BinaryTypeIndex.String,
        ];
        var settings = TypeSettings.Default with { readStringsAsByteArrays = true };

        var type = (SimpleAggregateFunctionType)DecodeWholeHeader(byteCode, settings);

        Assert.That(((StringType)type.UnderlyingType).ReadAsByteArray, Is.True);
    }

    [TestCaseSource(nameof(AggregateFunctionHeaderCases))]
    public void FromByteCode_AggregateFunction_DecodesFunctionName((byte[] ByteCode, string Function) testCase)
    {
        var type = (AggregateFunctionType)DecodeWholeHeader(testCase.ByteCode, TypeSettings.Default);

        Assert.That(type.Function, Is.EqualTo(testCase.Function));
    }

    private static readonly (byte[] ByteCode, string Function)[] AggregateFunctionHeaderCases =
    [
        (AggregateFunctionSumHeader, "sum"),
        (AggregateFunctionQuantilesHeader, "quantiles"),
        (AggregateFunctionUniqExactVersion1Header, "uniqExact"),
    ];

    [Test]
    public void FromByteCode_AggregateFunctionParameterWithUnknownTypeCode_Throws()
    {
        byte[] byteCode =
        [
            BinaryTypeIndex.SimpleAggregateFunction,
            0x03, (byte)'s', (byte)'u', (byte)'m',
            0x01,                      // one parameter
            0x7F,                      // undefined parameter type code
            0x01,
            BinaryTypeIndex.UInt64,
        ];

        Assert.Throws<System.NotSupportedException>(() => Decode(byteCode, TypeSettings.Default));
    }

    // SimpleAggregateFunction(any(<parameter>), UInt64): the parameter under test, followed by the
    // storage type the decoder has to arrive at once that parameter is consumed.
    private static byte[] SimpleAggregateFunctionHeaderWithParameter(byte[] parameter)
    {
        var header = new List<byte>
        {
            BinaryTypeIndex.SimpleAggregateFunction,
            0x03, (byte)'a', (byte)'n', (byte)'y',
            0x01, // one parameter
        };
        header.AddRange(parameter);
        header.Add(0x01); // one argument type
        header.Add(BinaryTypeIndex.UInt64);
        return [.. header];
    }

    // A parameter type code followed by its fixed-width payload (contents are irrelevant, width is not).
    private static byte[] FixedWidthParameter(byte typeCode, int payloadLength, byte[] prefix = null)
    {
        var parameter = new List<byte> { typeCode };
        if (prefix is not null)
        {
            parameter.AddRange(prefix);
        }

        for (int i = 0; i < payloadLength; i++)
        {
            parameter.Add((byte)(i + 1));
        }

        return [.. parameter];
    }

    // One case per aggregate function parameter type code of the "aggregate function parameter binary
    // encoding" table in https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding.
    // The decoder keeps no parameter values, so what each case pins is how many bytes that code occupies —
    // reading one byte too few or too many desynchronises the value (and the next column) that follows in
    // the same stream. The codes a server actually emits for SQL literals were captured from a live
    // server (UInt64 as `groupArray(3)`, Float64 as `quantileTDigest(0.5)`, String as
    // `topK(3, 10, 'counts')`); the remaining layouts come from the documented table.
    public static IEnumerable<TestCaseData> AggregateFunctionParameterCases
    {
        get
        {
            yield return new TestCaseData(new byte[] { 0x00 }).SetName("Parameter_Null");
            yield return new TestCaseData(new byte[] { 0xFE }).SetName("Parameter_NegativeInfinity");
            yield return new TestCaseData(new byte[] { 0xFF }).SetName("Parameter_PositiveInfinity");
            yield return new TestCaseData(new byte[] { 0x01, 0xAC, 0x02 }).SetName("Parameter_UInt64_MultiByteVarInt");
            yield return new TestCaseData(new byte[] { 0x02, 0x03 }).SetName("Parameter_Int64");
            yield return new TestCaseData(FixedWidthParameter(0x13, 1)).SetName("Parameter_Bool");
            yield return new TestCaseData(FixedWidthParameter(0x10, 4)).SetName("Parameter_IPv4");
            yield return new TestCaseData(FixedWidthParameter(0x07, 8)).SetName("Parameter_Float64");
            yield return new TestCaseData(FixedWidthParameter(0x03, 16)).SetName("Parameter_UInt128");
            yield return new TestCaseData(FixedWidthParameter(0x04, 16)).SetName("Parameter_Int128");
            yield return new TestCaseData(FixedWidthParameter(0x11, 16)).SetName("Parameter_IPv6");
            yield return new TestCaseData(FixedWidthParameter(0x12, 16)).SetName("Parameter_UUID");
            yield return new TestCaseData(FixedWidthParameter(0x05, 32)).SetName("Parameter_UInt256");
            yield return new TestCaseData(FixedWidthParameter(0x06, 32)).SetName("Parameter_Int256");
            yield return new TestCaseData(FixedWidthParameter(0x08, 4, [0x02])).SetName("Parameter_Decimal32");
            yield return new TestCaseData(FixedWidthParameter(0x09, 8, [0x02])).SetName("Parameter_Decimal64");
            yield return new TestCaseData(FixedWidthParameter(0x0A, 16, [0x02])).SetName("Parameter_Decimal128");
            yield return new TestCaseData(FixedWidthParameter(0x0B, 32, [0x02])).SetName("Parameter_Decimal256");
            yield return new TestCaseData(new byte[] { 0x0C, 0x06, (byte)'c', (byte)'o', (byte)'u', (byte)'n', (byte)'t', (byte)'s' })
                .SetName("Parameter_String");
            yield return new TestCaseData(new byte[] { 0x0C, 0x00 }).SetName("Parameter_EmptyString");
            yield return new TestCaseData(new byte[] { 0x0D, 0x02, 0x01, 0x03, 0x07, 0, 0, 0, 0, 0, 0, 0xE0, 0x3F })
                .SetName("Parameter_ArrayOfMixedParameters");
            yield return new TestCaseData(new byte[] { 0x0D, 0x00 }).SetName("Parameter_EmptyArray");
            yield return new TestCaseData(new byte[] { 0x0E, 0x02, 0x01, 0x03, 0x0C, 0x01, (byte)'a' })
                .SetName("Parameter_Tuple");
            yield return new TestCaseData(new byte[] { 0x0F, 0x01, 0x0C, 0x01, (byte)'k', 0x01, 0x07 })
                .SetName("Parameter_Map");
            yield return new TestCaseData(new byte[] { 0x0F, 0x00 }).SetName("Parameter_EmptyMap");
            yield return new TestCaseData(new byte[] { 0x14, 0x01, 0x01, (byte)'k', 0x01, 0x07 })
                .SetName("Parameter_Object");
            yield return new TestCaseData(new byte[] { 0x14, 0x00 }).SetName("Parameter_EmptyObject");
            yield return new TestCaseData(new byte[] { 0x15, 0x03, (byte)'s', (byte)'u', (byte)'m', 0x02, 0xAA, 0xBB })
                .SetName("Parameter_AggregateFunctionState");
        }
    }

    [TestCaseSource(nameof(AggregateFunctionParameterCases))]
    public void FromByteCode_SimpleAggregateFunctionParameter_ConsumesExactlyItsOwnBytes(byte[] parameter)
    {
        var type = (SimpleAggregateFunctionType)DecodeWholeHeader(SimpleAggregateFunctionHeaderWithParameter(parameter), TypeSettings.Default);

        Assert.That(type.AggregateFunction, Is.EqualTo("any"));
        Assert.That(type.UnderlyingType, Is.TypeOf<UInt64Type>());
    }

    [Test]
    public void FromByteCode_AggregateFunctionParameterWithMalformedVarInt_Throws()
    {
        // A UInt64 parameter whose LEB128 encoding never terminates: every byte has the continuation
        // bit set, so it cannot be consumed and the stream position can no longer be trusted.
        var parameter = new List<byte> { 0x01 };
        for (int i = 0; i < 11; i++)
        {
            parameter.Add(0xFF);
        }

        Assert.Throws<System.FormatException>(
            () => Decode(SimpleAggregateFunctionHeaderWithParameter([.. parameter]), TypeSettings.Default));
    }

    [Test]
    public void FromByteCode_SimpleAggregateFunctionWithoutArgumentTypes_Throws()
    {
        // The server always writes at least the storage (return) type; without it there is no type to
        // read values with, so this has to be reported rather than yielding a half-built type.
        byte[] byteCode =
        [
            BinaryTypeIndex.SimpleAggregateFunction,
            0x03, (byte)'s', (byte)'u', (byte)'m',
            0x00,                      // no parameters
            0x00,                      // no argument types
        ];

        var exception = Assert.Throws<System.NotSupportedException>(() => Decode(byteCode, TypeSettings.Default));
        Assert.That(exception.Message, Does.Contain("SimpleAggregateFunction(sum)"));
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
