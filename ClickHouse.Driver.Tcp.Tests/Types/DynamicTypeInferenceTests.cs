using System;
using System.Collections.Generic;
using System.Net;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DynamicTypeInferenceTests
{
    [Test]
    public void Infer_Null_Throws()
        => Assert.Throws<ArgumentNullException>(() => DynamicTypeInference.Infer(null));

    [TestCase((byte)1, "UInt8")]
    [TestCase((sbyte)-1, "Int8")]
    [TestCase((ushort)1, "UInt16")]
    [TestCase((short)-1, "Int16")]
    [TestCase(1u, "UInt32")]
    [TestCase(-1, "Int32")]
    [TestCase(1UL, "UInt64")]
    [TestCase(-1L, "Int64")]
    [TestCase(1.5f, "Float32")]
    [TestCase(1.5d, "Float64")]
    [TestCase(true, "Bool")]
    [TestCase("s", "String")]
    public void Infer_Scalar_MapsToClickHouseTypeAndKeepsValue(object value, string expected)
    {
        (string typeName, object canonical) = DynamicTypeInference.Infer(value);
        Assert.That(typeName, Is.EqualTo(expected));
        Assert.That(canonical, Is.EqualTo(value));
    }

    [Test]
    public void Infer_WideIntegers_Map()
    {
        Assert.That(DynamicTypeInference.Infer(UInt128.One).TypeName, Is.EqualTo("UInt128"));
        Assert.That(DynamicTypeInference.Infer(Int128.MinValue).TypeName, Is.EqualTo("Int128"));
        Assert.That(DynamicTypeInference.Infer(UInt256.Zero).TypeName, Is.EqualTo("UInt256"));
        Assert.That(DynamicTypeInference.Infer(Int256.Zero).TypeName, Is.EqualTo("Int256"));
    }

    [Test]
    public void Infer_Guid_MapsToUuid()
        => Assert.That(DynamicTypeInference.Infer(Guid.NewGuid()).TypeName, Is.EqualTo("UUID"));

    [Test]
    public void Infer_DateOnly_MapsToDate32()
        => Assert.That(DynamicTypeInference.Infer(new DateOnly(2024, 1, 1)).TypeName, Is.EqualTo("Date32"));

    [Test]
    public void Infer_IpAddress_DisambiguatesByFamily()
    {
        Assert.That(DynamicTypeInference.Infer(IPAddress.Parse("127.0.0.1")).TypeName, Is.EqualTo("IPv4"));
        Assert.That(DynamicTypeInference.Infer(IPAddress.Parse("::1")).TypeName, Is.EqualTo("IPv6"));
    }

    [Test]
    public void Infer_DateTimeOffset_MapsToDateTime64AndCoercesToNanosecondCount()
    {
        var value = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5));
        (string typeName, object canonical) = DynamicTypeInference.Infer(value);

        Assert.That(typeName, Is.EqualTo("DateTime64(9)"));
        Assert.That(canonical, Is.InstanceOf<long>());
        Assert.That((long)canonical, Is.EqualTo((value.UtcDateTime.Ticks - DateTime.UnixEpoch.Ticks) * 100));
    }

    [Test]
    public void Infer_Long_MapsToInt64_NotDateTime64()
        // A raw Int64 is ambiguous with a DateTime64 count, so Dynamic keeps it Int64; date-time semantics need a
        // DateTimeOffset/DateTime input.
        => Assert.That(DynamicTypeInference.Infer(1_700_000_000_123L).TypeName, Is.EqualTo("Int64"));

    [Test]
    public void Infer_Decimal_MapsToDecimal128AtItsScaleAndCoerces()
    {
        (string typeName, object canonical) = DynamicTypeInference.Infer(12.340m);
        Assert.That(typeName, Is.EqualTo("Decimal(38, 3)"));
        Assert.That(canonical, Is.InstanceOf<ClickHouseDecimal>());
    }

    [Test]
    public void Infer_ClickHouseDecimal_MapsToDecimal256AtItsScale()
        => Assert.That(DynamicTypeInference.Infer(new ClickHouseDecimal(new System.Numerics.BigInteger(12345), 2)).TypeName, Is.EqualTo("Decimal(76, 2)"));

    [Test]
    public void Infer_Array_RecursesIntoElementType()
        => Assert.That(DynamicTypeInference.Infer(new ulong[] { 1, 2 }).TypeName, Is.EqualTo("Array(UInt64)"));

    [Test]
    public void Infer_EmptyArray_UsesDeclaredElementType()
        => Assert.That(DynamicTypeInference.Infer(Array.Empty<int>()).TypeName, Is.EqualTo("Array(Int32)"));

    [Test]
    public void Infer_Map_MapsToMapOfKeyAndValue()
        => Assert.That(DynamicTypeInference.Infer(new[] { new KeyValuePair<string, uint>("a", 1) }).TypeName, Is.EqualTo("Map(String, UInt32)"));

    // A Map key or value whose ClickHouse type only its value can settle — an IPAddress's family, a
    // ClickHouseDecimal's scale — resolves from the pairs, the same way an Array element or a Tuple element does.
    [Test]
    public void Infer_MapWithValueDisambiguatedKeyAndValue_ReadsThePairs()
        => Assert.That(
            DynamicTypeInference.Infer(new[] { new KeyValuePair<IPAddress, ClickHouseDecimal>(IPAddress.Parse("::1"), new ClickHouseDecimal(new System.Numerics.BigInteger(12345), 2)) }).TypeName,
            Is.EqualTo("Map(IPv6, Decimal(76, 2))"));

    [Test]
    public void Infer_MapOfBoxedValues_ReadsTheRuntimeTypeOfEachValue()
        => Assert.That(
            DynamicTypeInference.Infer(new[] { new KeyValuePair<string, object>("a", IPAddress.Parse("127.0.0.1")) }).TypeName,
            Is.EqualTo("Map(String, IPv4)"));

    [Test]
    public void Infer_EmptyMap_UsesDeclaredKeyAndValueTypes()
        => Assert.That(DynamicTypeInference.Infer(Array.Empty<KeyValuePair<string, uint>>()).TypeName, Is.EqualTo("Map(String, UInt32)"));

    // The write path buckets a whole map as one key type and one value type, so a mixed slot must be rejected at
    // inference rather than failing the element cast later.
    [Test]
    public void Infer_MapWithMixedKeyTypes_Throws()
        => Assert.That(
            () => DynamicTypeInference.Infer(new[]
            {
                new KeyValuePair<IPAddress, uint>(IPAddress.Parse("127.0.0.1"), 1),
                new KeyValuePair<IPAddress, uint>(IPAddress.Parse("::1"), 2),
            }),
            Throws.TypeOf<NotSupportedException>().With.Message.Contains("keys").And.Message.Contains("IPv4").And.Message.Contains("IPv6"));

    [Test]
    public void Infer_MapWithMixedValueTypes_Throws()
        => Assert.That(
            () => DynamicTypeInference.Infer(new[]
            {
                new KeyValuePair<string, object>("a", 1),
                new KeyValuePair<string, object>("b", "x"),
            }),
            Throws.TypeOf<NotSupportedException>().With.Message.Contains("values").And.Message.Contains("Int32").And.Message.Contains("String"));

    [Test]
    public void Infer_MapWithCoercionNeedingValue_Throws()
        => Assert.Throws<NotSupportedException>(() => DynamicTypeInference.Infer(new[] { new KeyValuePair<string, decimal>("a", 2.5m) }));

    [Test]
    public void Infer_ArrayWithMixedElementTypes_Throws()
        => Assert.That(
            () => DynamicTypeInference.Infer(new object[] { IPAddress.Parse("127.0.0.1"), IPAddress.Parse("::1") }),
            Throws.TypeOf<NotSupportedException>().With.Message.Contains("elements").And.Message.Contains("IPv4").And.Message.Contains("IPv6"));

    [Test]
    public void Infer_Tuple_MapsToTupleOfElements()
        => Assert.That(DynamicTypeInference.Infer((1, "a")).TypeName, Is.EqualTo("Tuple(Int32, String)"));

    [Test]
    public void Infer_UnsupportedType_Throws()
        => Assert.Throws<NotSupportedException>(() => DynamicTypeInference.Infer(new object()));

    [Test]
    public void Infer_ArrayOfCoercionNeedingElement_Throws()
        => Assert.Throws<NotSupportedException>(() => DynamicTypeInference.Infer(new[] { DateTimeOffset.UnixEpoch }));

    [Test]
    public void Infer_TupleWithCoercionNeedingElement_Throws()
        => Assert.Throws<NotSupportedException>(() => DynamicTypeInference.Infer((1, 2.5m)));
}
