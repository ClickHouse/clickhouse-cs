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
    public void Infer_DateTimeOffset_MapsToDateTime64AndCoercesToClickHouseDateTime64()
    {
        var value = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5));
        (string typeName, object canonical) = DynamicTypeInference.Infer(value);

        Assert.That(typeName, Is.EqualTo("DateTime64(9)"));
        Assert.That(canonical, Is.InstanceOf<ClickHouseDateTime64>());
        Assert.That(((ClickHouseDateTime64)canonical).ToDateTimeOffset(), Is.EqualTo(value));
    }

    [Test]
    public void Infer_ClickHouseDateTime64_KeepsItsScale()
        => Assert.That(DynamicTypeInference.Infer(new ClickHouseDateTime64(0, 3, TimeSpan.Zero)).TypeName, Is.EqualTo("DateTime64(3)"));

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

    [TestCase(1, 1)]
    [TestCase(255, 1)]
    [TestCase(256, 2)]
    [TestCase(65535, 2)]
    [TestCase(65536, 4)]
    public void DiscriminatorWidth_GrowsWithTypeCount(int typeCount, int expectedWidth)
        => Assert.That(DynamicWire.DiscriminatorWidth(typeCount), Is.EqualTo(expectedWidth));
}
