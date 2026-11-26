using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>Tests which CLR element types each codec accepts for writing.</summary>
[TestFixture]
public class ColumnWriteAcceptanceTests
{
    private static IColumnCodec Codec(string type)
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = "UTC" });

    [TestCase("UInt64")]
    [TestCase("String")]
    [TestCase("DateTime('UTC')")]
    [TestCase("Array(UInt64)")]
    [TestCase("Map(String, UInt64)")]
    [TestCase("Tuple(UInt64, String)")]
    [TestCase("LowCardinality(String)")]
    [TestCase("Nullable(Int32)")]
    [TestCase("Variant(String, UInt64)")]
    [TestCase("Dynamic")]
    public void CanWriteElementType_ItsOwnElementType_IsAccepted(string type)
    {
        IColumnCodec codec = Codec(type);

        Assert.That(codec.CanWriteElementType(codec.ElementType), Is.True);
    }

    [TestCase("DateTime('UTC')", typeof(DateTimeOffset))]
    [TestCase("DateTime('UTC')", typeof(DateTime))]
    [TestCase("DateTime64(3, 'UTC')", typeof(DateTimeOffset))]
    [TestCase("DateTime64(3, 'UTC')", typeof(DateTime))]
    [TestCase("Time", typeof(TimeSpan))]
    [TestCase("Time64(3)", typeof(TimeSpan))]
    public void CanWriteElementType_LeafConvenienceType_IsAccepted(string type, Type candidate)
        => Assert.That(Codec(type).CanWriteElementType(candidate), Is.True);

    [TestCase("UInt64", typeof(DateTime))]
    [TestCase("String", typeof(Guid))]
    [TestCase("DateTime('UTC')", typeof(TimeSpan))]
    [TestCase("Time", typeof(DateTime))]
    public void CanWriteElementType_ATypeTheLeafDoesNotEncode_IsRefused(string type, Type candidate)
        => Assert.That(Codec(type).CanWriteElementType(candidate), Is.False);

    [TestCase("Array(DateTime('UTC'))", typeof(DateTime[]), true)]
    [TestCase("Array(DateTime('UTC'))", typeof(DateTimeOffset[]), true)]
    [TestCase("Array(DateTime('UTC'))", typeof(Guid[]), false)]
    [TestCase("Array(DateTime('UTC'))", typeof(DateTime), false)]
    [TestCase("Array(String)", typeof(DateTime[]), false)]
    [TestCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, DateTime>[]), true)]
    [TestCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, Guid>[]), false)]
    [TestCase("Map(String, DateTime('UTC'))", typeof(Tuple<string, DateTime>[]), false)]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<DateTime, string>), true)]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<DateTime>), false)]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof(Tuple<DateTime, string>), false)]
    public void CanWriteElementType_Container_AsksItsChildren(string type, Type candidate, bool accepted)
        => Assert.That(Codec(type).CanWriteElementType(candidate), Is.EqualTo(accepted));

    [Test]
    public void CanWriteElementType_ArrayGivenAMultiDimensionType_IsRefused()
        => Assert.That(Codec("Array(DateTime('UTC'))").CanWriteElementType(typeof(DateTime[,])), Is.False);

    [Test]
    public void CanWriteElementType_ArrayGivenANonZeroBasedRankOneType_IsRefused()
        => Assert.That(Codec("Array(DateTime('UTC'))").CanWriteElementType(typeof(DateTime).MakeArrayType(1)), Is.False);

    [Test]
    public void CanWriteElementType_MapGivenANonZeroBasedRankOnePairArray_IsRefused()
        => Assert.That(
            Codec("Map(String, DateTime('UTC'))").CanWriteElementType(typeof(KeyValuePair<string, DateTime>).MakeArrayType(1)),
            Is.False);

    [Test]
    public void CanWriteElementType_LowCardinalityOverACalendarInner_AcceptsTheCalendarType()
    {
        IColumnCodec codec = Codec("LowCardinality(DateTime('UTC'))");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWriteElementType(typeof(DateTime)), Is.True);
            Assert.That(codec.CanWriteElementType(typeof(DateTimeOffset)), Is.True);
            Assert.That(codec.CanWriteElementType(typeof(uint)), Is.True);
            Assert.That(codec.CanWriteElementType(typeof(Guid)), Is.False);
        });
    }

    [Test]
    public void CanWriteElementType_NullableLowCardinalityOverACalendarInner_AcceptsTheNullableCalendarType()
    {
        IColumnCodec codec = Codec("LowCardinality(Nullable(DateTime('UTC')))");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWriteElementType(typeof(DateTime?)), Is.True);
            Assert.That(codec.CanWriteElementType(typeof(uint?)), Is.True);
        });
    }

    [TestCase("Nullable(DateTime('UTC'))", typeof(DateTime))]
    [TestCase("Nullable(Int32)", typeof(int))]
    [TestCase("LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTime))]
    [TestCase("Array(Nullable(Int32))", typeof(int[]))]
    [TestCase("Array(Nullable(DateTime('UTC')))", typeof(DateTime[]))]
    [TestCase("Tuple(Nullable(Int32), String)", typeof(ValueTuple<int, string>))]
    public void CanWriteElementType_NullBearingColumnGivenABareValueType_IsRefused(string type, Type candidate)
        => Assert.That(Codec(type).CanWriteElementType(candidate), Is.False);

    [Test]
    public void CanWriteElementType_NullableOverAReferenceInner_AcceptsTheReferenceType()
        => Assert.That(Codec("Nullable(String)").CanWriteElementType(typeof(string)), Is.True);

    [Test]
    public void CanWriteElementType_NullableTuple_AsksEachTupleField()
    {
        IColumnCodec codec = Codec("Nullable(Tuple(DateTime('UTC'), String))");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWriteElementType(typeof((DateTime, string)?)), Is.True);
            Assert.That(codec.CanWriteElementType(typeof((DateTimeOffset, string)?)), Is.True);
            Assert.That(codec.CanWriteElementType(typeof((Guid, string)?)), Is.False);
            Assert.That(codec.CanWriteElementType(typeof((DateTime, string))), Is.False);
        });
    }

    [TestCase("Nested(a UInt8)")]
    [TestCase("Nothing")]
    public void CanWriteElementType_CodecNeedingItsOwnColumnShape_RefusesEveryElementType(string type)
    {
        IColumnCodec codec = Codec(type);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWriteElementType(codec.ElementType), Is.False);
            Assert.That(codec.CanWriteElementType(typeof(object)), Is.False);
            Assert.That(codec.CanWriteElementType(typeof(object[][])), Is.False);
        });
    }

    [TestCase("Array(DateTime('UTC'))", typeof(uint[]))]
    [TestCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, uint>[]))]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<uint, string>))]
    [TestCase("LowCardinality(DateTime('UTC'))", typeof(uint))]
    public void WritableElementTypes_ContainerThatLifts_StillReportsOnlyItsElementType(string type, Type elementType)
    {
        IColumnCodec codec = Codec(type);

        Assert.Multiple(() =>
        {
            Assert.That(codec.WritableElementTypes, Is.EqualTo(new[] { elementType }));
            Assert.That(codec.ElementType, Is.EqualTo(elementType));
        });
    }
}
