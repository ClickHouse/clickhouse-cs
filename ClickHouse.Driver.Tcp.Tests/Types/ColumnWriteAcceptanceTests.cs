using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers <see cref="IColumnCodec.CanWriteElementType"/> — the authority on which CLR element types a codec can be
/// written from, and the write-side counterpart of <see cref="IColumnCodec.TryProjectRead"/>.
///
/// <para>
/// This is API surface a server round-trip cannot observe: a refusal never reaches the wire, and a codec that accepted
/// too much would fail mid-insert rather than at plan build. The composite recursion at depth is swept by
/// <see cref="CompositeLiftMatrixTests"/>; here are the per-shape rules and the refusals.
/// </para>
/// </summary>
[TestFixture]
public class ColumnWriteAcceptanceTests
{
    private static IColumnCodec Codec(string type)
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = "UTC" });

    /// <summary>Every codec can be written from its own canonical element type, whatever else it accepts.</summary>
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

    /// <summary>A leaf's convenience write types are accepted; its <see cref="IColumnCodec.WriteColumn"/> converts them.</summary>
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

    /// <summary>
    /// A container asks its children about the matching part of the type, so it accepts a lifted element and refuses one
    /// its child refuses.
    /// </summary>
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

    /// <summary>A rank-2 array is not an <c>Array(T)</c> row, so it is refused rather than read as one.</summary>
    [Test]
    public void CanWriteElementType_ArrayGivenAMultiDimensionType_IsRefused()
        => Assert.That(Codec("Array(DateTime('UTC'))").CanWriteElementType(typeof(DateTime[,])), Is.False);

    /// <summary>
    /// The asymmetry this closes: a <c>LowCardinality(DateTime)</c> column read into a <see cref="DateTime"/> property
    /// but could be written only from raw epoch seconds. A non-nullable <c>LowCardinality</c> surfaces its inner's
    /// element type unchanged, so whatever the inner accepts, it accepts.
    /// </summary>
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

    /// <summary>
    /// A column with null rows refuses a bare value type: it has nowhere to put a null. That refusal is what keeps the
    /// write side inside the read side, so a property that inserts can always be selected back.
    /// </summary>
    [TestCase("Nullable(DateTime('UTC'))", typeof(DateTime))]
    [TestCase("Nullable(Int32)", typeof(int))]
    [TestCase("LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTime))]
    [TestCase("Array(Nullable(Int32))", typeof(int[]))]
    [TestCase("Array(Nullable(DateTime('UTC')))", typeof(DateTime[]))]
    [TestCase("Tuple(Nullable(Int32), String)", typeof(ValueTuple<int, string>))]
    public void CanWriteElementType_NullBearingColumnGivenABareValueType_IsRefused(string type, Type candidate)
        => Assert.That(Codec(type).CanWriteElementType(candidate), Is.False);

    /// <summary>
    /// A nullable wrapper over a reference inner holds the null itself, so the unwrapped reference type stays
    /// acceptable — the refusal above is about value types with no null, not about nullability in general.
    /// </summary>
    [Test]
    public void CanWriteElementType_NullableOverAReferenceInner_AcceptsTheReferenceType()
        => Assert.That(Codec("Nullable(String)").CanWriteElementType(typeof(string)), Is.True);

    /// <summary>
    /// <c>Nested</c> is written only from its own wire-shaped column and <c>Nothing</c> has no values at all, so neither
    /// names an element type a row-oriented insert could gather.
    /// </summary>
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

    /// <summary>
    /// The diagnostics list stays canonical-only for a container: the honest list is its children's product, and
    /// <see cref="IColumnCodec.CanWriteElementType"/> is the authority that accepts shapes the list omits.
    /// </summary>
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
