using System;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class ColumnCodecRegistryTests
{
    [TestCase("UInt8")]
    [TestCase("Int8")]
    [TestCase("UInt16")]
    [TestCase("Int16")]
    [TestCase("UInt32")]
    [TestCase("Int32")]
    [TestCase("UInt64")]
    [TestCase("Int64")]
    [TestCase("UInt128")]
    [TestCase("Int128")]
    [TestCase("UInt256")]
    [TestCase("Int256")]
    [TestCase("String")]
    public void Resolve_SupportedType_ReturnsCodecWithMatchingTypeName(string type)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, default);
        Assert.That(codec.TypeName, Is.EqualTo(type));
    }

    [Test]
    public void Resolve_DateTimeWithTimezone_StampsFullTypeName()
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve("DateTime('UTC')", default);
        Assert.That(codec.TypeName, Is.EqualTo("DateTime('UTC')"));
    }

    // A parseable type the registry has no factory for. Deliberately not a real ClickHouse type: every real one this
    // stood for has since been implemented, and the fallback itself is what the test is about.
    [Test]
    public void Resolve_UnsupportedButWellFormedType_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => ColumnCodecRegistry.Default.Resolve("NotAType(UInt8)", default));

    [Test]
    public void Resolve_MalformedType_ThrowsFormat()
        => Assert.Throws<FormatException>(() => ColumnCodecRegistry.Default.Resolve(string.Empty, default));

    // The geo aliases name structures the client already encodes. The round-trip corpus proves the values; what
    // only a resolution test reaches is that the alias survives as the codec's own name — a codec reporting
    // "Tuple(Float64, Float64)" would still round-trip, and would still misname the type in every diagnostic.
    [TestCase("Point", typeof((double, double)))]
    [TestCase("Ring", typeof((double, double)[]))]
    [TestCase("LineString", typeof((double, double)[]))]
    [TestCase("Polygon", typeof((double, double)[][]))]
    [TestCase("MultiLineString", typeof((double, double)[][]))]
    [TestCase("MultiPolygon", typeof((double, double)[][][]))]
    public void Resolve_GeoAlias_KeepsTheAliasNameAndSurfacesTheStructuralType(string type, Type elementType)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, default);
        Assert.Multiple(() =>
        {
            Assert.That(codec.TypeName, Is.EqualTo(type));
            Assert.That(codec.ElementType, Is.EqualTo(elementType));
        });
    }

    // Geometry expands to a Variant the header never spells out. The discriminator order is pinned against the
    // server by GeometryIntegrationTests, which reads variantType() back — not by the round-trip corpus, whose
    // value comparison cannot see a transposition of Ring with LineString or Polygon with MultiLineString: those
    // pairs are byte-identical, so swapping them emits the same block and reads back the same CLR values.
    [Test]
    public void Resolve_Geometry_KeepsTheAliasNameAndSurfacesTheVariantType()
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve("Geometry", default);
        Assert.Multiple(() =>
        {
            Assert.That(codec.TypeName, Is.EqualTo("Geometry"));
            Assert.That(codec.ElementType, Is.EqualTo(typeof(object)));
        });
    }


    // SimpleAggregateFunction encodes as its inner type, so it must resolve to the inner codec *itself* rather than
    // to a renaming wrapper — which the corpus cannot tell apart, since a wrapper would round-trip identically.
    // The aliased inner additionally pins that the inner goes back through the registry: a resolver that only
    // handled plain type names would fail there and nowhere else.
    [TestCase("SimpleAggregateFunction(sum, UInt64)", "UInt64")]
    [TestCase("SimpleAggregateFunction(anyLast, Point)", "Point")]
    public void Resolve_SimpleAggregateFunction_ResolvesToTheInnerTypesCodec(string type, string innerTypeName)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, default);
        Assert.That(codec.TypeName, Is.EqualTo(innerTypeName));
    }

    [TestCase("SimpleAggregateFunction(sum)")]
    [TestCase("SimpleAggregateFunction(sum, UInt64, UInt8)")]
    public void Resolve_SimpleAggregateFunctionWithoutExactlyOneInnerType_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => ColumnCodecRegistry.Default.Resolve(type, default));

    // AggregateFunction holds the function's own intermediate state, which no generic codec decodes. The hint has to
    // be a query that actually runs: the combinator attaches to the bare name, and a parameterized function keeps its
    // parameters in their own list ahead of the column. "quantiles(0.5, 0.9)Merge" is not a function, and a bare
    // "quantilesMerge(column)" is rejected by the server for wanting its parameters — verified on 26.6.
    [TestCase("AggregateFunction(sum, UInt64)", "sumMerge(column)")]
    [TestCase("AggregateFunction(quantiles(0.5, 0.9), UInt64)", "quantilesMerge(0.5, 0.9)(column)")]
    public void Resolve_AggregateFunction_ThrowsSuggestingAMergeQueryThatRuns(string type, string expectedHint)
    {
        var exception = Assert.Throws<NotSupportedException>(() => ColumnCodecRegistry.Default.Resolve(type, default));
        Assert.That(exception.Message, Does.Contain(expectedHint));
    }

    [Test]
    public void Resolve_AggregateFunctionNamingNoFunction_ThrowsFormat()
        => Assert.Throws<FormatException>(() => ColumnCodecRegistry.Default.Resolve("AggregateFunction()", default));
}
