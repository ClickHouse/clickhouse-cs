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

    // Geometry expands to a Variant the header never spells out. The client applies its own alternative order to
    // the write and to the read, so no round trip can see a transposition of Ring with LineString or Polygon with
    // MultiLineString: the same wrong order on both sides returns the value intact, and each pair is
    // byte-identical so the server never objects. GeometryIntegrationTests pins the order by asking the server.
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
    // handled plain type names would fail there and nowhere else. The composite inners pin that the whole parsed
    // node reaches the registry with its own arguments, not just its name — including when the function itself is
    // parameterized, which parses as a node with arguments of its own and must not be read as a second inner type.
    [TestCase("SimpleAggregateFunction(sum, UInt64)", "UInt64")]
    [TestCase("SimpleAggregateFunction(anyLast, Point)", "Point")]
    [TestCase("SimpleAggregateFunction(groupArrayArray, Array(UInt64))", "Array(UInt64)")]
    [TestCase("SimpleAggregateFunction(maxMap, Map(String, UInt64))", "Map(String, UInt64)")]
    [TestCase("SimpleAggregateFunction(groupArrayLastArray(10), Array(String))", "Array(String)")]
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
    // A leading integer is a serialization version rather than a function: 26.6 reports sumMapState(...) as
    // AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)), and sumMapMerge / sumMapFilteredMerge([1, 2])
    // are the queries that run — both verified on 26.6.
    [TestCase("AggregateFunction(sum, UInt64)", "sumMerge(column)")]
    [TestCase("AggregateFunction(quantiles(0.5, 0.9), UInt64)", "quantilesMerge(0.5, 0.9)(column)")]
    [TestCase("AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))", "sumMapMerge(column)")]
    [TestCase("AggregateFunction(1, sumMapFiltered([1, 2]), Array(UInt64), Array(UInt64))", "sumMapFilteredMerge([1, 2])(column)")]
    public void Resolve_AggregateFunction_ThrowsSuggestingAMergeQueryThatRuns(string type, string expectedHint)
    {
        var exception = Assert.Throws<NotSupportedException>(() => ColumnCodecRegistry.Default.Resolve(type, default));
        Assert.That(exception.Message, Does.Contain(expectedHint));
    }

    [Test]
    public void Resolve_AggregateFunctionWithASerializationVersion_NamesTheFunctionAndNotTheVersion()
    {
        var exception = Assert.Throws<NotSupportedException>(
            () => ColumnCodecRegistry.Default.Resolve("AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64))", default));

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("'sumMap' aggregate function"));
            Assert.That(exception.Message, Does.Not.Contain("'1' aggregate function"));
        });
    }

    [Test]
    public void Resolve_UnsupportedChildType_NamesTheOuterTypeAsWell()
    {
        // The refusal comes from the child, and 'MultiPoint' on its own sends a caller searching their code for
        // it. No "yet" either: MultiPoint is in no version's system.data_type_families (26.6 answers "Unknown
        // data type family") and Object('json') was removed, so some of what lands here is not coming.
        var exception = Assert.Throws<NotSupportedException>(
            () => ColumnCodecRegistry.Default.Resolve("Map(String, Array(MultiPoint))", default));

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("'MultiPoint'"));
            Assert.That(exception.Message, Does.Contain("'Map(String, Array(MultiPoint))'"));
            Assert.That(exception.Message, Does.Not.Contain("yet"));
            Assert.That(exception.InnerException, Is.TypeOf<NotSupportedException>(), "the child's own refusal is kept");
        });
    }

    [Test]
    public void Resolve_UnsupportedTopLevelType_NamesItOnce()
    {
        // The type the caller wrote is the type that failed, so there is no outer type to add.
        var exception = Assert.Throws<NotSupportedException>(() => ColumnCodecRegistry.Default.Resolve("MultiPoint", default));

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("'MultiPoint'").And.Not.Contain("inside"));
            Assert.That(exception.InnerException, Is.Null);
        });
    }

    // system.data_type_families on 26.6, as the names a caller writes rather than as a header ever carries them.
    // The canonical name is what the codec is stamped with, so DEC(4, 2) reports itself as Decimal(4, 2).
    [TestCase("VARCHAR", "String")]
    [TestCase("nchar varying(456)", "String")]
    [TestCase("BIGINT", "Int64")]
    [TestCase("TINYINT UNSIGNED", "UInt8")]
    [TestCase("DOUBLE PRECISION", "Float64")]
    [TestCase("BINARY(10)", "FixedString(10)")]
    [TestCase("DEC(4, 2)", "Decimal(4, 2)")]
    [TestCase("TIMESTAMP", "DateTime")]
    [TestCase("INET4", "IPv4")]
    [TestCase("Boolean", "Bool")]
    [TestCase("GEOMETRY", "Geometry")]
    [TestCase("DateTime32", "DateTime")]
    [TestCase("datetime64(3)", "DateTime64(3)")]
    [TestCase("json", "JSON")]
    [TestCase("DATE", "Date")]
    [TestCase("time64(9)", "Time64(9)")]
    public void Resolve_AliasOrCaseVariant_ResolvesToTheCanonicalType(string written, string canonical)
        => Assert.That(ColumnCodecRegistry.Default.Resolve(written, ResolveContext.ForWrite).TypeName, Is.EqualTo(canonical));

    [Test]
    public void Resolve_AliasInsideAComposite_ResolvesThroughTheChildNodes()
    {
        // Child nodes resolve through the same registry, so one table covers a composite. Checked on 26.6: a
        // column created as Array(Map(String, Tuple(Int32, BIGINT))) reports the Int64 spelling.
        IColumnCodec written = ColumnCodecRegistry.Default.Resolve(
            "Array(Map(String, Tuple(Int32, BIGINT)))",
            ResolveContext.ForWrite);
        IColumnCodec canonical = ColumnCodecRegistry.Default.Resolve(
            "Array(Map(String, Tuple(Int32, Int64)))",
            ResolveContext.ForWrite);

        Assert.Multiple(() =>
        {
            Assert.That(written.ElementType, Is.EqualTo(canonical.ElementType));

            // Only the node whose own name is the alias is rewritten, so a composite keeps the spelling it was
            // given in its type name. Canonicalizing the whole tree would mean walking it on every resolve,
            // which is the read path, to tidy a name no header ever carries.
            Assert.That(written.TypeName, Is.EqualTo("Array(Map(String, Tuple(Int32, BIGINT)))"));
        });
    }

    // The server marks only some families case_insensitive, and this client does not copy that rule: which
    // spellings a server takes is its own business, it changes between versions and settings, and it reports a
    // name it rejects far better than a stale copy of the rule here could. So any case resolves, and the codec
    // is stamped with the registered spelling.
    // As with an alias, only the node whose own name was rewritten reports the registered spelling: a child
    // keeps what the caller wrote, because canonicalizing a whole tree would mean walking it on every resolve.
    [TestCase("string", "String")]
    [TestCase("int64", "Int64")]
    [TestCase("array(uint8)", "Array(uint8)")]
    [TestCase("nullable(string)", "Nullable(string)")]
    [TestCase("geometry", "Geometry")]
    [TestCase("VARIANT(Int64, String)", "Variant(Int64, String)")]
    public void Resolve_AnyCaseOfAKnownName_Resolves(string written, string expectedTypeName)
        => Assert.That(
            ColumnCodecRegistry.Default.Resolve(written, ResolveContext.ForWrite).TypeName,
            Is.EqualTo(expectedTypeName));

    [Test]
    public void Resolve_AggregateFunctionNamingNoFunction_ThrowsFormat()
        => Assert.Throws<FormatException>(() => ColumnCodecRegistry.Default.Resolve("AggregateFunction()", default));

    // A bare Enum names no width, so the client has to pick the one the server would. Verified on 26.6: Enum8
    // while every ordinal is in the Int8 range, Enum16 otherwise, and the column is reported under the width
    // chosen. No round trip can check this — the server normalizes the name away — so the codec's own name is
    // the only place the choice is visible.
    [TestCase("Enum('A' = 1, 'B' = 2)", "Enum8('A' = 1, 'B' = 2)")]
    [TestCase("Enum('A' = -128, 'B' = 127)", "Enum8('A' = -128, 'B' = 127)")]
    [TestCase("Enum('A' = 1, 'B' = 200)", "Enum16('A' = 1, 'B' = 200)")]
    [TestCase("Enum('A' = -129)", "Enum16('A' = -129)")]
    [TestCase("enum('a' = 1)", "Enum8('a' = 1)")]
    public void Resolve_BareEnum_PicksTheWidthTheServerWould(string written, string expectedTypeName)
        => Assert.That(ColumnCodecRegistry.Default.Resolve(written, ResolveContext.ForWrite).TypeName, Is.EqualTo(expectedTypeName));

    [Test]
    public void Resolve_BareEnumDeclaringNoMembers_ThrowsFormat()
        => Assert.Throws<FormatException>(() => ColumnCodecRegistry.Default.Resolve("Enum()", ResolveContext.ForWrite));

    /// <summary>
    /// Forms a real server accepts and this client refuses on purpose. Each one either has no generator — no
    /// header carries it and no tool writes it, so accepting it would only hide a typo in a hand-written type —
    /// or needs grammar this client has no caller for. The assertion is the refusal *type*, because that is what
    /// separates "malformed" from "not supported" for anyone catching them.
    /// </summary>
    [TestCase("Enum8('a', 'b')", typeof(FormatException), TestName = "Enum members with implicit ordinals")]
    [TestCase("DateTime64", typeof(FormatException), TestName = "DateTime64 with no scale, which the server defaults to 3")]
    [TestCase("Decimal", typeof(FormatException), TestName = "Decimal with no precision, which the server defaults to (10, 0)")]
    [TestCase("Decimal(4)", typeof(FormatException), TestName = "Decimal with no scale, which the server defaults to 0")]
    [TestCase("Tuple(UInt8,)", typeof(FormatException), TestName = "A trailing comma")]
    [TestCase("Array(/* c */ String)", typeof(NotSupportedException), TestName = "A comment inside a type")]
    [TestCase("`Int64`", typeof(NotSupportedException), TestName = "A backticked type name")]
    [TestCase("\"String\"", typeof(NotSupportedException), TestName = "A double-quoted type name")]
    [TestCase("INT(10) UNSIGNED", typeof(FormatException), TestName = "A trailing word after the arguments")]
    public void Resolve_FormTheServerAcceptsAndThisClientDoesNot_RefusesWithTheTypeThatSaysWhy(string written, Type refusal)
        => Assert.That(
            Assert.Throws(refusal, () => ColumnCodecRegistry.Default.Resolve(written, ResolveContext.ForWrite)).Message,
            Does.Contain(written),
            "a refusal that does not quote what was written leaves the caller nothing to search for");
}
