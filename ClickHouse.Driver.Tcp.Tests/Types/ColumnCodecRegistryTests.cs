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

    // Use a parseable synthetic type to reach the unsupported-type fallback.
    [Test]
    public void Resolve_UnsupportedButWellFormedType_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => ColumnCodecRegistry.Default.Resolve("NotAType(UInt8)", default));

    [Test]
    public void Resolve_MalformedType_ThrowsFormat()
        => Assert.Throws<FormatException>(() => ColumnCodecRegistry.Default.Resolve(string.Empty, default));

    // Geo codecs must retain their alias names for diagnostics.
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

    // Resolution pins Geometry's alternative order; a client-only round trip cannot detect transposition.
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


    // SimpleAggregateFunction must resolve directly to its fully parsed inner codec, including aliases,
    // composites, and parameterized functions.
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

    // AggregateFunction is unsupported, but diagnostics must suggest valid Merge syntax. A leading integer is a
    // serialization version; parameterized functions keep their parameters on the Merge combinator.
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
        // Add the outer type because the unsupported child alone does not identify the caller's declaration.
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

    // Alias resolution stamps the codec with the canonical name.
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
        // Child nodes resolve through the same alias registry.
        IColumnCodec written = ColumnCodecRegistry.Default.Resolve(
            "Array(Map(String, Tuple(Int32, BIGINT)))",
            ResolveContext.ForWrite);
        IColumnCodec canonical = ColumnCodecRegistry.Default.Resolve(
            "Array(Map(String, Tuple(Int32, Int64)))",
            ResolveContext.ForWrite);

        Assert.Multiple(() =>
        {
            Assert.That(written.ElementType, Is.EqualTo(canonical.ElementType));

            // Canonicalize only the aliased node, not the entire type tree.
            Assert.That(written.TypeName, Is.EqualTo("Array(Map(String, Tuple(Int32, BIGINT)))"));
        });
    }

    // Resolve registered names case-insensitively and stamp only that node with the registered spelling.
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

    // A bare Enum uses Enum8 when all ordinals fit, otherwise Enum16.
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
    /// Verifies the exception type for valid server forms the client intentionally does not support.
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
