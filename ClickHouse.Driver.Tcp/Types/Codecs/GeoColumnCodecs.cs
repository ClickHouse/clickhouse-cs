namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The geo type aliases. Each names a structure the client already encodes, so none needs a codec of its own —
/// only a registration that resolves its structure and keeps its own name:
///
/// <list type="table">
/// <item><term><c>Point</c></term><description><c>Tuple(Float64, Float64)</c> → <c>(double, double)</c></description></item>
/// <item><term><c>Ring</c>, <c>LineString</c></term><description><c>Array(Point)</c> → <c>(double, double)[]</c></description></item>
/// <item><term><c>Polygon</c>, <c>MultiLineString</c></term><description><c>Array(Ring)</c> / <c>Array(LineString)</c> → <c>(double, double)[][]</c></description></item>
/// <item><term><c>MultiPolygon</c></term><description><c>Array(Polygon)</c> → <c>(double, double)[][][]</c></description></item>
/// <item><term><c>Geometry</c></term><description>a <c>Variant</c> over the six above</description></item>
/// </list>
///
/// <para>
/// The bytes are the structure's, but the *name* on the wire is always the alias: a column header says
/// <c>Point</c>, never <c>Tuple(Float64, Float64)</c>. So the client expands the alias itself, and reports the
/// alias back as the codec's type name. That is also structurally what the HTTP driver surfaces, though that one
/// builds <c>System.Tuple</c> where this builds <c>ValueTuple</c>.
/// </para>
///
/// <para>
/// <c>Ring</c>/<c>LineString</c> and <c>Polygon</c>/<c>MultiLineString</c> are distinct types to the server and
/// identical to this client beyond their names. Inside <c>Geometry</c> that makes a value of either shared shape
/// ambiguous, so it can only be written from the dense column, whose discriminators name the alternative; a
/// <c>Point</c> or <c>MultiPolygon</c> is unique and still writes from an ergonomic one.
/// </para>
/// </summary>
internal static class GeoColumnCodecs
{
    // Each alias in the server's own terms, parsed once. The nested aliases name each other rather than spelling
    // out the whole structure, so the codec a Polygon resolves to holds a Ring codec that reports itself as "Ring".
    private static readonly TypeNode PointStructure = TypeParser.Parse("Tuple(Float64, Float64)");
    private static readonly TypeNode RingStructure = TypeParser.Parse("Array(Point)");
    private static readonly TypeNode LineStringStructure = TypeParser.Parse("Array(Point)");
    private static readonly TypeNode PolygonStructure = TypeParser.Parse("Array(Ring)");
    private static readonly TypeNode MultiLineStringStructure = TypeParser.Parse("Array(LineString)");
    private static readonly TypeNode MultiPolygonStructure = TypeParser.Parse("Array(Polygon)");

    // The order is the discriminator order the server sends: the canonical name-sorted one, confirmed against
    // variantType's Enum8 ('LineString' = 0 … 'Ring' = 5) and pinned by GeometryIntegrationTests.
    private static readonly TypeNode GeometryStructure =
        TypeParser.Parse("Variant(LineString, MultiLineString, MultiPolygon, Point, Polygon, Ring)");

    public static IColumnCodec CreatePoint(in ResolveContext context, ColumnCodecRegistry registry)
        => TupleColumnCodec.Create(PointStructure, in context, registry, "Point");

    public static IColumnCodec CreateRing(in ResolveContext context, ColumnCodecRegistry registry)
        => ArrayColumnCodec.Create(RingStructure, in context, registry, "Ring");

    public static IColumnCodec CreateLineString(in ResolveContext context, ColumnCodecRegistry registry)
        => ArrayColumnCodec.Create(LineStringStructure, in context, registry, "LineString");

    public static IColumnCodec CreatePolygon(in ResolveContext context, ColumnCodecRegistry registry)
        => ArrayColumnCodec.Create(PolygonStructure, in context, registry, "Polygon");

    public static IColumnCodec CreateMultiLineString(in ResolveContext context, ColumnCodecRegistry registry)
        => ArrayColumnCodec.Create(MultiLineStringStructure, in context, registry, "MultiLineString");

    public static IColumnCodec CreateMultiPolygon(in ResolveContext context, ColumnCodecRegistry registry)
        => ArrayColumnCodec.Create(MultiPolygonStructure, in context, registry, "MultiPolygon");

    public static IColumnCodec CreateGeometry(in ResolveContext context, ColumnCodecRegistry registry)
        => VariantColumnCodec.Create(GeometryStructure, in context, registry, "Geometry");
}
