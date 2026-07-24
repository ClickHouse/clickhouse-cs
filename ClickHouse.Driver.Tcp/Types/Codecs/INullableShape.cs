using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The generic bridge for one nullable element type: it knows how to build the typed wrapper column, test and
/// interrogate a writable column, and feed the inner codec. One implementation covers value-type inners
/// (surfacing <c>T?</c>), another reference-type inners; the concrete instance is chosen once per element type.
/// </summary>
internal interface INullableShape
{
    /// <summary>The CLR element type the wrapped column surfaces (<c>T?</c> for a value inner, <c>T</c> for a reference inner).</summary>
    Type NullableElementType { get; }

    /// <summary>Wraps a decoded inner column and its null-map into the typed nullable column.</summary>
    IColumn Wrap(string name, string typeName, IColumn inner, byte[] nullMap, int rowCount, bool pooledMap);

    /// <summary>Whether <paramref name="column"/> is a nullable column of this element type, writable by the codec.</summary>
    bool CanWrite(IColumn column);

    /// <summary>Whether the inner codec can write an inner-typed column at all (e.g. <c>Nothing</c> cannot).</summary>
    bool CanInnerWrite(IColumnCodec inner);

    /// <summary>
    /// Writes the full nullable body for rows [<paramref name="start"/>, start + length): the null-map, then the
    /// inner-type values with a placeholder at each null row. A dense nullable column (inner column + null-map) is
    /// written with no intermediate copy; the ergonomic <c>T?</c> / nullable-reference form writes the null-map from
    /// each row's nullness and hands the inner codec a substitute view (the inner placeholder at the null rows).
    /// </summary>
    void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length);
}
