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

    /// <summary>
    /// Wraps a decoded inner column and its null-map into the typed nullable column. The inner column's row count
    /// becomes the wrapper's, so the two cannot disagree; <paramref name="nullMap"/> may be longer (a pooled buffer)
    /// but not shorter.
    /// </summary>
    IColumn Wrap(string name, string typeName, IColumn inner, byte[] nullMap, bool pooledMap);

    /// <summary>Whether the inner codec can write <paramref name="column"/> through this nullable shape.</summary>
    bool CanWrite(IColumnCodec inner, IColumn column);

    /// <summary>
    /// Returns the inner column to use for this write. Dense columns expose their stored inner column; row-oriented
    /// columns return a lazy view that replaces nulls with a valid inner value.
    /// </summary>
    IColumn GetInnerColumn(IColumnCodec inner, IColumn column);

    /// <summary>Writes the null map for the requested rows.</summary>
    void WriteNullMap(ClickHouseBinaryWriter writer, IColumn column, int start, int length);
}
