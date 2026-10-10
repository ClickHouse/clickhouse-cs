using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The generic bridge for one nullable element type: it builds the typed wrapper column of a read. One implementation
/// covers value-type inners (surfacing <c>T?</c>), another reference-type inners; the concrete instance is chosen once
/// per element type.
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
}
