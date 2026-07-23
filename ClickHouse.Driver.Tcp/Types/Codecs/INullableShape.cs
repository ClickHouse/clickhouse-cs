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

    /// <summary>Whether the value at <paramref name="row"/> of <paramref name="column"/> is null.</summary>
    bool IsNull(IColumn column, int row);

    /// <summary>
    /// Writes the full nullable body for rows [<paramref name="start"/>, start + length): the null-map, then the
    /// inner-type values with a placeholder at each null row. The column is always the dense nullable column (inner
    /// column + null-map) — the pipeline densifies before the write — so it is written with no intermediate copy.
    /// </summary>
    void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length);

    /// <summary>
    /// Projects an ergonomic column of this element type (the <c>T?</c> / nullable-reference form) into the dense
    /// nullable column — an inner column holding a real value at every row (the inner codec's placeholder at the
    /// null rows) paired with the null-map — recursing the inner codec's own <see cref="IColumnCodec.TryDensify"/>.
    /// A column already in dense form is returned unchanged with <paramref name="built"/> = <see langword="false"/>;
    /// a freshly built column sets <paramref name="built"/> = <see langword="true"/> and transfers ownership to the caller.
    /// </summary>
    IColumn TryDensify(IColumnCodec inner, IColumn column, out bool built);

    /// <summary>The inner encoded byte length of a present (non-null) row's value.</summary>
    long MeasureInnerRow(IColumnCodec inner, IColumn column, int row);

    /// <summary>The inner encoded byte length of the inner codec's null placeholder (what a null row writes).</summary>
    long MeasureNullPlaceholder(IColumnCodec inner);
}
