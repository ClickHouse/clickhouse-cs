using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The shape for a value-type inner: the nullable column surfaces <c>T?</c>.</summary>
/// <typeparam name="T">The inner value type.</typeparam>
internal sealed class ValueNullableShape<T> : INullableShape
    where T : struct
{
    /// <inheritdoc/>
    public Type NullableElementType => typeof(T?);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn inner, byte[] nullMap, int rowCount, bool pooledMap)
        => new NullableValueColumn<T>(name, typeName, (IColumn<T>)inner, nullMap, rowCount, pooledMap);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<T?>;

    /// <inheritdoc/>
    public bool CanInnerWrite(IColumnCodec inner) => inner.CanWrite(new ArrayColumn<T>(string.Empty, inner.TypeName, Array.Empty<T>()));

    /// <inheritdoc/>
    public long MeasureNullPlaceholder(IColumnCodec inner)
        => inner.MeasureRowBytes(new ArrayColumn<T>(string.Empty, inner.TypeName, new[] { (T)inner.NullPlaceholderAs(typeof(T)) }), 0);

    /// <inheritdoc/>
    public bool IsNull(IColumn column, int row) => !((IColumn<T?>)column)[row].HasValue;

    /// <inheritdoc/>
    // Every column is densified before the write, so the body is always the dense (inner column + null-map) shape:
    // the null-map is already bytes and the inner column already holds a value at every row (a placeholder at the
    // null rows), so both are written directly with no intermediate copy. The ergonomic T? form was projected into
    // this shape by TryDensify.
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var dense = (NullableValueColumn<T>)column;
        writer.WriteBytes(dense.NullMap.Slice(start, length));
        inner.WriteColumn(writer, dense.Inner, start, length);
    }

    /// <inheritdoc/>
    // Same projection as WriteErgonomic, but materialized once into the dense column instead of written: split the
    // T? values into an inner T column (the inner codec's placeholder at the null rows) plus the null-map, recurse
    // the inner codec's own TryDensify (a no-op for a leaf inner), and wrap. An already-dense column is returned
    // as-is (built = false).
    public IColumn TryDensify(IColumnCodec inner, IColumn column, out bool built)
    {
        if (column is NullableValueColumn<T>)
        {
            built = false;
            return column;
        }

        var source = (IColumn<T?>)column;
        int rowCount = source.RowCount;
        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        var nullMap = new byte[rowCount];
        var values = new T[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            T? value = source[i];
            nullMap[i] = value.HasValue ? (byte)0 : (byte)1;
            values[i] = value.GetValueOrDefault(placeholder);
        }

        IColumn innerColumn = inner.TryDensify(new ArrayColumn<T>(source.Name, inner.TypeName, values), out _);
        built = true;
        return new NullableValueColumn<T>(source.Name, source.TypeName, (IColumn<T>)innerColumn, nullMap, rowCount, pooledMap: false);
    }

    /// <inheritdoc/>
    // A value-type inner is fixed-width, so its per-row size is column-independent — read it straight off the
    // inner codec rather than passing the T? column through (the inner's IColumn<T> view could not accept it).
    public long MeasureInnerRow(IColumnCodec inner, IColumn column, int row) => inner.FixedRowByteSize ?? inner.MeasureRowBytes(column, row);
}
