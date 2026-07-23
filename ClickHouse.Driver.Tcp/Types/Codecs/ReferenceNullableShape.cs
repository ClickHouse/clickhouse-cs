using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The shape for a reference-type inner: the nullable column surfaces the nullable reference.</summary>
/// <typeparam name="T">The inner reference type.</typeparam>
internal sealed class ReferenceNullableShape<T> : INullableShape
    where T : class
{
    /// <inheritdoc/>
    public Type NullableElementType => typeof(T);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn inner, byte[] nullMap, int rowCount, bool pooledMap)
        => new NullableReferenceColumn<T>(name, typeName, (IColumn<T>)inner, nullMap, rowCount, pooledMap);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<T>;

    /// <inheritdoc/>
    public bool CanInnerWrite(IColumnCodec inner) => inner.CanWrite(new ArrayColumn<T>(string.Empty, inner.TypeName, Array.Empty<T>()));

    /// <inheritdoc/>
    public long MeasureNullPlaceholder(IColumnCodec inner)
        => inner.MeasureRowBytes(new ArrayColumn<T>(string.Empty, inner.TypeName, new[] { (T)inner.NullPlaceholderAs(typeof(T)) }), 0);

    /// <inheritdoc/>
    public bool IsNull(IColumn column, int row) => ((IColumn<T>)column)[row] is null;

    /// <inheritdoc/>
    // Every column is densified before the write, so the body is always the dense (inner column + null-map) shape:
    // the inner column already holds a value at every row (a placeholder at the null rows), so the null-map bytes
    // and the inner column are written directly with no copy. The ergonomic nullable-reference form was projected
    // into this shape by TryDensify.
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var dense = (NullableReferenceColumn<T>)column;
        writer.WriteBytes(dense.NullMap.Slice(start, length));
        inner.WriteColumn(writer, dense.Inner, start, length);
    }

    /// <inheritdoc/>
    // Same projection as WriteErgonomic, but materialized once into the dense column instead of written: split the
    // nullable-reference values into an inner T column (the inner codec's placeholder at the null rows) plus the
    // null-map, recurse the inner codec's own TryDensify (a no-op for a leaf inner), and wrap. An already-dense
    // column is returned as-is (built = false).
    public IColumn TryDensify(IColumnCodec inner, IColumn column, out bool built)
    {
        if (column is NullableReferenceColumn<T>)
        {
            built = false;
            return column;
        }

        var source = (IColumn<T>)column;
        int rowCount = source.RowCount;
        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        var nullMap = new byte[rowCount];
        var values = new T[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            T value = source[i];
            nullMap[i] = value is null ? (byte)1 : (byte)0;
            values[i] = value ?? placeholder;
        }

        IColumn innerColumn = inner.TryDensify(new ArrayColumn<T>(source.Name, inner.TypeName, values), out _);
        built = true;
        return new NullableReferenceColumn<T>(source.Name, source.TypeName, (IColumn<T>)innerColumn, nullMap, rowCount, pooledMap: false);
    }

    /// <inheritdoc/>
    // Reached only for a present (non-null) row of a variable-width inner (String); null rows are priced by the
    // codec via MeasureNullPlaceholder, so the inner codec never measures a null value.
    public long MeasureInnerRow(IColumnCodec inner, IColumn column, int row) => inner.MeasureRowBytes(column, row);
}
