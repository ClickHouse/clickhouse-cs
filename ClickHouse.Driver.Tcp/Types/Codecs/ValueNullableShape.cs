using System;
using System.Buffers;
using System.Runtime.CompilerServices;
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
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (column is NullableValueColumn<T> dense)
        {
            WriteDense(inner, writer, dense, start, length);
        }
        else
        {
            WriteErgonomic(inner, writer, (IColumn<T?>)column, start, length);
        }
    }

    // Dense form (the wire's own layout): the null-map is already bytes and the inner column already holds a
    // value at every row, so write both directly with no intermediate copy.
    private static void WriteDense(IColumnCodec inner, ClickHouseBinaryWriter writer, NullableValueColumn<T> dense, int start, int length)
    {
        writer.WriteBytes(dense.NullMap.Slice(start, length));
        inner.WriteColumn(writer, dense.Inner, start, length);
    }

    // Ergonomic T? form: emit the null-map per row, then materialize the values into a pooled inner-typed buffer
    // (T? cannot be reinterpreted as a contiguous T, so this copy is unavoidable), substituting the inner codec's
    // own null placeholder — in this write type T — at the null rows.
    private static void WriteErgonomic(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn<T?> source, int start, int length)
    {
        for (int i = 0; i < length; i++)
        {
            writer.WriteBool(!source[start + i].HasValue);
        }

        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        T[] rented = ArrayPool<T>.Shared.Rent(length);
        try
        {
            for (int i = 0; i < length; i++)
            {
                rented[i] = source[start + i].GetValueOrDefault(placeholder);
            }

            inner.WriteColumn(writer, ArrayColumn<T>.OverBuffer(source.Name, inner.TypeName, rented, length), 0, length);
        }
        finally
        {
            ArrayPool<T>.Shared.Return(rented, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
        }
    }

    /// <inheritdoc/>
    // Same projection as WriteErgonomic, but materialized once into the dense column instead of written: split the
    // T? values into an inner T column (the inner codec's placeholder at the null rows) plus the null-map, recurse
    // the inner codec's own Densify (a no-op for a leaf inner), and wrap. An already-dense column is returned as-is.
    public IColumn Densify(IColumnCodec inner, IColumn column)
    {
        if (column is NullableValueColumn<T>)
        {
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

        IColumn innerColumn = inner.Densify(new ArrayColumn<T>(source.Name, inner.TypeName, values));
        return new NullableValueColumn<T>(source.Name, source.TypeName, (IColumn<T>)innerColumn, nullMap, rowCount, pooledMap: false);
    }

    /// <inheritdoc/>
    // A value-type inner is fixed-width, so its per-row size is column-independent — read it straight off the
    // inner codec rather than passing the T? column through (the inner's IColumn<T> view could not accept it).
    public long MeasureInnerRow(IColumnCodec inner, IColumn column, int row) => inner.FixedRowByteSize ?? inner.MeasureRowBytes(column, row);
}
