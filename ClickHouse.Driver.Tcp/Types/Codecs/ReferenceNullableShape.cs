using System;
using System.Buffers;
using System.Runtime.CompilerServices;
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
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (column is NullableReferenceColumn<T> dense)
        {
            WriteDense(inner, writer, dense, start, length);
        }
        else
        {
            WriteErgonomic(inner, writer, (IColumn<T>)column, start, length);
        }
    }

    // Dense form (the wire's own layout): the inner column already holds a value at every row (a placeholder at
    // the null rows), so write the null-map bytes and the inner column with no copy.
    private static void WriteDense(IColumnCodec inner, ClickHouseBinaryWriter writer, NullableReferenceColumn<T> dense, int start, int length)
    {
        writer.WriteBytes(dense.NullMap.Slice(start, length));
        inner.WriteColumn(writer, dense.Inner, start, length);
    }

    // Ergonomic form (an inner-typed column with null entries): emit the null-map per row, then materialize the
    // values into a pooled buffer with the inner codec's own null placeholder at the null rows, so a null never
    // reaches the inner codec — a genuinely non-nullable inner column with a stray null still fails fast.
    private static void WriteErgonomic(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn<T> typed, int start, int length)
    {
        for (int i = 0; i < length; i++)
        {
            writer.WriteBool(typed[start + i] is null);
        }

        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        T[] rented = ArrayPool<T>.Shared.Rent(length);
        try
        {
            for (int i = 0; i < length; i++)
            {
                rented[i] = typed[start + i] ?? placeholder;
            }

            inner.WriteColumn(writer, ArrayColumn<T>.OverBuffer(typed.Name, inner.TypeName, rented, length), 0, length);
        }
        finally
        {
            ArrayPool<T>.Shared.Return(rented, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
        }
    }

    /// <inheritdoc/>
    // Same projection as WriteErgonomic, but materialized once into the dense column instead of written: split the
    // nullable-reference values into an inner T column (the inner codec's placeholder at the null rows) plus the
    // null-map, recurse the inner codec's own Densify (a no-op for a leaf inner), and wrap. An already-dense column
    // is returned as-is.
    public IColumn Densify(IColumnCodec inner, IColumn column)
    {
        if (column is NullableReferenceColumn<T>)
        {
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

        IColumn innerColumn = inner.Densify(new ArrayColumn<T>(source.Name, inner.TypeName, values));
        return new NullableReferenceColumn<T>(source.Name, source.TypeName, (IColumn<T>)innerColumn, nullMap, rowCount, pooledMap: false);
    }

    /// <inheritdoc/>
    // Reached only for a present (non-null) row of a variable-width inner (String); null rows are priced by the
    // codec via MeasureNullPlaceholder, so the inner codec never measures a null value.
    public long MeasureInnerRow(IColumnCodec inner, IColumn column, int row) => inner.MeasureRowBytes(column, row);
}
