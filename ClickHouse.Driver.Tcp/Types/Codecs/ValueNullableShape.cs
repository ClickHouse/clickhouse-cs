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
    // A dense column (inner column + null-map) writes both directly with no copy. The ergonomic T? column writes
    // the null-map from each row's HasValue, then hands the inner codec a substitute view that reads the present
    // value or the inner placeholder per row — no flat placeholder buffer is built; the null positions the wire
    // ignores are still written from the placeholder so the inner codec never sees a null.
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (column is NullableValueColumn<T> dense)
        {
            writer.WriteBytes(dense.NullMap.Slice(start, length));
            inner.WriteColumn(writer, dense.Inner, start, length);
            return;
        }

        var source = (IColumn<T?>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteBool(!source[start + i].HasValue);
        }

        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        inner.WriteColumn(writer, new SubstituteValueColumn<T>(inner.TypeName, source, placeholder), start, length);
    }
}
