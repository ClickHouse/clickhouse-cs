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
    public IColumn Wrap(string name, string typeName, IColumn inner, byte[] nullMap, bool pooledMap)
        => new NullableValueColumn<T>(name, typeName, (IColumn<T>)inner, nullMap, pooledMap);

    /// <inheritdoc/>
    public bool CanWrite(IColumnCodec inner, IColumn column)
        => column is NullableValueColumn<T> dense
            ? inner.CanWrite(dense.Inner)
            : column is IColumn<T?> && inner.CanWriteElementType(typeof(T));

    /// <inheritdoc/>
    public IColumn GetInnerColumn(IColumnCodec inner, IColumn column)
    {
        if (column is NullableValueColumn<T> dense)
        {
            return dense.Inner;
        }

        var source = (IColumn<T?>)column;
        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        return new SubstituteValueColumn<T>(inner.TypeName, source, placeholder);
    }

    /// <inheritdoc/>
    public void WriteNullMap(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (column is NullableValueColumn<T> dense)
        {
            writer.WriteBytes(dense.NullMap.Slice(start, length));
            return;
        }

        var source = (IColumn<T?>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteBool(!source[start + i].HasValue);
        }
    }
}
