using System;

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
}
