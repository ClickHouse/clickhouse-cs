using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The shape for a reference-type inner: the nullable column surfaces the nullable reference.</summary>
/// <typeparam name="T">The inner reference type.</typeparam>
internal sealed class ReferenceNullableShape<T> : INullableShape
    where T : class
{
    /// <inheritdoc/>
    public Type NullableElementType => typeof(T);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn inner, byte[] nullMap, bool pooledMap)
        => new NullableReferenceColumn<T>(name, typeName, (IColumn<T>)inner, nullMap, pooledMap);
}
