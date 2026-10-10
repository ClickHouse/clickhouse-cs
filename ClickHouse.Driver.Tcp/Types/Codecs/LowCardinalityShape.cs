using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The bridge for an inner element type <typeparamref name="T"/>: the low-cardinality column surfaces <c>T</c>.</summary>
/// <typeparam name="T">The inner codec's element type.</typeparam>
internal sealed class LowCardinalityShape<T> : ILowCardinalityShape
{
    /// <inheritdoc/>
    public Type SurfaceElementType => typeof(T);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys)
        => new LowCardinalityColumn<T>(name, typeName, (IColumn<T>)dictionary, keys, rowCount, pooledKeys);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is LowCardinalityColumn<T>;
}
