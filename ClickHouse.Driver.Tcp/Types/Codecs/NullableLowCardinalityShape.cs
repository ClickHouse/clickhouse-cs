using System;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Handles <c>LowCardinality(Nullable(T))</c>. Dictionary slot 0 represents null and slot 1 holds the inner default.
/// </summary>
/// <typeparam name="T">The non-nullable inner element type.</typeparam>
internal abstract class NullableLowCardinalityShape<T> : ILowCardinalityShape
{
    /// <inheritdoc/>
    public abstract Type SurfaceElementType { get; }

    /// <inheritdoc/>
    public abstract IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IDenseLowCardinality<T>;
}

/// <summary>The nullable bridge for a value-type inner: the column surfaces <c>T?</c>.</summary>
/// <typeparam name="T">The inner value type.</typeparam>
internal sealed class ValueLowCardinalityShape<T> : NullableLowCardinalityShape<T>
    where T : struct
{
    /// <inheritdoc/>
    public override Type SurfaceElementType => typeof(T?);

    /// <inheritdoc/>
    public override IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys)
        => new NullableLowCardinalityValueColumn<T>(name, typeName, (IColumn<T>)dictionary, keys, rowCount, pooledKeys);
}

/// <summary>The nullable bridge for a reference-type inner: the column surfaces the nullable reference.</summary>
/// <typeparam name="T">The inner reference type.</typeparam>
internal sealed class ReferenceLowCardinalityShape<T> : NullableLowCardinalityShape<T>
    where T : class
{
    /// <inheritdoc/>
    public override Type SurfaceElementType => typeof(T);

    /// <inheritdoc/>
    public override IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys)
        => new NullableLowCardinalityReferenceColumn<T>(name, typeName, (IColumn<T>)dictionary, keys, rowCount, pooledKeys);
}
