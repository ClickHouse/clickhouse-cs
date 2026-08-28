using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Handles <c>LowCardinality(Nullable(T))</c>. Dictionary slot 0 represents null and slot 1 holds the inner default.
/// </summary>
/// <typeparam name="T">The non-nullable inner element type.</typeparam>
internal abstract class NullableLowCardinalityShape<T> : ILowCardinalityShape, ILowCardinalityNullMap
{
    /// <inheritdoc/>
    public abstract Type SurfaceElementType { get; }

    /// <inheritdoc/>
    public abstract IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys);

    /// <inheritdoc/>
    public abstract bool CanWrite(IColumn column);

    /// <summary>Whether row <paramref name="row"/> of <paramref name="column"/> is NULL (maps to the reserved slot 0).</summary>
    protected abstract bool IsNull(IColumn column, int row);

    /// <summary>Returns a borrowed bare-value view, substituting the inner placeholder at null rows.</summary>
    protected abstract IColumn<T> WithoutNulls(IColumn column, T placeholder);

    bool ILowCardinalityNullMap.IsNull(IColumn source, int row) => IsNull(source, row);

    /// <inheritdoc/>
    public bool CanInnerWrite(IColumnCodec inner) => inner.CanWriteElementType(typeof(T));

    /// <inheritdoc/>
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // A zero-length slice writes no body at all: only the state prefix (emitted by the block layer, or by a
        // composite's prefix phase) precedes it. Metadata, dictionary, and keys are all absent.
        if (length == 0)
        {
            return;
        }

        if (column is IDenseLowCardinality<T> dense)
        {
            // Dense form (the wire's own layout): the dictionary already carries the two reserved slots and the keys
            // already point correctly (NULL rows at slot 0), so re-emit both directly with no rebuild.
            WriteDense(inner, writer, dense, start, length);
            return;
        }

        WriteErgonomic(inner, writer, column, start, length);
    }

    private static void WriteDense(IColumnCodec inner, ClickHouseBinaryWriter writer, IDenseLowCardinality<T> dense, int start, int length)
    {
        IColumn<T> dictionary = dense.Dictionary;
        int dictSize = dictionary.RowCount;
        int code = LowCardinalityWire.SelectKeyWidthCode(dictSize);

        writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
        writer.WriteUInt64((ulong)dictSize);
        inner.WriteColumn(writer, dictionary, 0, dictSize);
        writer.WriteUInt64((ulong)length);

        ReadOnlySpan<int> keys = dense.Keys;
        for (int i = 0; i < length; i++)
        {
            LowCardinalityWire.WriteKey(writer, code, keys[start + i]);
        }
    }

    // Substitute a valid value at null rows, convert through the inner codec, then deduplicate only present rows.
    private void WriteErgonomic(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn source, int start, int length)
    {
        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        IColumn<T> present = WithoutNulls(source, placeholder);
        IColumn canonical = inner.ToCanonicalWriteColumn(present);
        if (canonical.ElementType != inner.CanonicalWriteElementType)
        {
            throw new InvalidOperationException(
                $"The '{inner.TypeName}' codec projected {canonical.ElementType}, expected {inner.CanonicalWriteElementType}.");
        }

        CanonicalLowCardinalityWriters.For(canonical.ElementType)
            .Write(inner, writer, canonical, source, this, start, length);
    }
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

    /// <inheritdoc/>
    public override bool CanWrite(IColumn column) => column is IColumn<T?>;

    /// <inheritdoc/>
    protected override bool IsNull(IColumn column, int row) => !((IColumn<T?>)column)[row].HasValue;

    /// <inheritdoc/>
    protected override IColumn<T> WithoutNulls(IColumn column, T placeholder)
        => new SubstituteValueColumn<T>(column.TypeName, (IColumn<T?>)column, placeholder);
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

    /// <inheritdoc/>
    public override bool CanWrite(IColumn column) => column is IColumn<T>;

    /// <inheritdoc/>
    protected override bool IsNull(IColumn column, int row) => ((IColumn<T>)column)[row] is null;

    /// <inheritdoc/>
    protected override IColumn<T> WithoutNulls(IColumn column, T placeholder)
        => new SubstituteReferenceColumn<T>(column.TypeName, (IColumn<T>)column, placeholder);
}
