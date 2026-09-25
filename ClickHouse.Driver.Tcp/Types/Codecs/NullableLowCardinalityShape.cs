using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The generic bridge for one <c>LowCardinality(Nullable(T))</c> element type. The dictionary is still serialized
/// with the <em>bare</em> inner codec (no null-map); nullability is expressed positionally by two reserved leading
/// dictionary slots — <c>dict[0]</c> is the NULL marker and <c>dict[1]</c> the inner default — with a NULL row's key
/// pointing at <c>dict[0]</c>. This base owns the T-independent parts of that (the dedup-with-two-reserved-slots
/// write, the dense re-emit, row pricing); the value/reference subclasses supply the null test, the value
/// extraction, and the typed column the reader surfaces.
/// </summary>
/// <typeparam name="T">The inner (bare) codec's element type; the surfaced element type is this made nullable.</typeparam>
internal abstract class NullableLowCardinalityShape<T> : ILowCardinalityShape
{
    // The comparer used to deduplicate present values into the block-local dictionary. Reference types generally
    // carry value equality (String), but byte[] — the element type of FixedString — defaults to reference equality;
    // use a structural comparer so equal FixedString values collapse to one entry. NULL is handled positionally
    // (slot 0), never through this comparer.
    private static readonly IEqualityComparer<T> DictionaryComparer = typeof(T) == typeof(byte[])
        ? (IEqualityComparer<T>)(object)ByteArrayEqualityComparer.Instance
        : EqualityComparer<T>.Default;

    /// <inheritdoc/>
    public abstract Type SurfaceElementType { get; }

    /// <inheritdoc/>
    public abstract IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys);

    /// <inheritdoc/>
    public abstract bool CanWrite(IColumn column);

    /// <summary>Whether row <paramref name="row"/> of <paramref name="column"/> is NULL (maps to the reserved slot 0).</summary>
    protected abstract bool IsNull(IColumn column, int row);

    /// <summary>The present (non-NULL) value at row <paramref name="row"/> of <paramref name="column"/>.</summary>
    protected abstract T Value(IColumn column, int row);

    /// <inheritdoc/>
    public bool CanInnerWrite(IColumnCodec inner) => inner.CanWrite(new ArrayColumn<T>(string.Empty, inner.TypeName, Array.Empty<T>()));

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

    // Ergonomic form: deduplicate the sliced present values into a fresh block-local dictionary. Two slots are
    // reserved up front — dict[0] the NULL marker and dict[1] the inner default — so real distinct values start at
    // index 2. A NULL row maps to key 0; a present value equal to the inner default reuses the reserved default
    // slot 1 (so it reads back as that default value, present rather than NULL) rather than adding a duplicate.
    private void WriteErgonomic(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn source, int start, int length)
    {
        var defaultValue = (T)inner.NullPlaceholderAs(typeof(T));
        var indexByValue = new Dictionary<T, int>(DictionaryComparer) { [defaultValue] = 1 };

        // The dictionary can grow to length + 2 (every value distinct, plus the two reserved slots). Rent both
        // buffers up front; the dictionary is filled as new values are discovered.
        T[] dict = ArrayPool<T>.Shared.Rent(length + 2);
        int[] keys = ArrayPool<int>.Shared.Rent(length);
        try
        {
            dict[0] = defaultValue;
            dict[1] = defaultValue;
            int dictSize = 2;
            for (int i = 0; i < length; i++)
            {
                int row = start + i;
                if (IsNull(source, row))
                {
                    keys[i] = 0;
                    continue;
                }

                T value = Value(source, row);
                if (!indexByValue.TryGetValue(value, out int index))
                {
                    index = dictSize;
                    dict[dictSize++] = value;
                    indexByValue[value] = index;
                }

                keys[i] = index;
            }

            int code = LowCardinalityWire.SelectKeyWidthCode(dictSize);
            writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
            writer.WriteUInt64((ulong)dictSize);
            inner.WriteColumn(writer, ArrayColumn<T>.OverBuffer(source.Name, inner.TypeName, dict, dictSize), 0, dictSize);
            writer.WriteUInt64((ulong)length);

            for (int i = 0; i < length; i++)
            {
                LowCardinalityWire.WriteKey(writer, code, keys[i]);
            }
        }
        finally
        {
            ArrayPool<T>.Shared.Return(dict, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
            ArrayPool<int>.Shared.Return(keys);
        }
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
    protected override T Value(IColumn column, int row) => ((IColumn<T?>)column)[row].GetValueOrDefault();
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
    protected override T Value(IColumn column, int row) => ((IColumn<T>)column)[row];
}
