using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The bridge for an inner element type <typeparamref name="T"/>: the low-cardinality column surfaces <c>T</c>.</summary>
/// <typeparam name="T">The inner codec's element type.</typeparam>
internal sealed class LowCardinalityShape<T> : ILowCardinalityShape
{
    // The comparer used to deduplicate values into the block-local dictionary. Reference types generally carry
    // value equality (String, IPAddress), but byte[] — the element type of FixedString — defaults to reference
    // equality, which would give every equal-valued row its own dictionary slot; use a structural comparer so
    // FixedString values actually collapse to one entry.
    private static readonly IEqualityComparer<T> DictionaryComparer = typeof(T) == typeof(byte[])
        ? (IEqualityComparer<T>)(object)ByteArrayEqualityComparer.Instance
        : EqualityComparer<T>.Default;

    /// <inheritdoc/>
    public Type SurfaceElementType => typeof(T);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys)
        => new LowCardinalityColumn<T>(name, typeName, (IColumn<T>)dictionary, keys, rowCount, pooledKeys);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<T>;

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

        if (column is LowCardinalityColumn<T> dense)
        {
            // Dense form (the wire's own layout): the dictionary and keys already exist, so re-emit both directly.
            // The whole dictionary is written even for a slice — unused entries are harmless, and the key width is
            // fixed by the dictionary size, so a slice's keys keep the same encoding.
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

            return;
        }

        WriteErgonomic(inner, writer, (IColumn<T>)column, start, length);
    }

    // Ergonomic form: deduplicate the sliced values into a fresh block-local dictionary (slot 0 reserved for the
    // inner type's default, so a value equal to the default reuses it), then write the dictionary and the keys.
    private static void WriteErgonomic(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn<T> source, int start, int length)
    {
        var defaultValue = (T)inner.NullPlaceholderAs(typeof(T));
        var indexByValue = new Dictionary<T, int>(DictionaryComparer) { [defaultValue] = 0 };

        // The dictionary can grow to length + 1 (every value distinct, plus the reserved default). Rent both
        // buffers up front; the dictionary is filled as new values are discovered.
        T[] dict = ArrayPool<T>.Shared.Rent(length + 1);
        int[] keys = ArrayPool<int>.Shared.Rent(length);
        try
        {
            dict[0] = defaultValue;
            int dictSize = 1;
            for (int i = 0; i < length; i++)
            {
                T value = source[start + i];
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

/// <summary>Structural (content) equality for <see cref="byte"/> arrays, so equal FixedString values deduplicate.</summary>
internal sealed class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
{
    /// <summary>The shared instance.</summary>
    public static readonly ByteArrayEqualityComparer Instance = new();

    private ByteArrayEqualityComparer()
    {
    }

    /// <inheritdoc/>
    public bool Equals(byte[] x, byte[] y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        return x is not null && y is not null && x.AsSpan().SequenceEqual(y);
    }

    /// <inheritdoc/>
    public int GetHashCode(byte[] obj)
    {
        var hash = new HashCode();
        hash.AddBytes(obj);
        return hash.ToHashCode();
    }
}
