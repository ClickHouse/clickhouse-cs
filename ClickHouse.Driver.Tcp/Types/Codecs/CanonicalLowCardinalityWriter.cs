using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>Reports which source rows are null while their canonical values are deduplicated.</summary>
internal interface ILowCardinalityNullMap
{
    bool IsNull(IColumn source, int row);
}

/// <summary>Deduplicates canonical values and writes a LowCardinality body.</summary>
internal interface ICanonicalLowCardinalityWriter
{
    void Write(
        IColumnCodec inner,
        ClickHouseBinaryWriter writer,
        IColumn canonical,
        IColumn source,
        ILowCardinalityNullMap nullMap,
        int start,
        int length);
}

/// <summary>Resolves the dictionary writer for a canonical element type.</summary>
internal static class CanonicalLowCardinalityWriters
{
    private static readonly ConcurrentDictionary<Type, ICanonicalLowCardinalityWriter> Cache = new();

    public static ICanonicalLowCardinalityWriter For(Type elementType)
        => Cache.GetOrAdd(elementType, static type =>
            (ICanonicalLowCardinalityWriter)Activator.CreateInstance(
                typeof(CanonicalLowCardinalityWriter<>).MakeGenericType(type),
                nonPublic: true));
}

/// <summary>Writes one canonical element type as a dictionary and key stream.</summary>
internal sealed class CanonicalLowCardinalityWriter<T> : ICanonicalLowCardinalityWriter
{
    public void Write(
        IColumnCodec inner,
        ClickHouseBinaryWriter writer,
        IColumn canonical,
        IColumn source,
        ILowCardinalityNullMap nullMap,
        int start,
        int length)
    {
        var values = (IColumn<T>)canonical;
        var defaultValue = (T)inner.CanonicalWritePlaceholder;
        int reserved = nullMap is null ? 1 : 2;
        var indexByValue = new Dictionary<T, int> { [defaultValue] = reserved - 1 };

        T[] dictionary = ArrayPool<T>.Shared.Rent(length + reserved);
        int[] keys = ArrayPool<int>.Shared.Rent(length);
        try
        {
            dictionary[0] = defaultValue;
            if (reserved == 2)
            {
                dictionary[1] = defaultValue;
            }

            int dictionarySize = reserved;
            if (nullMap is null)
            {
                for (int i = 0; i < length; i++)
                {
                    T value = values[start + i];
                    if (!indexByValue.TryGetValue(value, out int index))
                    {
                        index = dictionarySize;
                        dictionary[dictionarySize++] = value;
                        indexByValue[value] = index;
                    }

                    keys[i] = index;
                }
            }
            else
            {
                for (int i = 0; i < length; i++)
                {
                    int row = start + i;
                    if (nullMap.IsNull(source, row))
                    {
                        keys[i] = 0;
                        continue;
                    }

                    T value = values[row];
                    if (!indexByValue.TryGetValue(value, out int index))
                    {
                        index = dictionarySize;
                        dictionary[dictionarySize++] = value;
                        indexByValue[value] = index;
                    }

                    keys[i] = index;
                }
            }

            int code = LowCardinalityWire.SelectKeyWidthCode(dictionarySize);
            writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
            writer.WriteUInt64((ulong)dictionarySize);
            inner.WriteCanonicalColumn(
                writer,
                ArrayColumn<T>.OverBuffer(source.Name, inner.TypeName, dictionary, dictionarySize),
                0,
                dictionarySize);
            writer.WriteUInt64((ulong)length);

            for (int i = 0; i < length; i++)
            {
                LowCardinalityWire.WriteKey(writer, code, keys[i]);
            }
        }
        finally
        {
            ArrayPool<T>.Shared.Return(dictionary, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
            ArrayPool<int>.Shared.Return(keys);
        }
    }
}
