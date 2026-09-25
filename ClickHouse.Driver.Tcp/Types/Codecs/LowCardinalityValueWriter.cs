using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>Reports which source rows are null while the present rows are deduplicated.</summary>
internal interface ILowCardinalityNullMap
{
    bool IsNull(IColumn source, int row);
}

/// <summary>Builds and writes a LowCardinality dictionary from one surfaced element type.</summary>
internal interface ILowCardinalityKeyWriter<TSource>
{
    void Write(
        IColumnCodec inner,
        ClickHouseBinaryWriter writer,
        IColumn<TSource> values,
        TSource placeholder,
        IColumn source,
        ILowCardinalityNullMap nullMap,
        int start,
        int length);
}

/// <summary>Projects a surfaced value to the key used for LowCardinality dictionary lookup.</summary>
internal interface ILowCardinalityKeySelector<TSource, TKey>
{
    TKey Select(TSource value);
}

/// <summary>Builds a block-local LowCardinality dictionary from typed encoding keys.</summary>
internal static class LowCardinalityValueWriter
{
    /// <summary>Writes the dictionary and keys for rows [<paramref name="start"/>, start + length).</summary>
    /// <typeparam name="TSource">The surfaced element type being written.</typeparam>
    /// <typeparam name="TKey">The key type that represents its encoding.</typeparam>
    /// <param name="inner">The codec that encodes the dictionary.</param>
    /// <param name="selector">Projects one source value to its dictionary key.</param>
    /// <param name="writer">The writer to encode the body into.</param>
    /// <param name="values">The values, with a placeholder already substituted at null rows.</param>
    /// <param name="placeholder">The value the reserved slots take.</param>
    /// <param name="source">The caller's column, which <paramref name="nullMap"/> reads.</param>
    /// <param name="nullMap">Reports null rows, or null when the column cannot hold one.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    public static void Write<TSource, TKey, TSelector>(
        IColumnCodec inner,
        TSelector selector,
        ClickHouseBinaryWriter writer,
        IColumn<TSource> values,
        TSource placeholder,
        IColumn source,
        ILowCardinalityNullMap nullMap,
        int start,
        int length)
        where TSelector : struct, ILowCardinalityKeySelector<TSource, TKey>
    {
        int reserved = nullMap is null ? 1 : 2;
        var index = new Dictionary<TKey, int> { [selector.Select(placeholder)] = reserved - 1 };

        TSource[] dictionary = ArrayPool<TSource>.Shared.Rent(length + reserved);
        int[] keys = ArrayPool<int>.Shared.Rent(length);
        try
        {
            for (int slot = 0; slot < reserved; slot++)
            {
                dictionary[slot] = placeholder;
            }

            int dictionarySize = reserved;
            if (nullMap is null)
            {
                for (int i = 0; i < length; i++)
                {
                    TSource value = values[start + i];
                    keys[i] = Intern(index, dictionary, selector.Select(value), value, ref dictionarySize);
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
                    }
                    else
                    {
                        TSource value = values[row];
                        keys[i] = Intern(index, dictionary, selector.Select(value), value, ref dictionarySize);
                    }
                }
            }

            int code = LowCardinalityWire.SelectKeyWidthCode(dictionarySize);
            writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
            writer.WriteUInt64((ulong)dictionarySize);
            inner.WriteColumn(writer, ArrayColumn<TSource>.OverBuffer(source.Name, inner.TypeName, dictionary, dictionarySize), 0, dictionarySize);
            writer.WriteUInt64((ulong)length);

            for (int i = 0; i < length; i++)
            {
                LowCardinalityWire.WriteKey(writer, code, keys[i]);
            }
        }
        finally
        {
            ArrayPool<TSource>.Shared.Return(dictionary, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TSource>());
            ArrayPool<int>.Shared.Return(keys);
        }
    }

    private static int Intern<TSource, TKey>(
        Dictionary<TKey, int> index,
        TSource[] dictionary,
        TKey key,
        TSource value,
        ref int dictionarySize)
    {
        if (index.TryGetValue(key, out int existing))
        {
            return existing;
        }

        dictionary[dictionarySize] = value;
        index[key] = dictionarySize;
        return dictionarySize++;
    }
}
