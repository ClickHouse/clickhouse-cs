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

/// <summary>
/// Builds a block-local LowCardinality dictionary: the distinct values in first-appearance order, plus a key per
/// row indexing them.
///
/// <para>
/// Which rows count as the same value is the inner codec's answer, through
/// <see cref="IColumnCodec.WireEqualityComparer"/>. It has to be, because the relation the wire needs is "encodes
/// to the same bytes" and CLR equality is a different relation for several types. Deduplicating on the wrong one
/// merges rows the server must see as distinct, and the values reach it changed.
/// </para>
/// </summary>
internal static class LowCardinalityValueWriter
{
    /// <summary>Writes the dictionary and keys for rows [<paramref name="start"/>, start + length).</summary>
    /// <typeparam name="T">The surfaced element type being written.</typeparam>
    /// <param name="inner">The codec that encodes the dictionary.</param>
    /// <param name="comparer">The inner codec's wire-equality comparer for <typeparamref name="T"/>.</param>
    /// <param name="writer">The writer to encode the body into.</param>
    /// <param name="values">The values, with a placeholder already substituted at null rows.</param>
    /// <param name="placeholder">The value the reserved slots take.</param>
    /// <param name="source">The caller's column, which <paramref name="nullMap"/> reads.</param>
    /// <param name="nullMap">Reports null rows, or null when the column cannot hold one.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    public static void Write<T>(
        IColumnCodec inner,
        IEqualityComparer<T> comparer,
        ClickHouseBinaryWriter writer,
        IColumn<T> values,
        T placeholder,
        IColumn source,
        ILowCardinalityNullMap nullMap,
        int start,
        int length)
    {
        int reserved = nullMap is null ? 1 : 2;
        var index = new Dictionary<T, int>(comparer) { [placeholder] = reserved - 1 };

        T[] dictionary = ArrayPool<T>.Shared.Rent(length + reserved);
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
                    keys[i] = Intern(index, dictionary, values[start + i], ref dictionarySize);
                }
            }
            else
            {
                for (int i = 0; i < length; i++)
                {
                    int row = start + i;
                    keys[i] = nullMap.IsNull(source, row) ? 0 : Intern(index, dictionary, values[row], ref dictionarySize);
                }
            }

            int code = LowCardinalityWire.SelectKeyWidthCode(dictionarySize);
            writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
            writer.WriteUInt64((ulong)dictionarySize);
            inner.WriteColumn(writer, ArrayColumn<T>.OverBuffer(source.Name, inner.TypeName, dictionary, dictionarySize), 0, dictionarySize);
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

    private static int Intern<T>(Dictionary<T, int> index, T[] dictionary, T value, ref int dictionarySize)
    {
        if (index.TryGetValue(value, out int existing))
        {
            return existing;
        }

        dictionary[dictionarySize] = value;
        index[value] = dictionarySize;
        return dictionarySize++;
    }
}
