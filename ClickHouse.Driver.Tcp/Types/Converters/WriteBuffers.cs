using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The pooled buffers of one write of a composite writer, and the operations that its writers share. A buffer lives in
/// the state of one write, so a cached tree keeps none.
/// </summary>
internal static class WriteBuffers
{
    /// <summary>Rents an array of at least <paramref name="count"/> values from <see cref="ArrayPool{T}.Shared"/>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="count">The number of values.</param>
    /// <returns>The array, or an empty array for zero values.</returns>
    public static T[] Rent<T>(int count) => count == 0 ? Array.Empty<T>() : ArrayPool<T>.Shared.Rent(count);

    /// <summary>Returns an array from <see cref="Rent{T}"/>. An array that holds references is cleared first.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="array">The array, or null.</param>
    public static void Return<T>(T[] array)
    {
        if (array is { Length: > 0 })
        {
            ArrayPool<T>.Shared.Return(array, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
        }
    }

    /// <summary>Copies the values of all runs of <paramref name="values"/> into <paramref name="destination"/>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="values">The source.</param>
    /// <param name="destination">Receives <see cref="ValueSource{T}.Count"/> values.</param>
    public static void CopyTo<T>(ValueSource<T> values, Span<T> destination)
    {
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            ReadOnlySpan<T> run = values.Run(r);
            run.CopyTo(destination.Slice(position));
            position += run.Length;
        }
    }

    /// <summary>
    /// Writes the dictionary keys of a LowCardinality body at the width that <paramref name="code"/> selects, in
    /// chunks, each in one copy.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="code">The key-width code (<see cref="LowCardinalityWire.SelectKeyWidthCode"/>).</param>
    /// <param name="keys">The keys, one for each value.</param>
    public static void WriteKeys(ClickHouseBinaryWriter writer, int code, ReadOnlySpan<int> keys)
    {
        switch (code)
        {
            case LowCardinalityWire.KeyUInt8:
                WriteKeys<byte>(writer, keys);
                break;
            case LowCardinalityWire.KeyUInt16:
                WriteKeys<ushort>(writer, keys);
                break;
            case LowCardinalityWire.KeyUInt32:
                WriteKeys<uint>(writer, keys);
                break;
            default:
                foreach (int key in keys)
                {
                    writer.WriteUInt64((ulong)key);
                }

                break;
        }
    }

    private static void WriteKeys<TKey>(ClickHouseBinaryWriter writer, ReadOnlySpan<int> keys)
        where TKey : unmanaged, System.Numerics.INumberBase<TKey>
    {
        Span<TKey> chunk = stackalloc TKey[Math.Max(1, 1024 / Unsafe.SizeOf<TKey>())];
        while (!keys.IsEmpty)
        {
            int count = Math.Min(chunk.Length, keys.Length);
            for (int i = 0; i < count; i++)
            {
                chunk[i] = TKey.CreateTruncating(keys[i]);
            }

            writer.WriteBytes(System.Runtime.InteropServices.MemoryMarshal.AsBytes(chunk.Slice(0, count)));
            keys = keys.Slice(count);
        }
    }
}
