using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>BFloat16</c> column: a 16-bit "brain float" (the top 16 bits of an IEEE-754
/// <c>Float32</c> — same 8-bit exponent, 7-bit mantissa) surfaced as a widened <see cref="float"/>. There is no
/// BCL <c>BFloat16</c> type, so a value is widened on read (shift the 16 bits into the high half of a 32-bit
/// float) and narrowed on write (drop the low 16 bits, losing mantissa precision).
/// </summary>
internal sealed class BFloat16ColumnCodec : IColumnCodec
{
    // The number of values that a write converts on the stack before it gives them to the writer in one copy.
    private const int ChunkValues = 2048;

    /// <summary>The shared, stateless instance.</summary>
    public static readonly BFloat16ColumnCodec Instance = new();

    private BFloat16ColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "BFloat16";

    /// <inheritdoc/>
    public Type ElementType => typeof(float);

    /// <inheritdoc/>
    public object NullPlaceholder => 0f;

    /// <inheritdoc/>
    // Only the top 16 bits are written, so floats that differ below them encode identically.
    public object LowCardinalityKeyWriter(Type writeType)
        => writeType == typeof(float) ? LowCardinalityKeys.Projected<float, ushort>(ToBFloat16Bits) : null;

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        return ArrayColumn<float>.ReadAsync(reader, columnName, columnType, rowCount, checked(rowCount * sizeof(ushort)), Fill, cancellationToken);

        static void Fill(ReadOnlySpan<byte> source, Span<float> destination)
        {
            ReadOnlySpan<ushort> bits = MemoryMarshal.Cast<byte, ushort>(source);
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = BitConverter.UInt32BitsToSingle((uint)bits[i] << 16);
            }
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<float>;

    /// <inheritdoc/>
    // The column that a query of the type reads.
    public bool WritesFromStorage(IColumn column) => column is ArrayColumn<float>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // The decoded column holds the floats: their high 16 bits are written a chunk at a time.
        if (column is ArrayColumn<float> stored)
        {
            Span<ushort> chunk = stackalloc ushort[ChunkValues];
            ReadOnlySpan<float> floats = stored.Values.Slice(start, length);
            while (!floats.IsEmpty)
            {
                int count = Math.Min(chunk.Length, floats.Length);
                for (int i = 0; i < count; i++)
                {
                    chunk[i] = ToBFloat16Bits(floats[i]);
                }

                writer.WriteBytes(MemoryMarshal.AsBytes(chunk.Slice(0, count)));
                floats = floats.Slice(count);
            }

            return;
        }

        var values = (IColumn<float>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt16(ToBFloat16Bits(values[start + i]));
        }
    }

    private static ushort ToBFloat16Bits(float value) => (ushort)(BitConverter.SingleToUInt32Bits(value) >> 16);
}
