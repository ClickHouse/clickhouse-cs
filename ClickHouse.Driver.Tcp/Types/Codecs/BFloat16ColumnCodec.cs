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
    public Type CanonicalWriteElementType => typeof(ushort);

    /// <inheritdoc/>
    public object CanonicalWritePlaceholder => ToBFloat16Bits((float)NullPlaceholder);

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
    public IColumn ToCanonicalWriteColumn(IColumn column)
        => column is IColumn<float> values
            ? new ProjectedColumn<float, ushort>(TypeName, values, ToBFloat16Bits)
            : throw new ArgumentException($"A BFloat16 column must hold float values, not {column.GetType()}.", nameof(column));

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var values = (IColumn<float>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt16(ToBFloat16Bits(values[start + i]));
        }
    }

    /// <inheritdoc/>
    public void WriteCanonicalColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var bits = (IColumn<ushort>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt16(bits[start + i]);
        }
    }

    private static ushort ToBFloat16Bits(float value) => (ushort)(BitConverter.SingleToUInt32Bits(value) >> 16);
}
