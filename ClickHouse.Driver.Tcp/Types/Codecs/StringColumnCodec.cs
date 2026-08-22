using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>String</c> column: each row is a VarUInt byte-length prefix followed by that
/// many bytes. The raw bytes are streamed into one pooled blob with per-row offsets and surfaced as a
/// <see cref="StringColumn"/>, which decodes to text on demand (UTF-8 by default, or a caller-chosen encoding)
/// and also exposes the raw bytes — ClickHouse <c>String</c> is byte-oriented and may hold non-UTF-8 data.
/// </summary>
internal sealed class StringColumnCodec : IColumnCodec, ISpanWritableCodec<string>
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly StringColumnCodec Instance = new();

    // A modest starting guess for the blob (16 bytes/row), clamped, that grows on demand as rows are read.
    private const int MinInitialBlobBytes = 256;
    private const int MaxInitialBlobBytes = 1 << 20;

    private StringColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "String";

    /// <inheritdoc/>
    public Type ElementType => typeof(string);

    /// <inheritdoc/>
    public object NullPlaceholder => string.Empty;

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new StringColumn(columnName, columnType, Array.Empty<byte>(), new int[1], rowCount: 0, pooled: false);
        }

        int[] offsets = ArrayPool<int>.Shared.Rent(rowCount + 1);
        byte[] blob = ArrayPool<byte>.Shared.Rent(Math.Clamp(rowCount * 16, MinInitialBlobBytes, MaxInitialBlobBytes));
        try
        {
            offsets[0] = 0;
            int pos = 0;
            for (int i = 0; i < rowCount; i++)
            {
                int length = await reader.ReadStringLengthAsync(cancellationToken).ConfigureAwait(false);
                if (length > 0)
                {
                    // The blob is addressed with int offsets, so its total size cannot exceed Array.MaxLength.
                    // Compute the new end in long so a large payload is rejected cleanly rather than wrapping past
                    // int and producing a bogus (possibly negative) capacity check and out-of-range reads.
                    long end = (long)pos + length;
                    if (end > Array.MaxLength)
                    {
                        throw new ClickHouseProtocolException(
                            $"String column '{columnName}' exceeds the maximum blob size ({Array.MaxLength} bytes) this client can buffer.");
                    }

                    if (end > blob.Length)
                    {
                        blob = Grow(blob, pos, (int)end);
                    }

                    await reader.ReadBytesAsync(blob.AsMemory(pos, length), cancellationToken).ConfigureAwait(false);
                    pos += length;
                }

                offsets[i + 1] = pos;
            }

            return new StringColumn(columnName, columnType, blob, offsets, rowCount, pooled: true);
        }
        catch
        {
            // Neither buffer was handed to a column, so return both rather than leak them on a read failure.
            ArrayPool<byte>.Shared.Return(blob);
            ArrayPool<int>.Shared.Return(offsets);
            throw;
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<string>;

    /// <inheritdoc/>
    // Read per element through the indexer so a scattered write-path view (a substitute for a nullable string, a
    // Tuple field) writes with no materialized copy; a dense StringColumn decodes each row on demand just the same.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var typed = (IColumn<string>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteString(typed[start + i]);
        }
    }

    /// <inheritdoc/>
    // Each element is its own length-prefixed byte run, so a run of values is just written in order.
    public void WriteValues(ClickHouseBinaryWriter writer, ReadOnlySpan<string> values)
    {
        foreach (string value in values)
        {
            writer.WriteString(value);
        }
    }

    /// <summary>Grows the blob to hold at least <paramref name="minCapacity"/> bytes, copying the <paramref name="used"/> prefix.</summary>
    private static byte[] Grow(byte[] blob, int used, int minCapacity)
    {
        int newCapacity = (int)Math.Min(Math.Max((long)blob.Length * 2, minCapacity), Array.MaxLength);
        byte[] bigger = ArrayPool<byte>.Shared.Rent(newCapacity);
        Array.Copy(blob, bigger, used);
        ArrayPool<byte>.Shared.Return(blob);
        return bigger;
    }
}
