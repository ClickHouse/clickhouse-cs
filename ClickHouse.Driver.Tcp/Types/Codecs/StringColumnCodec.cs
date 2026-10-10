using System;
using System.Buffers;
using System.Collections.Generic;
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
internal sealed class StringColumnCodec : IColumnCodec
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

    /// <summary>The failure of a <see cref="T:byte[]"/> reading of a column that does not expose its wire bytes.</summary>
    /// <param name="column">The column, which is not an <see cref="IStringColumn"/>.</param>
    /// <returns>The exception to throw.</returns>
    internal static InvalidOperationException NoWireBytes(IColumn column)
        => new(
            $"Column '{column.Name}' ({column.TypeName}) was read as {column.GetType()}, which does not expose the wire bytes through IStringColumn, " +
            $"so its values cannot be read as a byte[]. Only a String column decoded from a server response does.");

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
                        throw new ClickHouseTcpProtocolException(
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
    public bool CanWrite(IColumn column) => column is StringColumn;

    /// <inheritdoc/>
    // A column this client decoded holds the bytes the wire carried, so the write gives those again rather than the
    // UTF-8 of its decoded text: a byte string UTF-8 cannot spell decodes to U+FFFD, and encoding that again would
    // store the replacement character, not the original bytes.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var decoded = (StringColumn)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteString(decoded.GetBytes(start + i));
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
