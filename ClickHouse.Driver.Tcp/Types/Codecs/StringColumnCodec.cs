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
internal sealed class StringColumnCodec : IColumnCodec, ISpanWritableCodec<string>
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly StringColumnCodec Instance = new();

    private static readonly Func<IColumn, int, byte[]> ReadRowBytes = RowBytes;

    private static readonly ColumnReadProjection ProjectBytes =
        static source => new ProjectedReadColumn<byte[]>(source, ReadRowBytes);

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

    /// <summary>
    /// Accepts text or raw byte rows. Raw bytes are stored verbatim.
    /// </summary>
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(string), typeof(byte[]) };

    /// <summary>
    /// Offers text and lossless raw bytes. <see cref="TryProjectColumnRead"/> is authoritative.
    /// </summary>
    public IReadOnlyList<Type> ReadableElementTypes { get; } = new[] { typeof(string), typeof(byte[]) };

    /// <inheritdoc/>
    public object NullPlaceholderAs(Type writeType)
    {
        if (writeType == typeof(string))
        {
            return NullPlaceholder;
        }

        return writeType == typeof(byte[])
            ? Array.Empty<byte>()
            : throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
    }

    /// <summary>
    /// Reads raw bytes from the column because re-encoding decoded text would lose invalid UTF-8 sequences.
    /// </summary>
    public bool TryProjectColumnRead(Type targetType, out ColumnReadProjection projection)
    {
        projection = targetType == typeof(byte[]) ? ProjectBytes : null;
        return projection is not null;
    }

    /// <summary>One row's bytes, copied out of the column's blob into an array the caller owns.</summary>
    /// <param name="column">The decoded column, which must expose its bytes.</param>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>That row's bytes.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="column"/> does not expose its bytes.</exception>
    /// <exception cref="IndexOutOfRangeException"><paramref name="row"/> is negative or not less than the row count.</exception>
    // Caller-built columns may carry the String type name without exposing decoded byte storage.
    public static byte[] RowBytes(IColumn column, int row) => column is IStringColumn text
        ? text.GetBytes(row).ToArray()
        : throw new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) was read as {column.GetType()}, which does not expose the wire bytes through IStringColumn, " +
            $"so its values cannot be read as a byte[]. Only a String column decoded from a server response does.");

    /// <inheritdoc/>
    // String keys may keep redundant entries when different invalid UTF-16 inputs encode alike, but never merge
    // different bytes. Raw bytes have no key, so LowCardinality(String) rejects them during planning.
    public object LowCardinalityKeyWriter(Type writeType)
        => writeType == typeof(string) ? LowCardinalityKeys.Identity<string>() : null;

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
    public bool CanWrite(IColumn column) => column is IColumn<string> or IColumn<byte[]>;

    /// <inheritdoc/>
    // Read per element through the indexer so a scattered write-path view (a substitute for a nullable string, a
    // Tuple field) writes with no materialized copy.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // A column this client decoded still holds the bytes the wire carried, so re-emit those rather than the
        // UTF-8 of its decoded text: a byte string UTF-8 cannot spell decodes to U+FFFD, and re-encoding that would
        // store the replacement character instead of the original bytes.
        if (column is StringColumn decoded)
        {
            for (int i = 0; i < length; i++)
            {
                writer.WriteString(decoded.GetBytes(start + i));
            }

            return;
        }

        if (column is IColumn<byte[]> rawBytes)
        {
            for (int i = 0; i < length; i++)
            {
                int row = start + i;
                byte[] value = rawBytes[row];
                if (value is null)
                {
                    throw new ArgumentException(
                        $"A {TypeName} column cannot hold a null value (at row {row}); wrap the type in Nullable to write nulls.",
                        nameof(column));
                }

                writer.WriteString(value);
            }

            return;
        }

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
