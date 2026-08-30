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
    /// A <c>String</c> is a byte string, so a column of <c>byte[]</c> rows writes as well as a column of text, and
    /// stores those bytes verbatim. That is the only way to store bytes UTF-8 cannot spell, and the counterpart of
    /// reading them back through <see cref="IStringColumn"/>.
    /// </summary>
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(string), typeof(byte[]) };

    /// <inheritdoc/>
    // Raw bytes have no lossless spelling as a string, so they cannot go through the canonical form. That refuses
    // LowCardinality(String) from a byte column up front instead of faulting once the write is under way; giving
    // it a byte-oriented canonical form would make every LowCardinality(String) dictionary encode its text first.
    public bool CanCanonicalizeWriteType(Type writeType) => writeType == typeof(string);

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
