using System;
using System.Buffers;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>FixedString(N)</c> column: every row is exactly <c>N</c> bytes with no
/// length prefix, so the column body is <c>num_rows * N</c> contiguous bytes. The rows are read in one bulk
/// transfer into a pooled blob and surfaced as a <see cref="FixedStringColumn"/> (each row a <see cref="byte"/>
/// array). On write, a row's bytes are emitted verbatim and right-padded with zeros to <c>N</c>; a value longer
/// than <c>N</c> is rejected, matching the server, which stores over-length values as an error rather than
/// truncating.
/// </summary>
internal sealed class FixedStringColumnCodec : IColumnCodec, ISpanWritableCodec<byte[]>
{
    private readonly int size;

    private FixedStringColumnCodec(int size, string typeName)
    {
        this.size = size;
        TypeName = typeName;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(byte[]);

    /// <summary>
    /// The placeholder for a null row is the empty byte array; the write path pads it to <c>N</c> zero bytes,
    /// so the values stream stays aligned at a <c>Nullable(FixedString(N))</c> null position.
    /// </summary>
    public object NullPlaceholder => Array.Empty<byte>();

    /// <summary>Builds a <c>FixedString(N)</c> codec from its type node's single integer length argument.</summary>
    /// <param name="node">The parsed <c>FixedString</c> type node.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The type does not have exactly one positive integer length argument.</exception>
    public static FixedStringColumnCodec Create(TypeNode node)
    {
        if (node.Arguments.Count != 1)
        {
            throw new FormatException($"FixedString type '{node}' must have exactly one length argument.");
        }

        string token = node.Arguments[0].Name.Trim();
        if (!int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out int size) || size <= 0)
        {
            throw new FormatException($"FixedString type '{node}' has an invalid length '{token}'; expected a positive integer.");
        }

        return new FixedStringColumnCodec(size, node.ToString());
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new FixedStringColumn(columnName, columnType, size, Array.Empty<byte>(), rowCount: 0, pooled: false);
        }

        int byteCount = checked(rowCount * size);
        byte[] blob = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            await reader.ReadBytesAsync(blob.AsMemory(0, byteCount), cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // The column never took ownership of the rent, so return it rather than leak it on a read failure.
            ArrayPool<byte>.Shared.Return(blob);
            throw;
        }

        return new FixedStringColumn(columnName, columnType, size, blob, rowCount, pooled: true);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<byte[]>;

    /// <inheritdoc/>
    // Read per element through the indexer so a scattered write-path view (a substitute for a nullable value, a
    // Tuple field) writes with no materialized copy; a dense FixedStringColumn materializes each row's bytes just
    // the same.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // A reusable zero run for the right-padding; the common case (value exactly N bytes) writes none of it.
        Span<byte> zeros = stackalloc byte[64];
        zeros.Clear();

        var typed = (IColumn<byte[]>)column;
        for (int i = 0; i < length; i++)
        {
            WriteRow(writer, typed[start + i], zeros);
        }
    }

    /// <inheritdoc/>
    // Each row is its own fixed-width byte run, so a run of values is written in order.
    public void WriteValues(ClickHouseBinaryWriter writer, ReadOnlySpan<byte[]> values)
    {
        Span<byte> zeros = stackalloc byte[64];
        zeros.Clear();

        foreach (byte[] value in values)
        {
            WriteRow(writer, value, zeros);
        }
    }

    // Emits one row's bytes verbatim, right-padded with zeros to the fixed width.
    private void WriteRow(ClickHouseBinaryWriter writer, byte[] value, ReadOnlySpan<byte> zeros)
    {
        if (value is null)
        {
            throw new ArgumentException($"A {TypeName} column cannot hold a null row; wrap the type in Nullable to write nulls.", nameof(value));
        }

        if (value.Length > size)
        {
            throw new ArgumentException($"A {TypeName} value is {value.Length} bytes, longer than the fixed width of {size}.", nameof(value));
        }

        writer.WriteBytes(value);

        int pad = size - value.Length;
        while (pad > 0)
        {
            int chunk = Math.Min(pad, zeros.Length);
            writer.WriteBytes(zeros.Slice(0, chunk));
            pad -= chunk;
        }
    }
}
