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
/// array). The write of such a column gives its bytes again, in one copy.
/// </summary>
internal sealed class FixedStringColumnCodec : IColumnCodec
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

    /// <summary>The number of bytes in each value: the <c>N</c> of <c>FixedString(N)</c>.</summary>
    internal int Size => size;

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
    public bool CanWrite(IColumn column) => column is FixedStringColumn dense && dense.Size == size;

    /// <inheritdoc/>
    // A FixedStringColumn of this width holds its rows back to back at the stride of the wire, so the slice is one copy.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => writer.WriteBytes(((FixedStringColumn)column).GetBytes(start, length));
}
