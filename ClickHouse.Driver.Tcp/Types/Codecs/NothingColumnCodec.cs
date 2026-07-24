using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Nothing</c> column — the type carrying no information, used as the element of
/// an empty array or as the inner of <c>Nullable(Nothing)</c> (how a bare <c>NULL</c> literal is typed). It
/// serializes one placeholder byte per row on the wire; those bytes carry no value, so they are read and
/// discarded and every row surfaces as <see langword="null"/>. Values cannot be inserted into a
/// <c>Nothing</c> column, so the write path is unsupported.
/// </summary>
internal sealed class NothingColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly NothingColumnCodec Instance = new();

    private NothingColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "Nothing";

    /// <inheritdoc/>
    public Type ElementType => typeof(object);

    /// <inheritdoc/>
    public object NullPlaceholder => throw new NotSupportedException("Values cannot be written to a ClickHouse Nothing column.");

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        // One placeholder byte per row must be consumed to keep the stream aligned, even though it carries no
        // value. The decoded column surfaces a null for every row.
        if (rowCount > 0)
        {
            byte[] rented = ArrayPool<byte>.Shared.Rent(rowCount);
            try
            {
                await reader.ReadBytesAsync(rented.AsMemory(0, rowCount), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        return new ArrayColumn<object>(columnName, columnType, new object[rowCount]);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => false;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
        => throw new NotSupportedException("Values cannot be written to a ClickHouse Nothing column.");
}
