namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Reports a wire block sent during an insert. These client-side counters do not confirm that the server applied
/// the block and exclude the packet envelope.
/// </summary>
public readonly record struct ClickHouseTcpBlockWritten
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpBlockWritten"/> struct.</summary>
    /// <param name="blockIndex">The block's zero-based position in the insert's send order.</param>
    /// <param name="rowCount">The rows in this block.</param>
    /// <param name="uncompressedBytes">The block body's size before compression.</param>
    /// <param name="compressedBytes">The bytes this block put on the socket.</param>
    public ClickHouseTcpBlockWritten(int blockIndex, int rowCount, long uncompressedBytes, long compressedBytes)
    {
        BlockIndex = blockIndex;
        RowCount = rowCount;
        UncompressedBytes = uncompressedBytes;
        CompressedBytes = compressedBytes;
    }

    /// <summary>The block's zero-based position in the insert's send order.</summary>
    public int BlockIndex { get; }

    /// <summary>
    /// The rows in this block. Every block but the last holds the insert's block size, which is the lower of
    /// <see cref="ClickHouseTcpInsertOptions.MaxRowsPerBlock"/> and the total rows; this is where that setting
    /// becomes observable.
    /// </summary>
    public int RowCount { get; }

    /// <summary>The block body's size before compression, which is what the rows cost to encode.</summary>
    public long UncompressedBytes { get; }

    /// <summary>
    /// The framed bytes written to the socket. Frame overhead can make this larger than
    /// <see cref="UncompressedBytes"/> for small blocks.
    /// </summary>
    public long CompressedBytes { get; }
}
