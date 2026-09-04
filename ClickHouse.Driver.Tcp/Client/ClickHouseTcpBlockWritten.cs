namespace ClickHouse.Driver.Tcp;

/// <summary>
/// One wire block an insert has finished sending. Reported through
/// <see cref="ClickHouseTcpQueryCallbacks.OnBlockWritten"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>These are the client's own counters, not the server's.</b> The server sends no Progress packet for the rows
/// a client streams to it during a native insert, so <see cref="ClickHouseTcpProgress.WroteRows"/> stays zero and
/// this is the only account an insert has of its own progress. A block reported here has been handed to the
/// socket; whether the server has applied it is a separate question, answered only by the insert completing.
/// </para>
/// <para>
/// The counters cover the block's body — its header, column names, types and values — and not the two-byte
/// packet envelope around it.
/// </para>
/// </remarks>
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
    /// The bytes this block put on the socket. Equal to <see cref="UncompressedBytes"/> when the client is not
    /// compressing (a null <see cref="ClickHouseTcpClientOptions.Compressor"/>), and otherwise the framed and
    /// compressed size.
    /// </summary>
    /// <remarks>
    /// Each frame carries a header and a checksum, so <b>this can exceed <see cref="UncompressedBytes"/> on a
    /// small block</b> — a three-row block measured 110 bytes against 91 under LZ4. Read the two as a compression
    /// ratio only over blocks large enough for the payload to dominate that overhead.
    /// </remarks>
    public long CompressedBytes { get; }
}
