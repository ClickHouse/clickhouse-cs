using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>Shared write/read plumbing for the per-codec unit tests: encode to a buffer, then read it back.</summary>
internal static class CodecTestHarness
{
    public static readonly CancellationToken None = CancellationToken.None;

    /// <summary>Encodes <paramref name="write"/> into a flushed byte buffer.</summary>
    public static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    /// <summary>A reader over the given bytes.</summary>
    public static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));

    /// <summary>
    /// Writes then reads back <paramref name="column"/> through <paramref name="codec"/>, returning the decoded
    /// column. The column is densified first, mirroring the insert pipeline, which densifies every column before
    /// the write so the codecs only ever measure/write the dense wire shape.
    /// </summary>
    public static async Task<IColumn> RoundTripAsync(IColumnCodec codec, IColumn column, string columnType, int rowCount)
    {
        IColumn dense = codec.TryDensify(column, out bool built);
        try
        {
            byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, dense));
            using ClickHouseBinaryReader reader = ReaderOver(bytes);
            return await codec.ReadColumnAsync(reader, column.Name, columnType, rowCount, None);
        }
        finally
        {
            if (built)
            {
                dense.Dispose();
            }
        }
    }

    /// <summary>
    /// Densifies <paramref name="column"/> through <paramref name="codec"/> and encodes the requested row range,
    /// mirroring the insert pipeline. Use for the direct-<see cref="IColumnCodec.WriteColumn(ClickHouseBinaryWriter, IColumn, int, int)"/>
    /// slice tests that bypass <see cref="RoundTripAsync"/>.
    /// </summary>
    public static async Task<byte[]> WriteDenseAsync(IColumnCodec codec, IColumn column, int start, int length)
    {
        IColumn dense = codec.TryDensify(column, out bool built);
        try
        {
            return await WriteAsync(w => codec.WriteColumn(w, dense, start, length));
        }
        finally
        {
            if (built)
            {
                dense.Dispose();
            }
        }
    }
}
