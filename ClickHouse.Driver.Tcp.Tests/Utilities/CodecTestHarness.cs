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
    /// column. The column is written straight from its ergonomic form (no densify step), exactly as the insert
    /// pipeline now does — the codec projects it to the wire through lazy views with no intermediate dense buffer.
    /// </summary>
    public static async Task<IColumn> RoundTripAsync(IColumnCodec codec, IColumn column, string columnType, int rowCount)
    {
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));
        using ClickHouseBinaryReader reader = ReaderOver(bytes);
        return await codec.ReadColumnAsync(reader, column.Name, columnType, rowCount, None);
    }

    /// <summary>
    /// Encodes the requested row range of <paramref name="column"/> straight from its ergonomic form. Use for the
    /// direct-<see cref="IColumnCodec.WriteColumn(ClickHouseBinaryWriter, IColumn, int, int)"/> slice tests that
    /// bypass <see cref="RoundTripAsync"/>.
    /// </summary>
    public static Task<byte[]> WriteSliceAsync(IColumnCodec codec, IColumn column, int start, int length)
        => WriteAsync(w => codec.WriteColumn(w, column, start, length));
}
