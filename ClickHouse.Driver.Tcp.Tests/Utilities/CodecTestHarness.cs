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

    /// <summary>Writes then reads back <paramref name="column"/> through <paramref name="codec"/>, returning the decoded column.</summary>
    public static async Task<IColumn> RoundTripAsync(IColumnCodec codec, IColumn column, string columnType, int rowCount)
    {
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));
        using ClickHouseBinaryReader reader = ReaderOver(bytes);
        return await codec.ReadColumnAsync(reader, column.Name, columnType, rowCount, None);
    }
}
