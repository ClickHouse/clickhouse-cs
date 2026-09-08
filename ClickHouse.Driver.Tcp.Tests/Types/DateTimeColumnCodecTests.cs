using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DateTimeColumnCodecTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task RoundTrip_UtcWholeSeconds_Preserved()
    {
        var values = new[] { DateTime.UnixEpoch, DateTime.UnixEpoch.AddSeconds(1_700_000_000) };

        byte[] bytes = await WriteAsync(w => DateTimeColumnCodec.Instance.WriteColumn(w, new ArrayColumn<DateTime>("c", "DateTime", values)));
        using var reader = ReaderOver(bytes);
        using var column = (IColumn<DateTime>)await DateTimeColumnCodec.Instance.ReadColumnAsync(reader, "c", "DateTime('UTC')", values.Length, None);

        Assert.Multiple(() =>
        {
            CollectionAssert.AreEqual(values, column.Values.ToArray());
            Assert.That(column.TypeName, Is.EqualTo("DateTime('UTC')"));
        });
    }

    [Test]
    public async Task ReadColumn_SingleValue_DecodesUnixSecondsAsUtc()
    {
        // 1 row: the little-endian UInt32 1000 = 1970-01-01T00:16:40Z.
        byte[] bytes = await WriteAsync(w => w.WriteUInt32(1000));
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<DateTime>)await DateTimeColumnCodec.Instance.ReadColumnAsync(reader, "c", "DateTime", 1, None);

        Assert.That(column[0], Is.EqualTo(DateTime.UnixEpoch.AddSeconds(1000)));
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<DateTime>)await DateTimeColumnCodec.Instance.ReadColumnAsync(reader, "c", "DateTime", 0, None);
        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
