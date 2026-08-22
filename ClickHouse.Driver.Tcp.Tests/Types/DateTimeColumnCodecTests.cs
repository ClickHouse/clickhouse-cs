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

    [Test]
    public async Task WriteColumn_DateTimeOffset_EncodesSameUnixSecondsAsEquivalentDateTime()
    {
        // The same instant expressed as a DateTimeOffset (with a non-zero offset) and as a UTC DateTime must
        // produce identical column bodies — both are epoch seconds of the UTC instant.
        DateTime utc = DateTime.UnixEpoch.AddSeconds(1_700_000_000);
        var offset = new DateTimeOffset(utc, TimeSpan.Zero).ToOffset(TimeSpan.FromHours(5));

        byte[] fromDateTime = await WriteAsync(w => DateTimeColumnCodec.Instance.WriteColumn(w, new ArrayColumn<DateTime>("c", "DateTime", new[] { utc })));
        byte[] fromOffset = await WriteAsync(w => DateTimeColumnCodec.Instance.WriteColumn(w, new ArrayColumn<DateTimeOffset>("c", "DateTime", new[] { offset })));

        CollectionAssert.AreEqual(fromDateTime, fromOffset);
    }

    [Test]
    public void WriteColumn_DateTimeOutsideClickHouseRange_Throws()
    {
        using var ms = new MemoryStream();
        using var writer = new ClickHouseBinaryWriter(ms);
        var beforeEpoch = new ArrayColumn<DateTime>("c", "DateTime", new[] { new DateTime(1969, 12, 31, 23, 59, 59, DateTimeKind.Utc) });
        var pastMax = new ArrayColumn<DateTime>("c", "DateTime", new[] { new DateTime(2200, 1, 1, 0, 0, 0, DateTimeKind.Utc) });

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => DateTimeColumnCodec.Instance.WriteColumn(writer, beforeEpoch));
            Assert.Throws<ArgumentOutOfRangeException>(() => DateTimeColumnCodec.Instance.WriteColumn(writer, pastMax));
        });
    }

    [Test]
    public void CanWrite_AcceptsDateTimeAndDateTimeOffset_RejectsOthers()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DateTimeColumnCodec.Instance.CanWrite(new ArrayColumn<DateTime>("c", "DateTime", Array.Empty<DateTime>())), Is.True);
            Assert.That(DateTimeColumnCodec.Instance.CanWrite(new ArrayColumn<DateTimeOffset>("c", "DateTime", Array.Empty<DateTimeOffset>())), Is.True);
            Assert.That(DateTimeColumnCodec.Instance.CanWrite(new ArrayColumn<string>("c", "DateTime", Array.Empty<string>())), Is.False);
        });
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
