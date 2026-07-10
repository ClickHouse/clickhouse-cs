using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class DateColumnCodecTests
{
    [Test]
    public async Task Date_RoundTrip_PreservesDates()
    {
        var values = new[] { new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2149, 6, 6) };

        using var column = (IColumn<DateOnly>)await RoundTripAsync(DateColumnCodec.Instance, new ArrayColumn<DateOnly>("c", "Date", values), "Date", values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public async Task Date_ReadSingle_DecodesDaysSinceEpoch()
    {
        byte[] bytes = await WriteAsync(w => w.WriteUInt16(1)); // one day after the epoch
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<DateOnly>)await DateColumnCodec.Instance.ReadColumnAsync(reader, "c", "Date", 1, None);

        Assert.That(column[0], Is.EqualTo(new DateOnly(1970, 1, 2)));
    }

    [Test]
    public void Date_OutOfRange_Throws()
    {
        var column = new ArrayColumn<DateOnly>("c", "Date", new[] { new DateOnly(1969, 12, 31) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await WriteAsync(w => DateColumnCodec.Instance.WriteColumn(w, column)));
    }

    [Test]
    public async Task Date32_RoundTrip_PreservesDatesIncludingNegativeDays()
    {
        var values = new[] { new DateOnly(1900, 1, 1), new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2299, 12, 31) };

        using var column = (IColumn<DateOnly>)await RoundTripAsync(Date32ColumnCodec.Instance, new ArrayColumn<DateOnly>("c", "Date32", values), "Date32", values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public void Date32_OutOfRange_Throws()
    {
        var column = new ArrayColumn<DateOnly>("c", "Date32", new[] { new DateOnly(1899, 12, 31) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await WriteAsync(w => Date32ColumnCodec.Instance.WriteColumn(w, column)));
    }

    [Test]
    public void CanWrite_AcceptsDateOnly()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DateColumnCodec.Instance.CanWrite(new ArrayColumn<DateOnly>("c", "Date", Array.Empty<DateOnly>())), Is.True);
            Assert.That(DateColumnCodec.Instance.CanWrite(new ArrayColumn<DateTime>("c", "Date", Array.Empty<DateTime>())), Is.False);
        });
    }
}
