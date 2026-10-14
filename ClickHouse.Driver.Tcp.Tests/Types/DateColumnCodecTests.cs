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
    public async Task Date_ReadSingle_DecodesDaysSinceEpoch()
    {
        byte[] bytes = await WriteAsync(w => w.WriteUInt16(1)); // one day after the epoch
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<DateOnly>)await DateColumnCodec.Instance.ReadColumnAsync(reader, "c", "Date", 1, None);

        Assert.That(column[0], Is.EqualTo(new DateOnly(1970, 1, 2)));
    }

    // An ArrayColumn<DateOnly> is the storage of Date and Date32, so the codec writes it and checks the range.
    [Test]
    public void Date_OutOfRange_Throws()
    {
        var column = new ArrayColumn<DateOnly>("c", "Date", new[] { new DateOnly(1969, 12, 31) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => WriteStoredAsync(DateColumnCodec.Instance, column, 0, column.RowCount));
    }

    [Test]
    public void Date32_OutOfRange_Throws()
    {
        var column = new ArrayColumn<DateOnly>("c", "Date32", new[] { new DateOnly(1899, 12, 31) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => WriteStoredAsync(Date32ColumnCodec.Instance, column, 0, column.RowCount));
    }
}
