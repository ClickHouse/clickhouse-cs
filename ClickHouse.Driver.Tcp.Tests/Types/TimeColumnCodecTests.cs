using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class TimeColumnCodecTests
{
    // Time/Time64 value round-trips run against a live server (InsertRoundTripCase, with the Time type flag
    // enabled). These unit tests cover only the write-side range validation and precision truncation a server
    // round-trip does not.
    [Test]
    public void Time_OutOfRange_Throws()
    {
        var column = new ArrayColumn<TimeSpan>("c", "Time", new[] { TimeSpan.FromHours(1000) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await WriteAsync(w => TimeColumnCodec.Instance.WriteColumn(w, column)));
    }

    [Test]
    public async Task Time_SubSecondPrecision_TruncatesTowardZero()
    {
        // Time holds whole seconds only; the 500 ms component is dropped rather than rejected — precision is the caller's call.
        var column = new ArrayColumn<TimeSpan>("c", "Time", new[] { new TimeSpan(0, 0, 0, 1, 500) });
        byte[] bytes = await WriteAsync(w => TimeColumnCodec.Instance.WriteColumn(w, column));
        Assert.That(BitConverter.ToInt32(bytes), Is.EqualTo(1));
    }

    [Test]
    public async Task Time_WholeSeconds_Writes()
    {
        var column = new ArrayColumn<TimeSpan>("c", "Time", new[] { new TimeSpan(1, 2, 3) });
        byte[] bytes = await WriteAsync(w => TimeColumnCodec.Instance.WriteColumn(w, column));
        Assert.That(bytes.Length, Is.EqualTo(sizeof(int)));
    }

    [Test]
    public async Task Time64_SubScalePrecision_TruncatesTowardZero()
    {
        // Time64(3) is milliseconds; sub-millisecond ticks are truncated toward zero rather than rejected.
        // 4_560_789 ticks = 456.0789 ms → 456 at scale 3.
        Time64ColumnCodec codec = Time64ColumnCodec.Create(TypeParser.Parse("Time64(3)"));
        var column = new ArrayColumn<TimeSpan>("c", "Time64(3)", new[] { TimeSpan.FromTicks(4_560_789) });
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));
        Assert.That(BitConverter.ToInt64(bytes), Is.EqualTo(456));
    }

    [Test]
    public async Task Time64_ExactScale_Writes()
    {
        // A whole-millisecond value is exactly representable at scale 3.
        Time64ColumnCodec codec = Time64ColumnCodec.Create(TypeParser.Parse("Time64(3)"));
        var column = new ArrayColumn<TimeSpan>("c", "Time64(3)", new[] { new TimeSpan(0, 0, 0, 1, 456) });
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));
        Assert.That(bytes.Length, Is.EqualTo(sizeof(long)));
    }

    [Test]
    public void Time64_OutOfRange_Throws()
    {
        Time64ColumnCodec codec = Time64ColumnCodec.Create(TypeParser.Parse("Time64(3)"));
        var column = new ArrayColumn<TimeSpan>("c", "Time64(3)", new[] { TimeSpan.FromHours(1000) });
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await WriteAsync(w => codec.WriteColumn(w, column)));
    }

    [Test]
    public void Time64_MissingScale_Throws()
        => Assert.Throws<FormatException>(() => Time64ColumnCodec.Create(TypeParser.Parse("Time64")));

    [Test]
    public void Time64_ScaleOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => Time64ColumnCodec.Create(TypeParser.Parse("Time64(10)")));
}
