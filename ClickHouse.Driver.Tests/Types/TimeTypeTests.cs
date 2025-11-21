using System;
using System.IO;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class TimeTypeTests
{
    [Test]
    public void FrameworkType_ReturnsTimeSpan()
    {
        var type = new TimeType();
        Assert.That(type.FrameworkType, Is.EqualTo(typeof(TimeSpan)));
    }

    [TestCase(0, "0:00:00")]
    [TestCase(3661, "1:01:01")]
    [TestCase(-3661, "-1:01:01")]
    [TestCase(3599999, "999:59:59")] // Max value
    [TestCase(-3599999, "-999:59:59")] // Min value
    public void FormatTimeString_Int_FormatsCorrectly(int totalSeconds, string expected)
    {
        var result = TimeType.FormatTimeString(totalSeconds);
        Assert.That(result, Is.EqualTo(expected));
    }

    [TestCase(0, 0, 0, "0:00:00")]
    [TestCase(1, 1, 1, "1:01:01")]
    [TestCase(-1, -1, -1, "-1:01:01")]
    [TestCase(999, 59, 59, "999:59:59")] // Max value
    [TestCase(-999, -59, -59, "-999:59:59")] // Min value
    public void FormatTimeString_TimeSpan_FormatsCorrectly(int hours, int minutes, int seconds, string expected)
    {
        var timeSpan = new TimeSpan(hours, minutes, seconds);
        var result = TimeType.FormatTimeString(timeSpan);
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    [TestCase(1,30,45, 5445)]
    [TestCase(-1,-30,-45, -5445)]
    public void Write_TimeSpan_WritesCorrectBytes(int hours, int minutes, int seconds, int expectedSeconds)
    {
        var type = new TimeType();
        var timeSpan = new TimeSpan(hours, minutes, seconds);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, timeSpan);

        stream.Position = 0;
        using var reader = new BinaryReader(stream);
        var actualSeconds = reader.ReadInt32();

        Assert.That(actualSeconds, Is.EqualTo(expectedSeconds));
    }


    [Test]
    public void Write_IntValue_WritesCorrectly()
    {
        var type = new TimeType();
        var seconds = 5400;

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, seconds);

        stream.Position = 0;
        using var reader = new BinaryReader(stream);
        var actualSeconds = reader.ReadInt32();

        Assert.That(actualSeconds, Is.EqualTo(seconds));
    }

    [Test]
    public void Write_TimeSpanWithMilliseconds_RoundsToNearestSecond()
    {
        var type = new TimeType();
        var timeSpan = TimeSpan.FromSeconds(5.7); // 5.7 seconds

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, timeSpan);

        stream.Position = 0;
        using var reader = new BinaryReader(stream);
        var actualSeconds = reader.ReadInt32();

        Assert.That(actualSeconds, Is.EqualTo(6)); // Rounded up
    }

    [Test]
    public void Write_ValueBeyondMaxRange_ClampedToMax()
    {
        var type = new TimeType();
        var hugeTimeSpan = TimeSpan.FromHours(10000); // Way beyond 999:59:59

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, hugeTimeSpan);

        stream.Position = 0;
        using var reader = new BinaryReader(stream);
        var actualSeconds = reader.ReadInt32();

        Assert.That(actualSeconds, Is.EqualTo(TimeType.MaxSeconds));
    }

    [Test]
    public void Write_ValueBeyondMinRange_ClampedToMin()
    {
        var type = new TimeType();
        var hugeTimeSpan = TimeSpan.FromHours(-10000); // Way beyond -999:59:59

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, hugeTimeSpan);

        stream.Position = 0;
        using var reader = new BinaryReader(stream);
        var actualSeconds = reader.ReadInt32();

        Assert.That(actualSeconds, Is.EqualTo(TimeType.MinSeconds));
    }

    [Test]
    public void Write_UnsupportedType_ThrowsNotSupportedException()
    {
        var type = new TimeType();

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        Assert.Throws<NotSupportedException>(() => type.Write(writer, "not a time"));
    }

    [Test]
    public void Write_NullValue_ThrowsNotSupportedException()
    {
        var type = new TimeType();

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        Assert.Throws<NotSupportedException>(() => type.Write(writer, null));
    }

    [Test]
    [TestCase(5445)]
    [TestCase(-8130)]
    [TestCase(0)]
    public void Read_Seconds_ReturnsCorrectTimeSpan(int seconds)
    {
        var type = new TimeType();

        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(seconds);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = type.Read(reader);

        Assert.That(result, Is.TypeOf<TimeSpan>());
        var timeSpan = (TimeSpan)result;
        Assert.That(timeSpan, Is.EqualTo(TimeSpan.FromSeconds(seconds)));
    }
}
