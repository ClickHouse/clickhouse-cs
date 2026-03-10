using System;
using System.IO;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class Time64TypeTests
{
    [Test]
    public void ToString_WithScale_ReturnsCorrectFormat()
    {
        var type = new Time64Type { Scale = 3 };
        Assert.That(type.ToString(), Is.EqualTo("Time64(3)"));
    }

    [Test]
    public void Name_ReturnsTime64()
    {
        var type = new Time64Type { Scale = 3 };
        Assert.That(type.Name, Is.EqualTo("Time64"));
    }

    [Test]
    public void FrameworkType_ReturnsTimeSpan()
    {
        var type = new Time64Type { Scale = 3 };
        Assert.That(type.FrameworkType, Is.EqualTo(typeof(TimeSpan)));
    }

    [TestCase("9")]
    public void Parse_ValidScale_CreatesTypeWithCorrectScale(string scaleStr)
    {
        var node = Parser.Parse($"Time64({scaleStr})");
        var type = new Time64Type();
        var result = (Time64Type)type.Parse(node, null, TypeSettings.Default);

        Assert.That(result.Scale, Is.EqualTo(int.Parse(scaleStr)));
    }

    [Test]
    public void Parse_NoParameter_ThrowsArgumentException()
    {
        var node = Parser.Parse("Time64");
        var type = new Time64Type();

        var ex = Assert.Throws<ArgumentException>(() => type.Parse(node, null, TypeSettings.Default));
        Assert.That(ex.Message, Does.Contain("Time64 requires a precision parameter"));
    }

    [TestCase("-1")]
    [TestCase("10")]
    public void Parse_ScaleOutOfRange_ThrowsArgumentOutOfRangeException(string scale)
    {
        var node = Parser.Parse($"Time64({scale})");
        var type = new Time64Type();

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => type.Parse(node, null, TypeSettings.Default));
    }

    [Test]
    public void Parse_InvalidNumber_ThrowsFormatException()
    {
        var node = Parser.Parse("Time64(abc)");
        var type = new Time64Type();

        Assert.Throws<FormatException>(() => type.Parse(node, null, TypeSettings.Default));
    }

    [TestCase(0, 0, 0, 0)] // Zero
    [TestCase(3600, 1, 0, 0)] // 1 hour
    [TestCase(90, 0, 1, 30)] // 1 minute 30 seconds
    [TestCase(3661, 1, 1, 1)] // 1:01:01
    public void ToClickHouseDecimal_TimeSpan_ConvertsCorrectly(double expectedSeconds, int hours, int minutes, int seconds)
    {
        var type = new Time64Type { Scale = 6 };
        var timeSpan = new TimeSpan(hours, minutes, seconds);

        var result = type.ToClickHouseDecimal(timeSpan);

        Assert.That(result, Is.EqualTo((decimal)expectedSeconds));
    }

    [Test]
    public void ToClickHouseDecimal_RoundsToScale()
    {
        var type = new Time64Type { Scale = 3 }; // milliseconds
        var timeSpan = TimeSpan.FromSeconds(1.123456789); // More precision than scale 3

        var result = type.ToClickHouseDecimal(timeSpan);

        Assert.That(result, Is.EqualTo(1.123m)); // Rounded to 3 decimal places
    }

    [Test]
    [TestCase(3661.123456)]
    [TestCase(-3661.123456)]
    public void FromClickHouseDecimal_ConvertsToTimeSpan(decimal fractionalSeconds)
    {
        var type = new Time64Type { Scale = 6 };
        
        var result = Time64Type.FromClickHouseDecimal(fractionalSeconds);

        var expected = TimeSpan.FromSeconds((double)fractionalSeconds);
        Assert.That(result, Is.EqualTo(expected).Within(TimeSpan.FromTicks(1)));
    }

    [Test]
    public void RoundTrip_ToAndFromClickHouseDecimal_PreservesValue()
    {
        var type = new Time64Type { Scale = 6 };
        var original = new TimeSpan(5, 25, 17) + TimeSpan.FromMilliseconds(123.456);

        var decimalValue = type.ToClickHouseDecimal(original);
        var result = Time64Type.FromClickHouseDecimal(decimalValue);

        Assert.That(result, Is.EqualTo(original).Within(TimeSpan.FromTicks(10)));
    }

    [TestCase(0, "0:00:00")]
    [TestCase(3, "0:00:00.000")]
    [TestCase(6, "0:00:00.000000")]
    public void FormatTime64String_Zero_FormatsWithCorrectPrecision(int scale, string expected)
    {
        var type = new Time64Type { Scale = scale };
        var result = type.FormatTime64String(TimeSpan.Zero);
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public void FormatTime64String_NegativeTimeSpanWithMicroseconds_CorrectlyFormats()
    {
        var type = new Time64Type { Scale = 6 };
        var timeSpan = -TimeSpan.FromSeconds(3661.123456);

        var result = type.FormatTime64String(timeSpan);

        Assert.That(result, Is.EqualTo("-1:01:01.123456"));
    }
    
    [TestCase("")]
    [TestCase("   ")]
    public void CoerceToTimeSpan_NullOrEmptyString_ThrowsArgumentException(string timeString)
    {
        var type = new Time64Type { Scale = 3 };

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        var ex = Assert.Throws<ArgumentException>(() => type.Write(writer, timeString));
        Assert.That(ex.Message, Does.Contain("cannot be null or empty"));
    }

    [TestCase("1:30")]
    [TestCase("1")]
    [TestCase("1:30:45:67")]
    [TestCase("abc:30:45")]
    [TestCase("1:abc:45")]
    [TestCase("1:30:abc")]
    public void CoerceToTimeSpan_InvalidFormat_ThrowsFormatException(string timeString)
    {
        var type = new Time64Type { Scale = 3 };

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        Assert.Throws<FormatException>(() => type.Write(writer, timeString));
    }

    [Test]
    [TestCase("01:05:06", 6956)]
    [TestCase("-01:05:06", -6956)]
    [TestCase("01:05:06.123456", 6956.123456)]
    [TestCase("-01:05:06.123456", -6956.123456)]
    public void CoerceToTimeSpan_String_ConvertsCorrectly(string time, double seconds)
    {
        var type = new Time64Type { Scale = 6 };

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, seconds);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        var expected = TimeSpan.FromSeconds((double)seconds);
        Assert.That(result, Is.EqualTo(expected).Within(TimeSpan.FromTicks(10)));
    }
    
    [Test]
    public void CoerceToTimeSpan_DecimalSeconds_ConvertsCorrectly()
    {
        var type = new Time64Type { Scale = 6 };
        var seconds = 3661.123456m;

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, seconds);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        var expected = TimeSpan.FromSeconds((double)seconds);
        Assert.That(result, Is.EqualTo(expected).Within(TimeSpan.FromTicks(10)));
    }

    [Test]
    public void CoerceToTimeSpan_DoubleSeconds_ConvertsCorrectly()
    {
        var type = new Time64Type { Scale = 3 };
        var seconds = 1234.567;

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, seconds);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        var expected = TimeSpan.FromSeconds(seconds);
        Assert.That(result, Is.EqualTo(expected).Within(TimeSpan.FromMilliseconds(1)));
    }

    [Test]
    public void CoerceToTimeSpan_FloatSeconds_ConvertsCorrectly()
    {
        var type = new Time64Type { Scale = 3 };
        float seconds = 1234.567f;

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, seconds);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        var expected = TimeSpan.FromSeconds(seconds);
        Assert.That(result, Is.EqualTo(expected).Within(TimeSpan.FromMilliseconds(1)));
    }

    [Test]
    public void CoerceToTimeSpan_IntSeconds_ConvertsCorrectly()
    {
        var type = new Time64Type { Scale = 0 };
        var seconds = 3600;

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, seconds);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        Assert.That(result, Is.EqualTo(TimeSpan.FromSeconds(seconds)));
    }

    [Test]
    public void CoerceToTimeSpan_LongSeconds_ConvertsCorrectly()
    {
        var type = new Time64Type { Scale = 0 };
        long seconds = 7200L;

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, seconds);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        Assert.That(result, Is.EqualTo(TimeSpan.FromSeconds(seconds)));
    }

    [Test]
    public void CoerceToTimeSpan_UnsupportedType_ThrowsNotSupportedException()
    {
        var type = new Time64Type { Scale = 3 };

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        var ex = Assert.Throws<NotSupportedException>(() => type.Write(writer, new object()));
        Assert.That(ex.Message, Does.Contain("Cannot convert"));
    }

    [Test]
    public void Write_ValueBeyondMaxRange_ClampedToMax()
    {
        var type = new Time64Type { Scale = 3 };
        var hugeTimeSpan = TimeSpan.FromHours(10000);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, hugeTimeSpan);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        // Should be clamped to max: 999:59:59.999
        Assert.That(result.TotalSeconds, Is.LessThanOrEqualTo(Time64Type.MaxSeconds));
    }

    [Test]
    public void Write_ValueBeyondMinRange_ClampedToMin()
    {
        var type = new Time64Type { Scale = 3 };
        var hugeTimeSpan = TimeSpan.FromHours(-10000);

        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, hugeTimeSpan);

        stream.Position = 0;
        using var reader = new ExtendedBinaryReader(stream);
        var result = (TimeSpan)type.Read(reader);

        // Should be clamped to min: -999:59:59.999
        Assert.That(result.TotalSeconds, Is.GreaterThanOrEqualTo(Time64Type.MinSeconds));
    }

    [Test]
    public void FormatTime64String_PadsMinutesAndSeconds()
    {
        var type = new Time64Type { Scale = 0 };
        var timeSpan = new TimeSpan(1, 5, 7); // 1:05:07

        var result = type.FormatTime64String(timeSpan);

        Assert.That(result, Is.EqualTo("1:05:07"));
    }

    [Test]
    public void ToClickHouseDecimal_Scale0_NoDecimalPlaces()
    {
        var type = new Time64Type { Scale = 0 };
        var timeSpan = TimeSpan.FromSeconds(1.999); // Should round to 2

        var result = type.ToClickHouseDecimal(timeSpan);

        Assert.That(result, Is.EqualTo(2m));
    }
}
