using System;
using System.IO;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class FixedStringTypeTests
{
    [Test]
    public void Write_WithMatchingByteArrayLength_Succeeds()
    {
        var type = new FixedStringType { Length = 4 };
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        var bytes = new byte[] { 1, 2, 3, 4 };
        Assert.DoesNotThrow(() => type.Write(writer, bytes));

        Assert.That(stream.ToArray(), Is.EqualTo(bytes));
    }

    [Test]
    public void Write_WithShorterByteArray_ThrowsArgumentException()
    {
        var type = new FixedStringType { Length = 4 };
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        var bytes = new byte[] { 1, 2 };
        var ex = Assert.Throws<ArgumentException>(() => type.Write(writer, bytes));

        Assert.That(ex.Message, Does.Contain("length 2"));
        Assert.That(ex.Message, Does.Contain("FixedString(4)"));
    }

    [Test]
    public void Write_WithLongerByteArray_ThrowsArgumentException()
    {
        var type = new FixedStringType { Length = 4 };
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        var bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
        var ex = Assert.Throws<ArgumentException>(() => type.Write(writer, bytes));

        Assert.That(ex.Message, Does.Contain("length 6"));
        Assert.That(ex.Message, Does.Contain("FixedString(4)"));
    }

    [Test]
    public void Write_WithNull_ThrowsArgumentException()
    {
        var type = new FixedStringType { Length = 4 };
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        var ex = Assert.Throws<ArgumentException>(() => type.Write(writer, null));

        Assert.That(ex.Message, Does.Contain("null"));
    }

    [Test]
    public void Write_WithUnsupportedType_ThrowsArgumentException()
    {
        var type = new FixedStringType { Length = 4 };
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        var ex = Assert.Throws<ArgumentException>(() => type.Write(writer, 12345));

        Assert.That(ex.Message, Does.Contain("Int32"));
    }

    [Test]
    public void Write_WithString_PadsWithNullBytes()
    {
        var type = new FixedStringType { Length = 6 };
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        type.Write(writer, "test");

        var result = stream.ToArray();
        Assert.That(result, Is.EqualTo(new byte[] { (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0, 0 }));
    }
}
