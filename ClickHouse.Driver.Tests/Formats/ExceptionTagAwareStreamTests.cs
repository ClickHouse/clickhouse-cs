using System;
using System.IO;
using System.Text;
using ClickHouse.Driver.Formats;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Formats;

public class ExceptionTagAwareStreamTests
{
    private const string TestToken = "PU1FNUFH98";

    [Test]
    public void Constructor_ValidatesNullStream()
    {
        Assert.Throws<System.ArgumentNullException>(() => new ExceptionTagAwareStream(null, TestToken));
    }

    [Test]
    public void Constructor_ValidatesNullTag()
    {
        using var ms = new MemoryStream();
        Assert.Throws<System.ArgumentException>(() => new ExceptionTagAwareStream(ms, null));
    }

    [Test]
    public void Constructor_ValidatesEmptyTag()
    {
        using var ms = new MemoryStream();
        Assert.Throws<System.ArgumentException>(() => new ExceptionTagAwareStream(ms, ""));
    }

    [Test]
    public void Constructor_ValidatesTagLength()
    {
        using var ms = new MemoryStream();
        Assert.Throws<System.ArgumentException>(() => new ExceptionTagAwareStream(ms, "short"));
        Assert.Throws<System.ArgumentException>(() => new ExceptionTagAwareStream(ms, "toolongtoken"));
    }

    [Test]
    public void Read_PassesThroughToInnerStream()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[5];
        int bytesRead = stream.Read(buffer, 0, 5);

        Assert.That(bytesRead, Is.EqualTo(5));
        Assert.That(buffer, Is.EqualTo(data));
    }

    [Test]
    public void ReadByte_PassesThroughToInnerStream()
    {
        var data = new byte[] { 42, 43, 44 };
        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        Assert.That(stream.ReadByte(), Is.EqualTo(42));
        Assert.That(stream.ReadByte(), Is.EqualTo(43));
        Assert.That(stream.ReadByte(), Is.EqualTo(44));
        Assert.That(stream.ReadByte(), Is.EqualTo(-1)); // EOF
    }

    [Test]
    public void TryExtractMidStreamException_ReturnsNull_WhenNoMarkerPresent()
    {
        var data = Encoding.UTF8.GetBytes("Some random data without any exception markers");
        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read all data
        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);
        Assert.That(result, Is.Null);
    }

    [Test]
    public void TryExtractMidStreamException_ReturnsNull_WhenBufferTooSmall()
    {
        var data = new byte[] { 1, 2, 3 }; // Too small to contain 23-byte marker
        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);
        Assert.That(result, Is.Null);
    }

    [Test]
    public void TryExtractMidStreamException_DetectsMarker_WithCompleteFormat()
    {
        // Format: __exception__TOKEN\n<message>\n<size> TOKEN__exception__
        var exceptionData = $"__exception__{TestToken}\nTest error message\n18 {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes("Some data before" + exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException("SELECT 1");

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Test error message"));
        Assert.That(result.Query, Is.EqualTo("SELECT 1"));
    }

    [Test]
    public void TryExtractMidStreamException_DetectsMarker_WithMultilineMessage()
    {
        var message = "Error on line 1\nMore details on line 2\nAnd line 3";
        var messageLength = Encoding.UTF8.GetByteCount(message);
        var exceptionData = $"__exception__{TestToken}\n{message}\n{messageLength} {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo(message));
    }

    [Test]
    public void TryExtractMidStreamException_ExtractsMessage_WithoutClosingMarker()
    {
        // Incomplete format - no closing marker
        var exceptionData = $"__exception__{TestToken}\nPartial error message";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Partial error message"));
    }

    [Test]
    public void TryExtractMidStreamException_ParsesErrorCode()
    {
        var exceptionData = $"__exception__{TestToken}\nCode: 123. Error message here\n30 {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.ErrorCode, Is.EqualTo(123));
    }

    [Test]
    public void RingBuffer_RecordsBytes_FromBulkRead()
    {
        var prefix = new byte[100];
        var exceptionData = $"__exception__{TestToken}\nBulk read error\n15 {TestToken}__exception__";
        var data = new byte[prefix.Length + Encoding.UTF8.GetByteCount(exceptionData)];
        Array.Copy(prefix, 0, data, 0, prefix.Length);
        Encoding.UTF8.GetBytes(exceptionData, 0, exceptionData.Length, data, prefix.Length);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read all at once
        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Bulk read error"));
    }

    [Test]
    public void RingBuffer_RecordsBytes_FromByteByByteRead()
    {
        var exceptionData = $"__exception__{TestToken}\nByte by byte error\n18 {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read byte by byte
        while (stream.ReadByte() != -1) { }

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Byte by byte error"));
    }

    [Test]
    public void RingBuffer_WrapsCorrectly_WhenOverflowing()
    {
        // Create data larger than 4KB buffer
        var prefix = new byte[5000]; // More than 4KB
        var exceptionData = $"__exception__{TestToken}\nOverflow test error\n19 {TestToken}__exception__";
        var suffix = Encoding.UTF8.GetBytes(exceptionData);

        var data = new byte[prefix.Length + suffix.Length];
        Array.Copy(prefix, 0, data, 0, prefix.Length);
        Array.Copy(suffix, 0, data, prefix.Length, suffix.Length);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read all data - buffer should wrap
        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Overflow test error"));
    }

    [Test]
    public void RingBuffer_HandlesMultipleSmallReads()
    {
        var exceptionData = $"__exception__{TestToken}\nSmall reads error\n17 {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read in small chunks
        var buffer = new byte[10];
        while (stream.Read(buffer, 0, 10) > 0) { }

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Small reads error"));
    }

    [Test]
    public void StreamProperties_DelegateToInnerStream()
    {
        using var ms = new MemoryStream(new byte[100]);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        Assert.That(stream.CanRead, Is.EqualTo(ms.CanRead));
        Assert.That(stream.CanSeek, Is.EqualTo(ms.CanSeek));
        Assert.That(stream.CanWrite, Is.EqualTo(ms.CanWrite));
        Assert.That(stream.Length, Is.EqualTo(ms.Length));
    }

    [Test]
    public void Dispose_DisposesInnerStream()
    {
        var ms = new MemoryStream(new byte[10]);
        var stream = new ExceptionTagAwareStream(ms, TestToken);

        stream.Dispose();

        Assert.Throws<System.ObjectDisposedException>(() => ms.ReadByte());
    }

    [Test]
    public void TryExtractMidStreamException_IgnoresWrongToken()
    {
        // Use a different token in the data than what the stream is configured with
        var wrongToken = "WRONGTOKEN";
        var exceptionData = $"__exception__{wrongToken}\nWrong token error\n17 {wrongToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken); // Looking for TestToken

        var buffer = new byte[data.Length];
        stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException(null);

        Assert.That(result, Is.Null); // Should not match wrong token
    }
}
