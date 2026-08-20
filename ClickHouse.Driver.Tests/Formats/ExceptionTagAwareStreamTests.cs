using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
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
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();
        Assert.That(result, Is.Null);
    }

    [Test]
    public void TryExtractMidStreamException_ReturnsNull_WhenBufferTooSmall()
    {
        var data = new byte[] { 1, 2, 3 }; // Too small to contain 23-byte marker
        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();
        Assert.That(result, Is.Null);
    }

    [Test]
    public void TryExtractMidStreamException_DetectsMarker_WithCompleteFormat()
    {
        // Format: __exception__TOKEN\n<message>\n<size> TOKEN__exception__
        var message = "Test error message";
        var exceptionData = $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes("Some data before" + exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Test error message"));
    }

    [Test]
    public void TryExtractMidStreamException_DetectsMarker_WithServerCrlfFraming()
    {
        // Real ClickHouse (>= 25.11) frames the in-band block with a CRLF between the
        // "__exception__" literal and the tag, which the contiguous fixtures do not cover:
        // "\r\n__exception__\r\n<tag>\r\n<message>\n<len> <tag>\r\n__exception__\r\n".
        var message = "Code: 395. DB::Exception: boom mid stream";
        var messageLength = Encoding.UTF8.GetByteCount(message);
        var exceptionData =
            $"\r\n__exception__\r\n{TestToken}\r\n{message}\n{messageLength} {TestToken}\r\n__exception__\r\n";
        var data = Encoding.UTF8.GetBytes("some rows before" + exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo(message));
        Assert.That(result.ErrorCode, Is.EqualTo(395));
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
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

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
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Partial error message"));
    }

    [Test]
    public void TryExtractMidStreamException_ParsesErrorCode()
    {
        var message = "Code: 123. Error message here";
        var exceptionData = $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        var buffer = new byte[data.Length];
        _ = _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.ErrorCode, Is.EqualTo(123));
    }

    [Test]
    public void RingBuffer_RecordsBytes_FromBulkRead()
    {
        var prefix = new byte[100];
        var message = "Bulk read error";
        var exceptionData = $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__";
        var data = new byte[prefix.Length + Encoding.UTF8.GetByteCount(exceptionData)];
        Array.Copy(prefix, 0, data, 0, prefix.Length);
        Encoding.UTF8.GetBytes(exceptionData, 0, exceptionData.Length, data, prefix.Length);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read all at once
        var buffer = new byte[data.Length];
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Bulk read error"));
    }

    [Test]
    public void RingBuffer_RecordsBytes_FromByteByByteRead()
    {
        var message = "Byte by byte error";
        var exceptionData = $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read byte by byte
        while (stream.ReadByte() != -1) { }

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Byte by byte error"));
    }

    [Test]
    public void RingBuffer_WrapsCorrectly_WhenOverflowing()
    {
        // Create data larger than 4KB buffer
        var prefix = new byte[5000]; // More than 4KB
        var message = "Overflow test error";
        var exceptionData = $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__";
        var suffix = Encoding.UTF8.GetBytes(exceptionData);

        var data = new byte[prefix.Length + suffix.Length];
        Array.Copy(prefix, 0, data, 0, prefix.Length);
        Array.Copy(suffix, 0, data, prefix.Length, suffix.Length);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Read all data - buffer should wrap
        var buffer = new byte[data.Length];
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Overflow test error"));
    }

    // ----- Read(Span<byte>) — must record into the ring buffer exactly as the byte[] overload does,
    // otherwise mid-stream exception detection silently stops working on span reads. -----

    [Test]
    public void ReadSpan_PassesThroughToInnerStream()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        Span<byte> buffer = new byte[5];
        int bytesRead = stream.Read(buffer);

        Assert.That(bytesRead, Is.EqualTo(5));
        Assert.That(buffer.ToArray(), Is.EqualTo(data));
    }

    [Test]
    public void RingBuffer_RecordsBytes_FromSpanRead()
    {
        var prefix = new byte[100];
        var message = "Span read error";
        var exceptionData = $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__";
        var data = new byte[prefix.Length + Encoding.UTF8.GetByteCount(exceptionData)];
        Array.Copy(prefix, 0, data, 0, prefix.Length);
        Encoding.UTF8.GetBytes(exceptionData, 0, exceptionData.Length, data, prefix.Length);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        _ = stream.Read(new byte[data.Length].AsSpan());

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Span read error"));
    }

    [Test]
    public void RingBuffer_WrapsCorrectly_FromSpanRead()
    {
        // Exceeds the 4 KiB ring buffer, so the span path must handle the wrap branch too.
        var prefix = new byte[5000];
        var message = "Span overflow error";
        var suffix = Encoding.UTF8.GetBytes(
            $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__");

        var data = new byte[prefix.Length + suffix.Length];
        Array.Copy(prefix, 0, data, 0, prefix.Length);
        Array.Copy(suffix, 0, data, prefix.Length, suffix.Length);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken);

        // Small spans so the ring buffer wraps repeatedly instead of taking the single-copy shortcut.
        Span<byte> chunk = new byte[64];
        while (stream.Read(chunk) > 0)
        {
        }

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Not.Null);
        Assert.That(result.Message, Is.EqualTo("Span overflow error"));
    }

    [Test]
    public void RingBuffer_SpanAndArrayReads_ExtractIdenticalException()
    {
        var message = "Interleaved error";
        var data = Encoding.UTF8.GetBytes(
            new string('x', 200) +
            $"__exception__{TestToken}\n{message}\n{message.Length} {TestToken}__exception__");

        // Same payload, one stream driven through spans and one through arrays: both must detect it.
        using var spanInner = new MemoryStream(data);
        using var spanStream = new ExceptionTagAwareStream(spanInner, TestToken);
        Span<byte> spanChunk = new byte[7];
        while (spanStream.Read(spanChunk) > 0)
        {
        }

        using var arrayInner = new MemoryStream(data);
        using var arrayStream = new ExceptionTagAwareStream(arrayInner, TestToken);
        var arrayChunk = new byte[7];
        while (arrayStream.Read(arrayChunk, 0, arrayChunk.Length) > 0)
        {
        }

        var viaSpan = spanStream.TryExtractMidStreamException();
        var viaArray = arrayStream.TryExtractMidStreamException();

        Assert.That(viaSpan, Is.Not.Null);
        Assert.That(viaArray, Is.Not.Null);
        Assert.That(viaSpan.Message, Is.EqualTo(viaArray.Message));
        Assert.That(viaSpan.Message, Is.EqualTo("Interleaved error"));
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
        var message = "Wrong token error";
        var exceptionData = $"__exception__{wrongToken}\n{message}\n{message.Length} {wrongToken}__exception__";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken); // Looking for TestToken

        var buffer = new byte[data.Length];
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Null); // Should not match wrong token
    }

    [Test]
    public void TryExtractMidStreamException_IgnoresWrongToken_WithServerCrlfFraming()
    {
        // The CRLF-tolerant matcher must not loosen tag matching: a real-framed block whose tag
        // differs from the configured one must still be ignored.
        var wrongToken = "WRONGTOKEN";
        var message = "Wrong token error";
        var exceptionData =
            $"\r\n__exception__\r\n{wrongToken}\r\n{message}\n{message.Length} {wrongToken}\r\n__exception__\r\n";
        var data = Encoding.UTF8.GetBytes(exceptionData);

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken); // Looking for TestToken

        var buffer = new byte[data.Length];
        _ = stream.Read(buffer, 0, buffer.Length);

        var result = stream.TryExtractMidStreamException();

        Assert.That(result, Is.Null); // Should not match wrong token
    }

    public enum ReadApi
    {
        SyncRead,
        ReadByte,
        AsyncReadArray,
        AsyncReadMemory,
    }

    private static async Task DrainAsync(ExceptionTagAwareStream stream, ReadApi api)
    {
        var buffer = new byte[64];
        switch (api)
        {
            case ReadApi.SyncRead:
                while (stream.Read(buffer, 0, buffer.Length) > 0) { }
                break;
            case ReadApi.ReadByte:
                while (stream.ReadByte() >= 0) { }
                break;
            case ReadApi.AsyncReadArray:
                while (await stream.ReadAsync(buffer, 0, buffer.Length) > 0) { }
                break;
            default: // AsyncReadMemory
                while (await stream.ReadAsync(buffer.AsMemory()) > 0) { }
                break;
        }
    }

    // In throwAtEndOfStream mode the wrapper must surface the in-band exception no matter which read
    // API drains it, and whether the body ends cleanly (a 0-byte read) or the connection drops
    // mid-stream (an IOException — how a live truncated HTTP response actually terminates).
    [TestCase(ReadApi.SyncRead, false)]
    [TestCase(ReadApi.SyncRead, true)]
    [TestCase(ReadApi.ReadByte, false)]
    [TestCase(ReadApi.ReadByte, true)]
    [TestCase(ReadApi.AsyncReadArray, false)]
    [TestCase(ReadApi.AsyncReadArray, true)]
    [TestCase(ReadApi.AsyncReadMemory, false)]
    [TestCase(ReadApi.AsyncReadMemory, true)]
    public void ThrowAtEndOfStream_SurfacesServerException_AcrossReadApisAndTerminations(ReadApi api, bool prematureClose)
    {
        var message = "Code: 395. DB::Exception: boom";
        var data = Encoding.UTF8.GetBytes(
            $"data\r\n__exception__\r\n{TestToken}\r\n{message}\n{message.Length} {TestToken}\r\n__exception__\r\n");

        Stream inner = prematureClose ? new ThrowAtEndStream(data) : new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(inner, TestToken, throwAtEndOfStream: true);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => DrainAsync(stream, api));

        Assert.That(ex.Message, Does.Contain("boom"));
        Assert.That(ex.ErrorCode, Is.EqualTo(395));
    }

    [Test]
    public void ThrowAtEndOfStream_DetectsException_WhenBlockFollowsLargeData()
    {
        // The in-band block arrives after >4 KiB of row data (larger than the ring buffer) and the
        // stream is drained in small chunks; the block at the tail must still survive and be detected.
        var message = "Code: 395. DB::Exception: boom";
        var prefix = new string('x', 8192);
        var data = Encoding.UTF8.GetBytes(
            $"{prefix}\r\n__exception__\r\n{TestToken}\r\n{message}\n{message.Length} {TestToken}\r\n__exception__\r\n");

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken, throwAtEndOfStream: true);

        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            var buffer = new byte[64];
            while (stream.Read(buffer, 0, buffer.Length) > 0) { }
        });

        Assert.That(ex.Message, Does.Contain("boom"));
    }

    [Test]
    public void Read_WithThrowAtEndOfStream_DoesNotThrow_WhenNoMarkerPresent()
    {
        // Tag present but the query succeeded: no in-band block, so the full body must pass through cleanly.
        var data = Encoding.UTF8.GetBytes("clean,csv\r\ndata,rows\r\nno,error\r\n");

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken, throwAtEndOfStream: true);
        using var sink = new MemoryStream();

        Assert.DoesNotThrow(() => stream.CopyTo(sink));
        Assert.That(sink.ToArray(), Is.EqualTo(data));
    }

    [Test]
    public void Read_WithoutThrowAtEndOfStream_DoesNotThrow_EvenWithMarker()
    {
        // Default (passive-observer) mode used by the native reader path must be unchanged: reading never
        // throws by itself; the marker is only surfaced when the caller asks via TryExtractMidStreamException.
        var message = "Code: 395. DB::Exception: boom";
        var data = Encoding.UTF8.GetBytes(
            $"data\r\n__exception__\r\n{TestToken}\r\n{message}\n{message.Length} {TestToken}\r\n__exception__\r\n");

        using var ms = new MemoryStream(data);
        using var stream = new ExceptionTagAwareStream(ms, TestToken); // default: throwAtEndOfStream = false

        Assert.DoesNotThrow(() =>
        {
            var buffer = new byte[64];
            while (stream.Read(buffer, 0, buffer.Length) > 0) { }
        });

        Assert.That(stream.TryExtractMidStreamException(), Is.Not.Null);
    }

    /// <summary>Stream that yields its content, then throws an IOException at end-of-stream (like a dropped HTTP connection).</summary>
    private sealed class ThrowAtEndStream : Stream
    {
        private readonly MemoryStream inner;

        public ThrowAtEndStream(byte[] data) => inner = new MemoryStream(data);

        public override int Read(byte[] buffer, int offset, int count)
        {
            int n = inner.Read(buffer, offset, count);
            if (n == 0)
                throw new IOException("The response ended prematurely.");
            return n;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => inner.Position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
