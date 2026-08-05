using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class MidStreamExceptionTests : AbstractConnectionTestFixture
{
    [Test]
    [FromVersion(25, 11)]
    public void ShouldDetectMidStreamException()
    {
        using var command = connection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1; // Enable the exception tag feature on the server

        command.CommandText = @"
            SELECT toInt32(number) AS n,
                   throwIf(number = 10, 'boom') AS e
            FROM system.numbers
            LIMIT 10000000";

        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                // Keep reading until we hit the exception
            }
        });

        Assert.That(ex.Message, Does.Contain("boom"));
    }

    [Test]
    [FromVersion(25, 11)]
    public void ShouldDetectMidStreamException_AfterResponseIsCommitted()
    {
        // ShouldDetectMidStreamException above throws so early that the server returns a plain
        // HTTP 500 before committing a response, so it never exercises the in-band exception path.
        // Here the server streams a committed 200 OK plus rows before the throwIf fires, so the
        // failure is delivered in-band (X-ClickHouse-Exception-Tag) and reading past the truncated
        // body raises an HttpIOException that ExceptionTagAwareStream must convert into the real
        // server error. Compression is disabled so the server streams the response incrementally
        // rather than buffering it (a buffered response instead fails pre-commit as a 500).
        using var streamingClient = TestUtilities.GetTestClickHouseClient(compression: false);
        using var streamingConnection = streamingClient.CreateConnection();
        using var command = streamingConnection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1;
        command.CustomSettings["max_block_size"] = 1000;
        command.CustomSettings["http_response_buffer_size"] = 0;
        command.CustomSettings["wait_end_of_query"] = 0;

        command.CommandText = @"
            SELECT toInt32(number) AS n,
                   throwIf(number = 200000, 'boom mid stream') AS e
            FROM system.numbers
            LIMIT 400000";

        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                // Drain until the in-band mid-stream exception surfaces
            }
        });

        Assert.That(ex.Message, Does.Contain("boom mid stream"));
    }
}

/// <summary>
/// Tests for mid-stream exception handling using mock HTTP responses.
/// These tests don't require a ClickHouse server.
/// </summary>
public class MidStreamExceptionMockTests
{
    private const string TestToken = "PU1FNUFH98";

    /// <summary>
    /// Creates a mock HTTP response with the given stream content and optional exception tag header.
    /// </summary>
    private static HttpResponseMessage CreateMockResponse(byte[] content, string exceptionTag = null)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(content)
        };

        if (exceptionTag != null)
        {
            response.Headers.Add("X-ClickHouse-Exception-Tag", exceptionTag);
        }

        return response;
    }

    /// <summary>
    /// Creates RowBinary header for a single Int32 column named "n".
    /// Format: column_count (7-bit), column_name (length-prefixed string), column_type (length-prefixed string)
    /// </summary>
    private static byte[] CreateRowBinaryHeader()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8);

        // Column count: 1 (7-bit encoded)
        writer.Write((byte)1);

        // Column name: "n" (length-prefixed UTF-8 string)
        writer.Write((byte)1); // length
        writer.Write((byte)'n');

        // Column type: "Int32" (length-prefixed UTF-8 string)
        var typeBytes = Encoding.UTF8.GetBytes("Int32");
        writer.Write((byte)typeBytes.Length);
        writer.Write(typeBytes);

        return ms.ToArray();
    }

    /// <summary>
    /// Creates RowBinary data for Int32 values.
    /// </summary>
    private static byte[] CreateInt32Rows(params int[] values)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        foreach (var value in values)
        {
            writer.Write(value);
        }

        return ms.ToArray();
    }

    [Test]
    public async Task WithExceptionTagHeader_AndMarkerInStream_ShouldThrowClickHouseServerException()
    {
        // Arrange: Valid header + some rows + exception marker
        // The marker bytes will be read as Int32 values until we hit an incomplete read
        var header = CreateRowBinaryHeader();
        var rows = CreateInt32Rows(1, 2, 3);
        var exceptionMarker = Encoding.UTF8.GetBytes($"__exception__{TestToken}\nCode: 395. boom\n14 {TestToken}__exception__");

        var content = new byte[header.Length + rows.Length + exceptionMarker.Length];
        header.CopyTo(content, 0);
        rows.CopyTo(content, header.Length);
        exceptionMarker.CopyTo(content, header.Length + rows.Length);

        using var response = CreateMockResponse(content, TestToken);

        // Act & Assert
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default);

        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            while (reader.Read())
            {
                // Keep reading until exception
            }
        });

        Assert.That(ex.Message, Does.Contain("boom"));
        Assert.That(ex.ErrorCode, Is.EqualTo(395));
    }

    [Test]
    public async Task WithExceptionTagHeader_ButNoMarkerInStream_ShouldThrowEndOfStreamException()
    {
        // Arrange: Header present but stream ends without marker (incomplete transmission)
        var header = CreateRowBinaryHeader();
        var rows = CreateInt32Rows(1, 2, 3);
        var truncatedData = new byte[] { 0x01, 0x02 }; // Incomplete Int32 (only 2 bytes)

        var content = new byte[header.Length + rows.Length + truncatedData.Length];
        header.CopyTo(content, 0);
        rows.CopyTo(content, header.Length);
        truncatedData.CopyTo(content, header.Length + rows.Length);

        using var response = CreateMockResponse(content, TestToken);

        // Act & Assert
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default);

        var ex = Assert.Throws<EndOfStreamException>(() =>
        {
            while (reader.Read())
            {
                // Keep reading until exception
            }
        });

        // Should be standard EndOfStreamException (no marker found in buffer)
        Assert.That(ex, Is.TypeOf<EndOfStreamException>());
    }

    [Test]
    public async Task WithoutExceptionTagHeader_AndTruncatedStream_ShouldThrowEndOfStreamException()
    {
        // Arrange: No header (fallback to old behavior), stream truncated
        var header = CreateRowBinaryHeader();
        var rows = CreateInt32Rows(1, 2, 3);
        var truncatedData = new byte[] { 0x01, 0x02 }; // Incomplete Int32

        var content = new byte[header.Length + rows.Length + truncatedData.Length];
        header.CopyTo(content, 0);
        rows.CopyTo(content, header.Length);
        truncatedData.CopyTo(content, header.Length + rows.Length);

        using var response = CreateMockResponse(content, exceptionTag: null); // No header!

        // Act & Assert
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default);

        var ex = Assert.Throws<EndOfStreamException>(() =>
        {
            while (reader.Read())
            {
                // Keep reading until exception
            }
        });

        Assert.That(ex, Is.TypeOf<EndOfStreamException>());
    }

    [Test]
    public async Task WithoutExceptionTagHeader_AndExceptionMarkerInStream_ShouldNotDetectMarker()
    {
        // Arrange: Exception marker in stream but no header - should NOT detect it (old behavior)
        var header = CreateRowBinaryHeader();
        var rows = CreateInt32Rows(1, 2, 3);
        var exceptionMarker = Encoding.UTF8.GetBytes($"__exception__{TestToken}\nCode: 395. boom\n14 {TestToken}__exception__");

        var content = new byte[header.Length + rows.Length + exceptionMarker.Length];
        header.CopyTo(content, 0);
        rows.CopyTo(content, header.Length);
        exceptionMarker.CopyTo(content, header.Length + rows.Length);

        using var response = CreateMockResponse(content, exceptionTag: null); // No header!

        // Act & Assert
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default);

        // Without the header, we don't look for the marker, so we get EndOfStreamException not ClickHouseServerException
        var ex = Assert.Throws<EndOfStreamException>(() =>
        {
            while (reader.Read())
            {
                // Keep reading until exception
            }
        });

        // Should be EndOfStreamException, not ClickHouseServerException (marker not detected without header)
        Assert.That(ex, Is.TypeOf<EndOfStreamException>());
    }
}

/// <summary>
/// Integration tests for <see cref="ClickHouseRawResult"/> mid-stream exception detection against a real
/// server, covering every accessor of the raw / custom-FORMAT streaming surface.
/// </summary>
public class ClickHouseRawResultMidStreamTests : AbstractConnectionTestFixture
{
    public enum Accessor
    {
        Stream,
        Bytes,
        String,
        CopyTo,
    }

    /// <summary>
    /// Drains a raw result through the given accessor and returns the bytes it produced. The buffered
    /// accessors materialize the whole body, so a mid-stream failure surfaces before they hand anything
    /// back; the streaming ones surface it while the caller is draining.
    /// </summary>
    private static async Task<byte[]> DrainAsync(ClickHouseRawResult result, Accessor accessor)
    {
        switch (accessor)
        {
            case Accessor.Bytes:
                return await result.ReadAsByteArrayAsync();
            case Accessor.String:
                return Encoding.UTF8.GetBytes(await result.ReadAsStringAsync());
            case Accessor.CopyTo:
            {
                using var sink = new MemoryStream();
                await result.CopyToAsync(sink);
                return sink.ToArray();
            }
            default: // Stream, drained through the array ReadAsync overload
            {
                using var stream = await result.ReadAsStreamAsync();
                using var sink = new MemoryStream();
                var buffer = new byte[64 * 1024];
                int read;
                while ((read = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                    sink.Write(buffer, 0, read);
                return sink.ToArray();
            }
        }
    }

    private static ClickHouseCommand CreateStreamingCommand(ClickHouseConnection streamingConnection)
    {
        // Compression is disabled by the caller and buffering minimized here so the server streams the
        // response incrementally; a buffered response instead fails pre-commit as a plain HTTP 500, which
        // never exercises the in-band path.
        var command = streamingConnection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1;
        command.CustomSettings["max_block_size"] = 1000;
        command.CustomSettings["http_response_buffer_size"] = 0;
        command.CustomSettings["wait_end_of_query"] = 0;
        return command;
    }

    [TestCase(Accessor.Stream)]
    [TestCase(Accessor.Bytes)]
    [TestCase(Accessor.String)]
    [TestCase(Accessor.CopyTo)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_MidStreamException_SurfacesServerException(Accessor accessor)
    {
        // The query streams a committed 200 OK plus rows before throwIf fires, so the failure is delivered
        // in-band (X-ClickHouse-Exception-Tag) and the truncated body surfaced as an HttpIOException that
        // the raw path used to leak instead of the real server error.
        using var streamingClient = TestUtilities.GetTestClickHouseClient(compression: false);
        using var streamingConnection = streamingClient.CreateConnection();
        using var command = CreateStreamingCommand(streamingConnection);

        command.CommandText = @"
            SELECT toInt32(number) AS n,
                   throwIf(number = 200000, 'boom mid stream') AS e
            FROM system.numbers
            LIMIT 400000
            FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => DrainAsync(result, accessor));

        Assert.That(ex.Message, Does.Contain("boom mid stream"));
        Assert.That(ex.ErrorCode, Is.EqualTo(395)); // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
    }

    [TestCase(Accessor.Stream)]
    [TestCase(Accessor.Bytes)]
    [TestCase(Accessor.String)]
    [TestCase(Accessor.CopyTo)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_SuccessfulQuery_ReturnsCompleteBody(Accessor accessor)
    {
        // Contrast case. The server sends X-ClickHouse-Exception-Tag on every response, so detection is
        // engaged for successful queries too; the body must still come back complete and unmodified.
        using var command = connection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1;
        command.CommandText = "SELECT number, number * 2 FROM system.numbers LIMIT 3 FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);

        var body = await DrainAsync(result, accessor);

        Assert.That(Encoding.UTF8.GetString(body), Is.EqualTo("0,0\n1,2\n2,4\n"));
    }

    [TestCase(Accessor.Bytes)]
    [TestCase(Accessor.String)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_BufferedAccessorReadTwice_ReturnsSameBody(Accessor accessor)
    {
        // The buffering accessors materialize the whole body, so — as when reading straight off
        // HttpContent — asking twice must hand back the same body rather than failing on a consumed stream.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT number, number * 2 FROM system.numbers LIMIT 3 FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);

        var first = await DrainAsync(result, accessor);
        var second = await DrainAsync(result, accessor);

        Assert.That(Encoding.UTF8.GetString(first), Is.EqualTo("0,0\n1,2\n2,4\n"));
        Assert.That(second, Is.EqualTo(first));
    }

    [TestCase(Accessor.Bytes, Accessor.Stream)]
    [TestCase(Accessor.Bytes, Accessor.CopyTo)]
    [TestCase(Accessor.String, Accessor.Stream)]
    [TestCase(Accessor.String, Accessor.CopyTo)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_StreamingAccessorAfterBufferedRead_ReturnsCompleteBody(Accessor buffering, Accessor streaming)
    {
        // The exception tag is present on every response, so a buffering accessor materializes the whole
        // body into an internal buffer. A subsequent streaming accessor must serve that buffer — matching
        // the untagged HttpContent path, which buffers once and re-serves it — rather than re-reading the
        // now-exhausted underlying content stream and handing back an empty body.
        using var command = connection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1;
        command.CommandText = "SELECT number, number * 2 FROM system.numbers LIMIT 3 FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);

        var buffered = await DrainAsync(result, buffering);
        var streamed = await DrainAsync(result, streaming);

        Assert.That(Encoding.UTF8.GetString(buffered), Is.EqualTo("0,0\n1,2\n2,4\n"));
        Assert.That(streamed, Is.EqualTo(buffered));
    }

    /// <summary>
    /// Consumes the underlying content stream through a streaming accessor without buffering it: Stream is
    /// partially drained and left open (a caller that stopped mid-read — the reported scenario); CopyTo is
    /// fully drained. Either way the content stream can no longer be re-materialized from the start.
    /// </summary>
    private static async Task ConsumeViaStreamingAccessorAsync(ClickHouseRawResult result, Accessor consumer)
    {
        if (consumer == Accessor.CopyTo)
        {
            using var sink = new MemoryStream();
            await result.CopyToAsync(sink);
            return;
        }

        var stream = await result.ReadAsStreamAsync();
        var probe = new byte[4];
        Assert.That(await stream.ReadAsync(probe, 0, probe.Length), Is.GreaterThan(0));
    }

    [TestCase(Accessor.Stream, Accessor.Bytes)]
    [TestCase(Accessor.Stream, Accessor.String)]
    [TestCase(Accessor.Stream, Accessor.CopyTo)]
    [TestCase(Accessor.CopyTo, Accessor.Bytes)]
    [TestCase(Accessor.CopyTo, Accessor.String)]
    [TestCase(Accessor.CopyTo, Accessor.CopyTo)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_ReMaterializingAccessorAfterStreamingConsumer_ThrowsLikeUntaggedContent(Accessor consumer, Accessor rematerializer)
    {
        // Once a streaming accessor (ReadAsStream or CopyTo) has consumed the underlying content stream it
        // cannot be re-read from the start. A read that has to re-materialize the whole body — ReadAsByteArray,
        // ReadAsString or CopyTo — must then fail with the same InvalidOperationException the untagged
        // HttpContent path raises, rather than silently caching/copying a truncated body the caller cannot
        // tell apart from a complete one.
        using var command = connection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1;
        command.CommandText = "SELECT number, number * 2 FROM system.numbers LIMIT 3 FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);

        await ConsumeViaStreamingAccessorAsync(result, consumer);

        Assert.ThrowsAsync<InvalidOperationException>(() => DrainAsync(result, rematerializer));
    }

    /// <summary>
    /// A mid-stream failure that survives compression. The server buffers a compressed body, so it only
    /// commits the 200 OK — and with it the in-band exception path — once enough output has accumulated to
    /// flush; a smaller result fails pre-commit as a plain error instead, which is a different code path.
    /// </summary>
    private const string CompressibleMidStreamQuery = @"
        SELECT toInt32(number) AS n,
               throwIf(number = 1000000, 'boom mid stream') AS e
        FROM system.numbers
        LIMIT 2000000
        FORMAT CSV";

    /// <summary>Drains a plaintext stream, discarding the bytes.</summary>
    private static async Task DrainToEndAsync(Stream stream)
    {
        var buffer = new byte[64 * 1024];
        while (await stream.ReadAsync(buffer, 0, buffer.Length) > 0)
        {
            // The point is reaching the end of the body, where a mid-stream failure surfaces.
        }
    }

    [TestCase("gzip")]
    [TestCase("lz4")]
    [TestCase(null)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_MidStreamException_DecompressedStreamSurfacesServerException(string codec)
    {
        // The server writes its in-band exception block into the ENCODED body, so the marker only exists in
        // the decoded plaintext: the scanner must therefore sit above the decoder. Asking for a codec is what
        // makes a raw body compressed at all (a raw request advertises none by default), and the null case
        // pins that the same member still detects the block when there is nothing to decode.
        using var streamingClient = TestUtilities.GetTestClickHouseClient(compression: false);
        using var streamingConnection = streamingClient.CreateConnection();
        using var command = CreateStreamingCommand(streamingConnection);
        command.AcceptEncoding = codec;

        command.CommandText = CompressibleMidStreamQuery;

        using var result = await command.ExecuteRawResultAsync(default);

        Assert.That(result.ContentEncoding, Is.EqualTo(codec), "the body must be encoded as the test asked");

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await DrainToEndAsync(await result.ReadDecompressedStreamAsync()));

        Assert.That(ex.Message, Does.Contain("boom mid stream"));
        Assert.That(ex.ErrorCode, Is.EqualTo(395)); // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
    }

    [TestCase(Accessor.Stream)]
    [TestCase(Accessor.Bytes)]
    [TestCase(Accessor.String)]
    [TestCase(Accessor.CopyTo)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_MidStreamException_OnCompressedBody_VerbatimAccessorsCannotDetectIt(Accessor accessor)
    {
        // Contrast case, pinning the documented boundary of the fix rather than extending it: the four
        // original members hand the bytes on the wire over verbatim, and a compressed body carries the
        // exception block compressed too, so no scan of it can match. The caller still gets an error — the
        // truncated transport — just not the server's own; they can find the block in what they decode, or
        // use ReadDecompressedStreamAsync above.
        using var streamingClient = TestUtilities.GetTestClickHouseClient(compression: false);
        using var streamingConnection = streamingClient.CreateConnection();
        using var command = CreateStreamingCommand(streamingConnection);
        command.AcceptEncoding = "gzip";

        command.CommandText = CompressibleMidStreamQuery;

        using var result = await command.ExecuteRawResultAsync(default);

        Assert.That(result.ContentEncoding, Is.EqualTo("gzip"));

        var ex = Assert.CatchAsync(() => DrainAsync(result, accessor));

        Assert.That(ex, Is.Not.InstanceOf<ClickHouseServerException>());
        Assert.That(ex, Is.InstanceOf<IOException>(), "the truncated transport is what surfaces");
    }

    [TestCase("gzip")]
    [TestCase("lz4")]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_SuccessfulCompressedQuery_DecompressedStreamReturnsCompleteBody(string codec)
    {
        // Contrast case. The tag header is sent on every response, so the scanner is engaged over the decoder
        // for successful queries too; the decoded body must still come back complete and unmodified.
        using var command = connection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1;
        command.AcceptEncoding = codec;
        command.CommandText = "SELECT number, number * 2 FROM system.numbers LIMIT 3 FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);

        Assert.That(result.ContentEncoding, Is.EqualTo(codec));

        using var sink = new MemoryStream();
        await (await result.ReadDecompressedStreamAsync()).CopyToAsync(sink);

        Assert.That(Encoding.UTF8.GetString(sink.ToArray()), Is.EqualTo("0,0\n1,2\n2,4\n"));
    }

    [TestCase(Accessor.Stream)]
    [TestCase(Accessor.CopyTo)]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_StreamAccessorAfterStreamingConsumer_ContinuesWithoutThrowing(Accessor consumer)
    {
        // The consumed-stream guard is deliberately scoped to the re-materializing accessors: re-requesting
        // the stream itself must NOT throw, matching untagged HttpContent whose second ReadAsStreamAsync hands
        // back the same, now-drained stream. Pins the fix as targeted rather than a blanket "any second
        // accessor throws".
        using var command = connection.CreateCommand();
        command.CustomSettings["http_write_exception_in_output_format"] = 1;
        command.CommandText = "SELECT number, number * 2 FROM system.numbers LIMIT 3 FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);

        await ConsumeViaStreamingAccessorAsync(result, consumer);

        Assert.DoesNotThrowAsync(async () =>
        {
            using var again = await result.ReadAsStreamAsync();
            var buffer = new byte[64];
            while (await again.ReadAsync(buffer, 0, buffer.Length) > 0)
            {
                // Drain whatever remains; the point is that re-reading the stream does not throw.
            }
        });
    }
}
