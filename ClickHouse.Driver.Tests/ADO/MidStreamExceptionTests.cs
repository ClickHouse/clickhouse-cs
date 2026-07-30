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
/// Tests that the raw / custom-FORMAT streaming surface (<see cref="ClickHouseRawResult"/>) surfaces an
/// in-band mid-stream server exception as a <see cref="ClickHouseServerException"/> across every accessor,
/// using mock HTTP responses (no ClickHouse server required).
/// </summary>
public class ClickHouseRawResultMidStreamMockTests
{
    private const string Token = "PU1FNUFH98";
    private const string ErrorMessage = "Code: 395. DB::Exception: boom";

    public enum Accessor
    {
        Stream,
        Bytes,
        String,
        CopyTo,
    }

    // Real server framing: <rows>\r\n__exception__\r\n<token>\r\n<message>\n<size> <token>\r\n__exception__\r\n
    private static byte[] MidStreamBody() =>
        Encoding.UTF8.GetBytes($"1,0\r\n2,0\r\n__exception__\r\n{Token}\r\n{ErrorMessage}\n{ErrorMessage.Length} {Token}\r\n__exception__\r\n");

    private static HttpResponseMessage Response(byte[] content, string exceptionTag)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(content) };
        if (exceptionTag != null)
            response.Headers.Add("X-ClickHouse-Exception-Tag", exceptionTag);
        return response;
    }

    private static async Task<byte[]> DrainAsync(ClickHouseRawResult raw, Accessor accessor)
    {
        switch (accessor)
        {
            case Accessor.Bytes:
                return await raw.ReadAsByteArrayAsync();
            case Accessor.String:
                return Encoding.UTF8.GetBytes(await raw.ReadAsStringAsync());
            case Accessor.Stream:
            {
                using var stream = await raw.ReadAsStreamAsync();
                using var sink = new MemoryStream();
                await stream.CopyToAsync(sink);
                return sink.ToArray();
            }
            default: // CopyTo
            {
                using var sink = new MemoryStream();
                await raw.CopyToAsync(sink);
                return sink.ToArray();
            }
        }
    }

    [TestCase(Accessor.Stream)]
    [TestCase(Accessor.Bytes)]
    [TestCase(Accessor.String)]
    [TestCase(Accessor.CopyTo)]
    public void Accessor_WithExceptionTag_AndInBandException_ThrowsServerException(Accessor accessor)
    {
        using var response = Response(MidStreamBody(), Token);
        using var raw = new ClickHouseRawResult(response);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => DrainAsync(raw, accessor));

        Assert.That(ex.Message, Does.Contain("boom"));
        Assert.That(ex.ErrorCode, Is.EqualTo(395));
    }

    [TestCase(Accessor.Stream)]
    [TestCase(Accessor.Bytes)]
    [TestCase(Accessor.String)]
    [TestCase(Accessor.CopyTo)]
    public async Task Accessor_WithExceptionTag_ButSuccessfulBody_ReturnsFullBody(Accessor accessor)
    {
        var body = Encoding.UTF8.GetBytes("1,0\r\n2,0\r\n3,0\r\n"); // tag present but query succeeded (no in-band block)
        using var response = Response(body, Token);
        using var raw = new ClickHouseRawResult(response);

        var read = await DrainAsync(raw, accessor);

        Assert.That(read, Is.EqualTo(body));
    }

    [Test]
    public async Task ReadAsByteArrayAsync_WithoutExceptionTag_ReturnsBodyVerbatim()
    {
        // No tag header => the untagged path must be byte-for-byte unchanged (no wrapping, no detection),
        // even if the body coincidentally contains "__exception__" bytes.
        var body = MidStreamBody();
        using var response = Response(body, exceptionTag: null);
        using var raw = new ClickHouseRawResult(response);

        var bytes = await raw.ReadAsByteArrayAsync();

        Assert.That(bytes, Is.EqualTo(body));
    }
}

/// <summary>
/// Integration test for <see cref="ClickHouseRawResult"/> mid-stream exception detection against a real server.
/// </summary>
public class ClickHouseRawResultMidStreamTests : AbstractConnectionTestFixture
{
    [Test]
    [FromVersion(25, 11)]
    public async Task ExecuteRawResultAsync_MidStreamException_SurfacesServerException()
    {
        // The query streams a committed 200 OK plus rows before throwIf fires, so the failure is delivered
        // in-band (X-ClickHouse-Exception-Tag) and the truncated body surfaces as an HttpIOException that the
        // raw path used to leak instead of the real server error. Compression is disabled and buffering
        // minimized so the server streams incrementally (a buffered response instead fails pre-commit as 500).
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
            LIMIT 400000
            FORMAT CSV";

        using var result = await command.ExecuteRawResultAsync(default);
        using var stream = await result.ReadAsStreamAsync();

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            var buffer = new byte[64 * 1024];
            while (await stream.ReadAsync(buffer, 0, buffer.Length) > 0)
            {
                // Drain until the in-band mid-stream exception surfaces
            }
        });

        Assert.That(ex.Message, Does.Contain("boom mid stream"));
    }
}
