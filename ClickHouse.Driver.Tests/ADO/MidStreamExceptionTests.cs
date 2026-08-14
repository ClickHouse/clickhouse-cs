using System.Collections.Generic;
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

    public class MidStreamPoco
    {
        public int N { get; set; }

        public byte E { get; set; }
    }

    [Test]
    [FromVersion(25, 11)]
    public void QueryAsync_MidStreamExceptionAfterResponseIsCommitted_ThrowsClickHouseServerException()
    {
        // Same scenario as ShouldDetectMidStreamException_AfterResponseIsCommitted, but driven through
        // QueryAsync<T>, which takes the box-free fast path (TryMaterializeNextRow) instead of Read().
        using var streamingClient = TestUtilities.GetTestClickHouseClient(compression: false);
        streamingClient.RegisterPocoType<MidStreamPoco>();

        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object>
            {
                ["http_write_exception_in_output_format"] = 1,
                ["max_block_size"] = 1000,
                ["http_response_buffer_size"] = 0,
                ["wait_end_of_query"] = 0,
            },
        };

        const string Sql = @"
            SELECT toInt32(number) AS N,
                   throwIf(number = 200000, 'boom mid stream') AS E
            FROM system.numbers
            LIMIT 400000";

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var row in streamingClient.QueryAsync<MidStreamPoco>(Sql, options: options))
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
