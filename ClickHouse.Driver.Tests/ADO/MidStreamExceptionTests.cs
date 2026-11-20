using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Poco;
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

        // The bytes of the exception block must not be returned as rows before the error surfaces.
        var rows = 0;
        var firstUnexpectedRow = -1;
        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                if (firstUnexpectedRow < 0 && reader.GetInt32(0) != rows)
                    firstUnexpectedRow = rows;
                rows++;
            }
        });

        Assert.That(ex.Message, Does.Contain("boom mid stream"));
        Assert.That(rows, Is.GreaterThan(0));
        Assert.That(firstUnexpectedRow, Is.EqualTo(-1), $"row {firstUnexpectedRow} of {rows} is not a row the server sent");
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

        var rows = 0;
        var firstUnexpectedRow = -1;
        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var row in streamingClient.QueryAsync<MidStreamPoco>(Sql, options: options))
            {
                if (firstUnexpectedRow < 0 && row.N != rows)
                    firstUnexpectedRow = rows;
                rows++;
            }
        });

        Assert.That(ex.Message, Does.Contain("boom mid stream"));
        Assert.That(rows, Is.GreaterThan(0));
        Assert.That(firstUnexpectedRow, Is.EqualTo(-1), $"row {firstUnexpectedRow} of {rows} is not a row the server sent");
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
    private static HttpResponseMessage CreateMockResponse(byte[] content, string exceptionTag = null, int maxReadSize = 0)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = maxReadSize > 0 ? new StreamContent(new ShortReadStream(content, maxReadSize)) : new ByteArrayContent(content)
        };

        if (exceptionTag != null)
        {
            response.Headers.Add("X-ClickHouse-Exception-Tag", exceptionTag);
        }

        return response;
    }

    /// <summary>
    /// Creates RowBinary header for a single column named "n" (Int32 unless <paramref name="type"/> is given).
    /// Format: column_count (7-bit), column_name (length-prefixed string), column_type (length-prefixed string)
    /// </summary>
    private static byte[] CreateRowBinaryHeader(string type = "Int32")
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8);

        // Column count: 1 (7-bit encoded)
        writer.Write((byte)1);

        // Column name: "n" (length-prefixed UTF-8 string)
        writer.Write((byte)1); // length
        writer.Write((byte)'n');

        // Column type (length-prefixed UTF-8 string)
        var typeBytes = Encoding.UTF8.GetBytes(type);
        writer.Write((byte)typeBytes.Length);
        writer.Write(typeBytes);

        return ms.ToArray();
    }

    // The opening the server writes (WriteBufferFromHTTPServerResponse), and the same block without the leading CRLF.
    private const string ServerOpening = "\r\n__exception__\r\n";
    private const string BareOpening = "__exception__\r\n";
    private const string Message = "Code: 395. DB::Exception: boom";

    // Longer than the 4 KiB ring buffer of ExceptionTagAwareStream, shorter than the server's 16 KiB block limit.
    private static readonly string LongMessage = "Code: 395. DB::Exception: " + new string('x', 6000);

    /// <summary>
    /// Returns at most <c>maxReadSize</c> bytes per read, as a chunked HTTP body does at a chunk boundary.
    /// </summary>
    private sealed class ShortReadStream : MemoryStream
    {
        private readonly int maxReadSize;

        public ShortReadStream(byte[] buffer, int maxReadSize)
            : base(buffer, writable: false) => this.maxReadSize = maxReadSize;

        public override int Read(byte[] buffer, int offset, int count) => base.Read(buffer, offset, Math.Min(count, maxReadSize));

        public override int Read(Span<byte> buffer) => base.Read(buffer.Slice(0, Math.Min(buffer.Length, maxReadSize)));
    }

    /// <summary>
    /// Creates a complete in-band exception block: "&lt;opening&gt;&lt;tag&gt;\r\n&lt;message&gt;\n&lt;length&gt; &lt;tag&gt;\r\n__exception__\r\n".
    /// Spaces are appended to the message until <paramref name="lead"/> plus the block length is a multiple of
    /// <paramref name="alignTo"/>, so that a body of fixed-width rows, <paramref name="lead"/> stray bytes and the
    /// block ends exactly at a row boundary.
    /// </summary>
    private static byte[] CreateExceptionBlock(string opening, string message, int alignTo = 1, int lead = 0)
    {
        while (true)
        {
            var length = Encoding.UTF8.GetByteCount(message) + 1;
            var block = Encoding.UTF8.GetBytes($"{opening}{TestToken}\r\n{message}\n{length} {TestToken}\r\n__exception__\r\n");
            if ((lead + block.Length) % alignTo == 0)
                return block;
            message += " ";
        }
    }

    private static byte[] Concat(params byte[][] parts)
    {
        using var ms = new MemoryStream();
        foreach (var part in parts)
            ms.Write(part, 0, part.Length);
        return ms.ToArray();
    }

    private static byte[] Sequence(int count)
    {
        var bytes = new byte[count];
        for (var i = 0; i < count; i++)
            bytes[i] = (byte)(i + 1);
        return bytes;
    }

    private static object[] Boxed(byte[] values)
    {
        var boxed = new object[values.Length];
        for (var i = 0; i < values.Length; i++)
            boxed[i] = values[i];
        return boxed;
    }

    private static IEnumerable<TestCaseData> ExceptionBlockAfterLastRowCases()
    {
        const int DefaultBuffer = 64 * 1024;

        // One byte per row: every byte of the block is also a valid row, so the body always ends at a row boundary.
        yield return new TestCaseData("UInt8", Sequence(3), Boxed(Sequence(3)), 1, Message, DefaultBuffer, 0).SetName("{m}(UInt8)");

        // Four bytes per row, block length a multiple of four: the body ends at a row boundary.
        yield return new TestCaseData("Int32", new byte[] { 1, 0, 0, 0, 2, 0, 0, 0 }, new object[] { 1, 2 }, 4, Message, DefaultBuffer, 0)
            .SetName("{m}(aligned Int32)");

        // 9-byte header + 52 rows: the first 64-byte refill of the read buffer ends 3 bytes into the opening marker.
        yield return new TestCaseData("UInt8", Sequence(52), Boxed(Sequence(52)), 1, Message, 64, 0)
            .SetName("{m}(marker split across read buffer refills)");

        // 9-byte header + 54 rows: the '\r' that opens the marker is the last byte of the first refill.
        yield return new TestCaseData("UInt8", Sequence(54), Boxed(Sequence(54)), 1, Message, 64, 0)
            .SetName("{m}(marker starts at the last buffered byte)");

        // One byte per read, as a chunked body can give: the marker arrives one byte at a time.
        yield return new TestCaseData("UInt8", Sequence(3), Boxed(Sequence(3)), 1, Message, DefaultBuffer, 1)
            .SetName("{m}(one byte per read)");

        // A read buffer smaller than the marker is enlarged for a tagged response.
        yield return new TestCaseData("UInt8", Sequence(3), Boxed(Sequence(3)), 1, Message, 1, 0)
            .SetName("{m}(read buffer size 1)");

        yield return new TestCaseData("UInt8", Sequence(3), Boxed(Sequence(3)), 1, LongMessage, DefaultBuffer, 0)
            .SetName("{m}(message longer than the ring buffer)");
    }

    [TestCaseSource(nameof(ExceptionBlockAfterLastRowCases))]
    public async Task Read_ExceptionBlockAfterLastRow_ReturnsOnlyTheRowsThenThrowsServerException(
        string type, byte[] rows, object[] expected, int rowWidth, string message, int readBufferSize, int maxReadSize)
    {
        var content = Concat(CreateRowBinaryHeader(type), rows, CreateExceptionBlock(ServerOpening, message, rowWidth));
        using var response = CreateMockResponse(content, TestToken, maxReadSize);
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default, pocoRegistry: null, readBufferSize);

        var values = new List<object>();
        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            while (reader.Read())
                values.Add(reader.GetValue(0));
        });

        Assert.That(values, Is.EqualTo(expected));
        Assert.That(ex.ErrorCode, Is.EqualTo(395));
        Assert.That(ex.Message, Is.EqualTo(message));
    }

    public class ByteRow
    {
        [ClickHouseColumn(Name = "n")]
        public byte N { get; set; }
    }

    [Test]
    public async Task TryMaterializeNextRow_ExceptionBlockAfterLastRow_ReturnsOnlyTheRowsThenThrowsServerException()
    {
        var content = Concat(CreateRowBinaryHeader("UInt8"), Sequence(3), CreateExceptionBlock(ServerOpening, Message));
        using var response = CreateMockResponse(content, TestToken);
        var registry = new PocoTypeRegistry();
        registry.RegisterForBoth<ByteRow>();
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default, registry);
        Assert.That(reader.TryGetRowMaterializer<ByteRow>(out var materializers, out var constructor), Is.True);

        var values = new List<byte>();
        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            while (reader.TryMaterializeNextRow(materializers, constructor, out var row))
                values.Add(row.N);
        });

        Assert.That(values, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(ex.ErrorCode, Is.EqualTo(395));
        Assert.That(ex.Message, Is.EqualTo(Message));
    }

    private static IEnumerable<TestCaseData> ExceptionBlockNotFoundAtRowBoundaryCases()
    {
        // The block starts 1 byte into a 4-byte row, so no row starts with its opening marker. Its length makes the
        // body end at a row boundary.
        yield return new TestCaseData(
                "Int32",
                Concat(CreateInt32Rows(1, 2), new byte[] { 0 }, CreateExceptionBlock(ServerOpening, Message, alignTo: 4, lead: 1)),
                new object[] { 1, 2 },
                395,
                Message)
            .SetName("{m}(block inside a row)");

        // The block has no leading CRLF, so the row that starts with it is not stopped at.
        yield return new TestCaseData(
                "UInt8",
                Concat(Sequence(3), CreateExceptionBlock(BareOpening, Message)),
                Boxed(Sequence(3)),
                395,
                Message)
            .SetName("{m}(block without the leading CRLF)");

        // The message pushes the opening marker out of the ring buffer; the closing marker still carries the tag.
        yield return new TestCaseData(
                "Int32",
                Concat(CreateInt32Rows(1, 2), new byte[] { 0 }, CreateExceptionBlock(ServerOpening, LongMessage, alignTo: 4, lead: 1)),
                new object[] { 1, 2 },
                -1,
                "Unknown error (could not parse exception message)")
            .SetName("{m}(block inside a row, message longer than the ring buffer)");
    }

    [TestCaseSource(nameof(ExceptionBlockNotFoundAtRowBoundaryCases))]
    public async Task Read_ExceptionBlockNotAtRowBoundaryAndBodyEndsCleanly_ThrowsServerException(
        string type, byte[] body, object[] expectedFirstRows, int expectedCode, string expectedMessage)
    {
        // The rows decoded out of the block are returned, but the clean end of the body must not be reported as the
        // end of a complete result.
        using var response = CreateMockResponse(Concat(CreateRowBinaryHeader(type), body), TestToken);
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default);

        var values = new List<object>();
        var ex = Assert.Throws<ClickHouseServerException>(() =>
        {
            while (reader.Read())
                values.Add(reader.GetValue(0));
        });

        Assert.That(values.GetRange(0, expectedFirstRows.Length), Is.EqualTo(expectedFirstRows));
        Assert.That(ex.ErrorCode, Is.EqualTo(expectedCode));
        Assert.That(ex.Message, Is.EqualTo(expectedMessage));
    }

    [Test]
    public async Task Read_RowsThatLookLikeTheMarkerWithoutTheTag_AreReturnedAsData()
    {
        // A 13-character string is written as 0x0D ('\r') + its characters. The first row matches the opening marker
        // up to "\r\n__exception_", and the body ends with "__exception__", but the tag never follows.
        var content = Concat(
            CreateRowBinaryHeader("String"),
            new byte[] { 13 }, Encoding.UTF8.GetBytes("\n__exception_"),
            new byte[] { 13 }, Encoding.UTF8.GetBytes("__exception__"));
        using var response = CreateMockResponse(content, TestToken);
        using var reader = await ClickHouseDataReader.FromHttpResponseAsync(response, TypeSettings.Default);

        var values = new List<string>();
        while (reader.Read())
            values.Add(reader.GetString(0));

        Assert.That(values, Is.EqualTo(new[] { "\n__exception_", "__exception__" }));
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
