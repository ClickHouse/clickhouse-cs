using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Tests.Attributes;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

/// <summary>
/// Pins that a value written by copying a stream into the output does not flush the write stack.
/// </summary>
/// <remarks>
/// <see cref="BinaryWriter.BaseStream"/> flushes as a side effect of its getter, so a type that
/// copied a value through it flushed the whole stack - compressor and transport included - once per
/// value. For ZSTD, GZip and Brotli that closes and encodes a compression block per value, which
/// makes the insert scale with the value count instead of the byte count and gives up the
/// compression ratio. Writes reaching the transport are counted because they are what the flush
/// produces; the count has to track the payload size, not the number of rows.
/// </remarks>
[TestFixture]
public class InsertBinaryStreamCopyFlushTests : AbstractConnectionTestFixture
{
    public enum StreamValueShape
    {
        JsonPoco,
        StringSeekableStream,
        StringNonSeekableStream,
        FixedStringSeekableStream,
    }

    private const int RowCount = 500;

    private static readonly byte[] Payload = Encoding.UTF8.GetBytes("0123456789abcdef");

    private static readonly string PayloadText = Encoding.UTF8.GetString(Payload);

    // Never created: the recording handler answers every request without parsing it.
    private static readonly string FakeEndpointTable = TestUtilities.CreateTableName("insert_stream_copy_flush");

    private class JsonRow
    {
        public string Name { get; set; }
    }

    [TestCase(StreamValueShape.JsonPoco)]
    [TestCase(StreamValueShape.StringSeekableStream)]
    [TestCase(StreamValueShape.StringNonSeekableStream)]
    [TestCase(StreamValueShape.FixedStringSeekableStream)]
    public async Task InsertBinaryAsync_CompressedStreamCopiedValues_DoesNotFlushPerRow(StreamValueShape shape)
    {
        var compressed = await RecordInsertAsync(shape, RowCount, ZstdCompressor.Default);
        var uncompressed = await RecordInsertAsync(shape, RowCount, compressor: null);

        Assert.Multiple(() =>
        {
            // The batch fits in one write buffer, so a few closing blocks reach the transport. A flush
            // per value made this RowCount + 1 - the count tracked the rows, not the payload.
            Assert.That(compressed.WriteCount, Is.LessThan(RowCount / 10));

            // Every row carries the same payload, so a batch compressed as one stream is far smaller
            // than the raw bytes. A block per value gave most of that back.
            Assert.That(compressed.Bytes, Is.LessThan(uncompressed.Bytes / 2));
        });
    }

    [TestCase(StreamValueShape.JsonPoco)]
    [TestCase(StreamValueShape.StringSeekableStream)]
    [TestCase(StreamValueShape.StringNonSeekableStream)]
    [TestCase(StreamValueShape.FixedStringSeekableStream)]
    public async Task InsertBinaryAsync_UncompressedStreamCopiedValues_CoalescesIntoOneBlock(StreamValueShape shape)
    {
        // Without a compressor the flush hit the plain write buffer, so the same defect emitted one
        // transport write per value there too. The whole batch fits in the buffer, so the flush on
        // dispose is the only write that should reach the transport.
        var recorder = await RecordInsertAsync(shape, RowCount, compressor: null);

        Assert.That(recorder.WriteCount, Is.EqualTo(1));
    }

    [TestCase(StreamValueShape.JsonPoco)]
    [TestCase(StreamValueShape.StringSeekableStream)]
    [TestCase(StreamValueShape.StringNonSeekableStream)]
    [TestCase(StreamValueShape.FixedStringSeekableStream)]
    [RequiredFeature(Feature.Json)]
    public async Task InsertBinaryAsync_CompressedStreamCopiedValues_RoundTripsEveryRow(StreamValueShape shape)
    {
        // The bytes now leave the writer only as the stack unwinds, so this guards the real server
        // path against dropping, truncating or reordering a value.
        var table = CreateTableName($"stream_copy_{shape}");
        var columnType = ColumnTypeOf(shape);

        // JsonWriteMode.Binary is what takes a POCO through the copy path; the default String mode
        // writes it as a plain string and never reaches it.
        using var binaryJsonClient = TestUtilities.GetTestClickHouseClient(jsonWriteMode: JsonWriteMode.Binary);
        binaryJsonClient.RegisterJsonSerializationType<JsonRow>();
        await binaryJsonClient.ExecuteNonQueryAsync($"CREATE TABLE {table} (Value {columnType}) ENGINE = Memory");

        var options = new InsertOptions
        {
            Compressor = ZstdCompressor.Default,
            ColumnTypes = new Dictionary<string, string> { ["Value"] = columnType },
            BatchSize = 200_000,
            MaxDegreeOfParallelism = 1,
        };
        await binaryJsonClient.InsertBinaryAsync(table, new[] { "Value" }, Rows(shape, RowCount), options);

        var readColumn = shape == StreamValueShape.JsonPoco ? "Value.Name" : "Value";
        var intact = await binaryJsonClient.ExecuteScalarAsync(
            $"SELECT count() FROM {table} WHERE {readColumn} = '{PayloadText}'");
        var total = await binaryJsonClient.ExecuteScalarAsync($"SELECT count() FROM {table}");

        Assert.Multiple(() =>
        {
            Assert.That(Convert.ToInt32(total), Is.EqualTo(RowCount));
            Assert.That(Convert.ToInt32(intact), Is.EqualTo(RowCount));
        });
    }

    private static string ColumnTypeOf(StreamValueShape shape) => shape switch
    {
        StreamValueShape.JsonPoco => "JSON(Name String)",
        StreamValueShape.StringSeekableStream or StreamValueShape.StringNonSeekableStream => "String",
        StreamValueShape.FixedStringSeekableStream => $"FixedString({Payload.Length})",
        _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, null),
    };

    private static IEnumerable<object[]> Rows(StreamValueShape shape, int rows) =>
        Enumerable.Range(0, rows).Select(_ => new object[] { ValueOf(shape) });

    private static object ValueOf(StreamValueShape shape) => shape switch
    {
        StreamValueShape.JsonPoco => new JsonRow { Name = PayloadText },
        StreamValueShape.StringSeekableStream or StreamValueShape.FixedStringSeekableStream => new MemoryStream(Payload),
        StreamValueShape.StringNonSeekableStream => new NonSeekableStream(Payload),
        _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, null),
    };

    private static async Task<RecordingStream> RecordInsertAsync(StreamValueShape shape, int rows, IClickHouseCompressor compressor)
    {
        var recorder = new RecordingStream();
        using var httpClient = new HttpClient(new RecordingHandler(recorder));
        using var recordingClient = new ClickHouseClient(new ClickHouseClientSettings
        {
            HttpClient = httpClient,
            JsonWriteMode = JsonWriteMode.Binary,
        });
        recordingClient.RegisterJsonSerializationType<JsonRow>();

        // Batching and parallelism are pinned: a split batch would make the write count depend on the
        // default batch size, and a second batch thread would race the recorder.
        var options = new InsertOptions
        {
            Compressor = compressor,
            ColumnTypes = new Dictionary<string, string> { ["Value"] = ColumnTypeOf(shape) },
            BatchSize = 200_000,
            MaxDegreeOfParallelism = 1,
        };

        await recordingClient.InsertBinaryAsync(FakeEndpointTable, new[] { "Value" }, Rows(shape, rows), options);

        return recorder;
    }

    /// <summary>A stream whose length is unknown, forcing the buffered copy path.</summary>
    private sealed class NonSeekableStream : MemoryStream
    {
        public NonSeekableStream(byte[] buffer)
            : base(buffer, writable: false) { }

        public override bool CanSeek => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        private readonly Stream destination;

        public RecordingHandler(Stream destination) => this.destination = destination;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // CopyToAsync hands the content the destination stream directly, so the serializer's writes
            // land on it one for one - exactly as they would on the request stream.
            await request.Content.CopyToAsync(destination, cancellationToken).ConfigureAwait(false);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };
        }
    }

    private sealed class RecordingStream : Stream
    {
        public int WriteCount { get; private set; }

        public long Bytes { get; private set; }

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => Bytes;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(new ReadOnlySpan<byte>(buffer, offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            WriteCount++;
            Bytes += buffer.Length;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Write(new ReadOnlySpan<byte>(buffer, offset, count));
            return Task.CompletedTask;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            return default;
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
