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
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

/// <summary>
/// Pins how binary inserts hand their payload to the transport.
/// </summary>
/// <remarks>
/// The request body of a binary insert has no known length, so it is sent with
/// <c>Transfer-Encoding: chunked</c> and every write that reaches the request stream becomes its own
/// HTTP chunk (framed with a hex length, CRLF, and a trailing CRLF). The row serializer issues one
/// write per field, so without a buffer between it and the request stream a one-column <c>Int64</c>
/// insert emits one 8-byte chunk per row and the framing alone inflates the request body by ~70%.
/// Compressed inserts never had the problem because every compressor already wraps itself in a
/// <see cref="PooledWriteBufferStream"/>.
/// <para>
/// Writes reaching the request stream are counted rather than chunks on the wire: the two are 1:1 for
/// a chunked body, and counting them keeps the test independent of the socket layer.
/// </para>
/// </remarks>
[TestFixture]
public class InsertBinaryRequestBufferingTests : AbstractConnectionTestFixture
{
    public enum InsertPath
    {
        ObjectArray,
        Poco,
    }

    private const int RowCount = 1000;

    private static readonly string[] Columns = { "Id" };

    // Supplying the schema skips the "SELECT ... WHERE 1=0" probe, so the recording handler below only
    // ever sees the insert itself.
    private static readonly IReadOnlyDictionary<string, string> ColumnTypes = new Dictionary<string, string>
    {
        ["Id"] = "UInt64",
    };

    // Never created: the recording handler answers every request without parsing it.
    private static readonly string FakeEndpointTable = TestUtilities.CreateTableName("insert_binary_buffering");

    // Below the ClickHouse DateTime lower bound, so serializing this row throws client-side.
    private static readonly DateTime OutOfRangeValue = new(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private class IdRow
    {
        public ulong Id { get; set; }
    }

    private class UnwritableRow
    {
        public ulong Id { get; set; }

        public DateTime Value { get; set; }
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public async Task InsertBinaryAsync_Uncompressed_CoalescesPerFieldWritesIntoOneBlock(InsertPath path)
    {
        var recorder = await RecordInsertAsync(path, RowCount, compressor: null);

        Assert.Multiple(() =>
        {
            // One write for the whole batch: it fits inside the buffer, so only the final flush reaches
            // the transport. Unbuffered this was RowCount + 1 (one per field, plus the query line).
            Assert.That(recorder.WriteCount, Is.EqualTo(1));
            Assert.That(recorder.Bytes, Is.EqualTo(ExpectedPayload(RowCount)));
        });
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public async Task InsertBinaryAsync_UncompressedBatchLargerThanBuffer_WritesEveryByteInFullBlocks(InsertPath path)
    {
        // Two buffer-fulls' worth of rows, so the buffer wraps and the trailing partial block is only
        // flushed on dispose - the case a missing or mis-ordered flush would truncate.
        const int Rows = 3 * 256 * 1024 / sizeof(ulong);

        var recorder = await RecordInsertAsync(path, Rows, compressor: null);

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Bytes, Is.EqualTo(ExpectedPayload(Rows)));
            // Every write but the last carries a full buffer, so the count tracks the payload size
            // rather than the row count.
            Assert.That(recorder.WriteCount, Is.LessThanOrEqualTo(recorder.Bytes.Length / (256 * 1024) + 1));
        });
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public async Task InsertBinaryAsync_Compressed_KeepsCoalescingThroughTheCompressor(InsertPath path)
    {
        // Contrast case: the compressed path already buffered inside the compressor and must be left
        // exactly as it was - the same handful of writes it made before, and still a compressed (not
        // RowBinary) body.
        var recorder = await RecordInsertAsync(path, RowCount, Lz4Compressor.Default);

        Assert.Multiple(() =>
        {
            // 4 is what this payload produced before the uncompressed path was buffered.
            Assert.That(recorder.WriteCount, Is.EqualTo(4));
            Assert.That(recorder.Bytes, Is.Not.EqualTo(ExpectedPayload(RowCount)));
            Assert.That(recorder.Bytes.Length, Is.LessThan(ExpectedPayload(RowCount).Length));
        });
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public async Task InsertBinaryAsync_Uncompressed_RoundTripsEveryRow(InsertPath path)
    {
        // The buffer is only flushed as the serializer unwinds, so this guards the real server path
        // against losing the tail of a batch.
        var table = CreateTableName($"roundtrip_{path}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (Id UInt64) ENGINE = Memory");
        client.RegisterBinaryInsertType<IdRow>();

        await InsertAsync(client, path, table, RowCount, new InsertOptions { Compressor = null, ColumnTypes = ColumnTypes });

        var sum = await client.ExecuteScalarAsync($"SELECT sum(Id) FROM {table}");
        Assert.That(Convert.ToUInt64(sum), Is.EqualTo((ulong)RowCount * (RowCount - 1) / 2));
    }

    [TestCase(InsertPath.ObjectArray)]
    [TestCase(InsertPath.Poco)]
    public void InsertBinaryAsync_WhenSerializationFailsAndTheFlushAlsoFails_KeepsTheSerializationError(InsertPath path)
    {
        // Buffering moves the batch's first transport write to the flush that happens as the serializer
        // unwinds. That write can fail in its own right, and if it did the transport error would replace
        // the serialization error - taking the failing row with it.
        var table = CreateTableName($"flushfails_{path}");
        var rows = Enumerable
            .Range(0, 8)
            .Select(i => new UnwritableRow { Id = (ulong)i, Value = OutOfRangeValue })
            .ToList();

        using var httpClient = new HttpClient(new RecordingHandler(new AlwaysFailingStream()));
        using var faultingClient = new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });
        faultingClient.RegisterBinaryInsertType<UnwritableRow>();

        var options = new InsertOptions
        {
            Compressor = null,
            ColumnTypes = new Dictionary<string, string> { ["Id"] = "UInt64", ["Value"] = "DateTime" },
        };

        var ex = Assert.CatchAsync<ClickHouseBulkCopySerializationException>(() => path switch
        {
            InsertPath.ObjectArray => faultingClient.InsertBinaryAsync(
                table,
                new[] { "Id", "Value" },
                rows.Select(row => new object[] { row.Id, row.Value }),
                options),
            InsertPath.Poco => faultingClient.InsertBinaryAsync(table, rows, options),
            _ => throw new ArgumentOutOfRangeException(nameof(path), path, null),
        });

        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(ex.Row, Is.Not.Null);
            Assert.That(ex.Row[1], Is.EqualTo(OutOfRangeValue));
        });
    }

    private static byte[] ExpectedPayload(int rows)
    {
        using var expected = new MemoryStream();
        expected.Write(Encoding.UTF8.GetBytes($"INSERT INTO {FakeEndpointTable} (`Id`) FORMAT RowBinary\n"));
        for (ulong i = 0; i < (ulong)rows; i++)
            expected.Write(BitConverter.GetBytes(i));
        return expected.ToArray();
    }

    private static async Task<RecordingStream> RecordInsertAsync(InsertPath path, int rows, IClickHouseCompressor compressor)
    {
        var recorder = new RecordingStream();
        using var httpClient = new HttpClient(new RecordingHandler(recorder));
        using var recordingClient = new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });
        recordingClient.RegisterBinaryInsertType<IdRow>();

        // Batching and parallelism are pinned rather than defaulted: a split batch would make the byte
        // and write counts depend on the default batch size, and a second batch thread would race the
        // recorder.
        var options = new InsertOptions
        {
            Compressor = compressor,
            ColumnTypes = ColumnTypes,
            BatchSize = 200_000,
            MaxDegreeOfParallelism = 1,
        };

        await InsertAsync(recordingClient, path, FakeEndpointTable, rows, options);

        return recorder;
    }

    private static Task InsertAsync(ClickHouseClient target, InsertPath path, string table, int rows, InsertOptions options)
    {
        return path switch
        {
            InsertPath.ObjectArray => target.InsertBinaryAsync(
                table,
                Columns,
                Enumerable.Range(0, rows).Select(i => new object[] { (ulong)i }),
                options),
            InsertPath.Poco => target.InsertBinaryAsync(
                table,
                Enumerable.Range(0, rows).Select(i => new IdRow { Id = (ulong)i }),
                options),
            _ => throw new ArgumentOutOfRangeException(nameof(path), path, null),
        };
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

    /// <summary>A request stream that is already broken: every write to it fails.</summary>
    private sealed class AlwaysFailingStream : Stream
    {
        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => 0;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(new ReadOnlySpan<byte>(buffer, offset, count));

        public override void Write(ReadOnlySpan<byte> buffer) => throw new IOException("connection reset by peer");

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            throw new IOException("connection reset by peer");

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            throw new IOException("connection reset by peer");

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }

    private sealed class RecordingStream : Stream
    {
        private readonly MemoryStream received = new();

        public int WriteCount { get; private set; }

        public byte[] Bytes => received.ToArray();

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => received.Length;
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
            received.Write(buffer);
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
