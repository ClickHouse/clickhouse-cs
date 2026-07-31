using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// The stream-taking overloads of <see cref="ClickHouseClient.InsertRawStreamAsync"/> and
/// <see cref="ClickHouseClient.PostStreamAsync(string, Stream, bool, CancellationToken, QueryOptions)"/>
/// read from a stream the caller owns, so they must never dispose it - on success or on failure.
/// </summary>
public class RawStreamOwnershipTests : AbstractConnectionTestFixture
{
    private const string TsvRows = "1\tAlice\n2\tBob\n";

    private static byte[] TsvBytes => Encoding.UTF8.GetBytes(TsvRows);

    /// <summary>Counts how many times the driver disposed a caller-owned stream.</summary>
    private class DisposeCountingStream : MemoryStream
    {
        public DisposeCountingStream(byte[] buffer)
            : base(buffer, writable: false)
        {
        }

        public int DisposeCount { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                DisposeCount++;
            base.Dispose(disposing);
        }
    }

    /// <summary>Forward-only variant, so the chunked (unknown Content-Length) request path is covered too.</summary>
    private sealed class NonSeekableDisposeCountingStream : DisposeCountingStream
    {
        public NonSeekableDisposeCountingStream(byte[] buffer)
            : base(buffer)
        {
        }

        public override bool CanSeek => false;

        public override long Length => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    }

    private static byte[] GZip(string payload)
    {
        using var compressed = new MemoryStream();
        using (var gzip = new GZipStream(compressed, CompressionLevel.Fastest, leaveOpen: true))
        {
            var bytes = Encoding.UTF8.GetBytes(payload);
            gzip.Write(bytes, 0, bytes.Length);
        }

        return compressed.ToArray();
    }

    private async Task<string> CreateTargetTableAsync(string prefix)
    {
        var tableName = CreateTableName(prefix);
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt64, name String) ENGINE Memory");
        return tableName;
    }

    [TestCase(true, TestName = "InsertRawStreamAsync, compressed")]
    [TestCase(false, TestName = "InsertRawStreamAsync, uncompressed")]
    public async Task InsertRawStreamAsync_WithCallerOwnedStream_ShouldNotDisposeStream(bool useCompression)
    {
        var tableName = await CreateTargetTableAsync($"ownership_insert_{useCompression}");

        using var stream = new DisposeCountingStream(TsvBytes);
        using var response = await client.InsertRawStreamAsync(
            table: tableName,
            stream: stream,
            format: "TSV",
            useCompression: useCompression);

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(stream.CanRead, Is.True);

        // Still usable: rewind and insert the same payload again.
        stream.Position = 0;
        using var secondResponse = await client.InsertRawStreamAsync(
            table: tableName,
            stream: stream,
            format: "TSV",
            useCompression: useCompression);

        Assert.That(secondResponse.IsSuccessStatusCode, Is.True);
        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}"), Is.EqualTo(4));
    }

    [TestCase(true, TestName = "InsertRawStreamAsync failing, compressed")]
    [TestCase(false, TestName = "InsertRawStreamAsync failing, uncompressed")]
    public void InsertRawStreamAsync_WhenServerRejectsInsert_ShouldNotDisposeStream(bool useCompression)
    {
        // Never created, so the server rejects the insert.
        var missingTable = CreateTableName($"ownership_missing_{useCompression}");

        using var stream = new DisposeCountingStream(TsvBytes);
        Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            using var response = await client.InsertRawStreamAsync(
                table: missingTable,
                stream: stream,
                format: "TSV",
                useCompression: useCompression);
        });

        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(stream.CanRead, Is.True);
    }

    [TestCase(true, TestName = "InsertRawStreamAsync cancelled, compressed")]
    [TestCase(false, TestName = "InsertRawStreamAsync cancelled, uncompressed")]
    public void InsertRawStreamAsync_WhenCancelledBeforeSending_ShouldNotDisposeStream(bool useCompression)
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        using var stream = new DisposeCountingStream(TsvBytes);
        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            using var response = await client.InsertRawStreamAsync(
                table: "any_table",
                stream: stream,
                format: "TSV",
                useCompression: useCompression,
                cancellationToken: cts.Token);
        });

        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(stream.CanRead, Is.True);
    }

    [TestCase(true, TestName = "InsertRawStreamAsync non-seekable, compressed")]
    [TestCase(false, TestName = "InsertRawStreamAsync non-seekable, uncompressed")]
    public async Task InsertRawStreamAsync_WithNonSeekableCallerStream_ShouldInsertAndNotDisposeStream(bool useCompression)
    {
        var tableName = await CreateTargetTableAsync($"ownership_nonseekable_{useCompression}");

        using var stream = new NonSeekableDisposeCountingStream(TsvBytes);
        using var response = await client.InsertRawStreamAsync(
            table: tableName,
            stream: stream,
            format: "TSV",
            useCompression: useCompression);

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}"), Is.EqualTo(2));
    }

    [TestCase(true, TestName = "PostStreamAsync, gzip payload")]
    [TestCase(false, TestName = "PostStreamAsync, plain payload")]
    public async Task PostStreamAsync_WithCallerOwnedStream_ShouldNotDisposeStream(bool isCompressed)
    {
        var tableName = await CreateTargetTableAsync($"ownership_post_{isCompressed}");
        var payload = $"INSERT INTO {tableName} FORMAT TSV\n{TsvRows}";

        // isCompressed only adds the Content-Encoding header, so the payload has to be gzipped here.
        using var stream = new DisposeCountingStream(isCompressed ? GZip(payload) : Encoding.UTF8.GetBytes(payload));
        using var response = await client.PostStreamAsync(null, stream, isCompressed, CancellationToken.None);

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(stream.CanRead, Is.True);
        Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}"), Is.EqualTo(2));
    }

    [Test]
    public void PostStreamAsync_WhenServerRejectsQuery_ShouldNotDisposeStream()
    {
        var missingTable = CreateTableName("ownership_post_missing");

        using var stream = new DisposeCountingStream(
            Encoding.UTF8.GetBytes($"INSERT INTO {missingTable} FORMAT TSV\n{TsvRows}"));
        Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            using var response = await client.PostStreamAsync(null, stream, isCompressed: false, CancellationToken.None);
        });

        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(stream.CanRead, Is.True);
    }

    [Test]
    public async Task ConnectionPostStreamAsync_WithCallerOwnedStream_ShouldNotDisposeStream()
    {
        var tableName = await CreateTargetTableAsync("ownership_connection_post");

        using var stream = new DisposeCountingStream(
            Encoding.UTF8.GetBytes($"INSERT INTO {tableName} FORMAT TSV\n{TsvRows}"));
        using var response = await connection.PostStreamAsync(null, stream, isCompressed: false, CancellationToken.None);

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(stream.DisposeCount, Is.Zero);
        Assert.That(stream.CanRead, Is.True);
        Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}"), Is.EqualTo(2));
    }

    [Test]
    public void PostStreamAsync_WithNullStream_ShouldThrowArgumentNullException()
    {
        var exception = Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await client.PostStreamAsync("SELECT 1", data: null, isCompressed: false, CancellationToken.None));

        Assert.That(exception.ParamName, Is.EqualTo("data"));
    }
}
