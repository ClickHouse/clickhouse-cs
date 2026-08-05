using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// The stream-taking write paths hand the supplied stream to a <see cref="System.Net.Http.StreamContent"/>,
/// which the request message disposes along with itself - so the stream is disposed exactly once, whether
/// the request succeeds or fails.
/// </summary>
public class RawStreamDisposalTests : AbstractConnectionTestFixture
{
    private const string TsvRows = "1\tAlice\n2\tBob\n";

    /// <summary>Counts how many times the driver disposed the supplied stream.</summary>
    private sealed class DisposeCountingStream : MemoryStream
    {
        public DisposeCountingStream(string payload)
            : base(Encoding.UTF8.GetBytes(payload), writable: false)
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

    [TestCase(true, TestName = "InsertRawStreamAsync, compressed")]
    [TestCase(false, TestName = "InsertRawStreamAsync, uncompressed")]
    public async Task InsertRawStreamAsync_ShouldDisposeSuppliedStreamExactlyOnce(bool useCompression)
    {
        var tableName = CreateTableName($"raw_stream_ok_{useCompression}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt64, name String) ENGINE Memory");

        var stream = new DisposeCountingStream(TsvRows);
        using var response = await client.InsertRawStreamAsync(tableName, stream, "TSV", useCompression: useCompression);

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(stream.DisposeCount, Is.EqualTo(1));
        Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}"), Is.EqualTo(2));
    }

    [TestCase(true, TestName = "InsertRawStreamAsync failing, compressed")]
    [TestCase(false, TestName = "InsertRawStreamAsync failing, uncompressed")]
    public void InsertRawStreamAsync_WhenServerRejectsInsert_ShouldDisposeSuppliedStreamExactlyOnce(bool useCompression)
    {
        // Never created, so the server rejects the insert.
        var missingTable = CreateTableName($"raw_stream_missing_{useCompression}");

        var stream = new DisposeCountingStream(TsvRows);
        Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            using var response = await client.InsertRawStreamAsync(missingTable, stream, "TSV", useCompression: useCompression);
        });

        Assert.That(stream.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task PostStreamAsync_ShouldDisposeSuppliedStreamExactlyOnce()
    {
        var tableName = CreateTableName("raw_stream_post");
        await connection.ExecuteStatementAsync($"CREATE TABLE {tableName} (id UInt64, name String) ENGINE Memory");

        var stream = new DisposeCountingStream($"INSERT INTO {tableName} FORMAT TSV\n{TsvRows}");
        using var response = await client.PostStreamAsync(null, stream, isCompressed: false, CancellationToken.None);

        Assert.That(response.IsSuccessStatusCode, Is.True);
        Assert.That(stream.DisposeCount, Is.EqualTo(1));
        Assert.That(await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}"), Is.EqualTo(2));
    }

    [Test]
    public void PostStreamAsync_WhenServerRejectsQuery_ShouldDisposeSuppliedStreamExactlyOnce()
    {
        var missingTable = CreateTableName("raw_stream_post_missing");

        var stream = new DisposeCountingStream($"INSERT INTO {missingTable} FORMAT TSV\n{TsvRows}");
        Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            using var response = await client.PostStreamAsync(null, stream, isCompressed: false, CancellationToken.None);
        });

        Assert.That(stream.DisposeCount, Is.EqualTo(1));
    }
}
