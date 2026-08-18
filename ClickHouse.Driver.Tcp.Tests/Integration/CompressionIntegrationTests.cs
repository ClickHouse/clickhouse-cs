using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Compression against a real server, in both directions. The frame codec and its checksum are unit-tested
/// against the reference implementation, so these cover what only a server can settle: that the server accepts
/// the frames we write, that we decode the frames it writes, and that the framing survives the shapes the
/// codec tests cannot produce — bodies that span several frames, packets whose bodies are <b>not</b> framed
/// arriving mid-stream, and a pooled connection carrying one compressed query after another.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CompressionIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static IEnumerable<TestCaseData> Codecs()
    {
        yield return new TestCaseData(Lz4Compressor.Default).SetName("{m}(LZ4)");
        yield return new TestCaseData(ZstdCompressor.Default).SetName("{m}(ZSTD)");
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task QueryAsync_Compressed_ReturnsEveryRow(IClickHouseCompressor codec)
    {
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });

        var values = new List<ulong>();
        await foreach (object[] row in client.QueryAsync("SELECT number FROM numbers(5000)", null, None))
        {
            values.Add((ulong)row[0]);
        }

        Assert.Multiple(() =>
        {
            Assert.That(values, Has.Count.EqualTo(5000));
            Assert.That(values[0], Is.Zero);
            Assert.That(values[^1], Is.EqualTo(4999UL));
        });
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task InsertAsync_Compressed_RoundTripsThroughSelect(IClickHouseCompressor codec)
    {
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String) ENGINE = Memory", null, None);

            object[][] rows = Enumerable.Range(0, 2000)
                .Select(i => new object[] { (ulong)i, $"name-{i}" })
                .ToArray();
            await client.InsertAsync($"INSERT INTO {table} (id, name) VALUES", rows, null, None);

            var readBack = new List<(ulong Id, string Name)>();
            await foreach (object[] row in client.QueryAsync($"SELECT id, name FROM {table} ORDER BY id", null, None))
            {
                readBack.Add(((ulong)row[0], (string)row[1]));
            }

            Assert.Multiple(() =>
            {
                Assert.That(readBack, Has.Count.EqualTo(2000));
                Assert.That(readBack[0], Is.EqualTo((0UL, "name-0")));
                Assert.That(readBack[^1], Is.EqualTo((1999UL, "name-1999")));
            });
        }
        finally
        {
            await DropAsync(client, table);
        }
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task QueryAsync_ABodyLargerThanOneFrame_ReassemblesAcrossFrameBoundaries(IClickHouseCompressor codec)
    {
        // 400k UInt64 is ~3.2 MB of plaintext, so the server's ~1 MiB buffer emits several frames for one block
        // and values land across the boundaries. Summing every row fails if a single byte is dropped or repeated.
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });

        ulong count = 0;
        ulong sum = 0;
        await foreach (object[] row in client.QueryAsync("SELECT number FROM numbers(400000)", null, None))
        {
            count++;
            sum += (ulong)row[0];
        }

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(400000UL));
            Assert.That(sum, Is.EqualTo(400000UL * 399999UL / 2UL));
        });
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task InsertAsync_ABlockLargerThanOneFrame_RoundTripsThroughSelect(IClickHouseCompressor codec)
    {
        // The write side's mirror: one block whose plaintext exceeds the frame target, so the client emits
        // several frames for it and the server must accept every one.
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory", null, None);

            object[][] rows = Enumerable.Range(0, 200000).Select(i => new object[] { (ulong)i }).ToArray();
            await client.InsertAsync($"INSERT INTO {table} (id) VALUES", rows, null, None);

            var stored = new List<ulong>();
            await foreach (object[] row in client.QueryAsync($"SELECT count(), sum(id) FROM {table}", null, None))
            {
                stored.Add((ulong)row[0]);
                stored.Add((ulong)row[1]);
            }

            Assert.Multiple(() =>
            {
                Assert.That(stored[0], Is.EqualTo(200000UL), "row count");
                Assert.That(stored[1], Is.EqualTo(200000UL * 199999UL / 2UL), "sum of ids");
            });
        }
        finally
        {
            await DropAsync(client, table);
        }
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task QueryAsync_CompressedWithServerLogs_ReadsTheUnframedLogPacketsAndTheFramedData(IClickHouseCompressor codec)
    {
        // Log packets carry a block and are decoded by the same block reader, but the server does not compress
        // them at our protocol target. Framing them would read a block name as a frame checksum, so this is the
        // case that fails loudly if the framed-packet list ever grows to include them.
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["send_logs_level"] = "trace" },
        };

        var values = new List<ulong>();
        await foreach (object[] row in client.QueryAsync("SELECT number FROM numbers(1000)", options, None))
        {
            values.Add((ulong)row[0]);
        }

        Assert.That(values, Has.Count.EqualTo(1000));
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task QueryAsync_CompressedWithTotals_ReadsTheFramedTotalsBlock(IClickHouseCompressor codec)
    {
        // Totals is framed, unlike Log, so this covers the other side of the same predicate.
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });

        var groups = new List<ulong>();
        await foreach (object[] row in client.QueryAsync(
            "SELECT number % 3 AS bucket, count() FROM numbers(100) GROUP BY bucket WITH TOTALS ORDER BY bucket", null, None))
        {
            groups.Add((ulong)row[1]);
        }

        Assert.That(groups, Is.Not.Empty);
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task QueryAsync_SequentialCompressedQueries_ReuseThePooledConnection(IClickHouseCompressor codec)
    {
        // The frame reader and writer live for the connection's life and are reused across queries, so a second
        // compressed query on the same pooled connection proves no frame state leaked out of the first.
        await using var client = new ClickHouseTcpClient(
            TcpServerFixture.Options() with { Compressor = codec, MaxPoolSize = 1 });

        for (int attempt = 0; attempt < 3; attempt++)
        {
            var values = new List<ulong>();
            await foreach (object[] row in client.QueryAsync($"SELECT number FROM numbers({100 + attempt})", null, None))
            {
                values.Add((ulong)row[0]);
            }

            Assert.That(values, Has.Count.EqualTo(100 + attempt), $"attempt {attempt}");
        }
    }

    [Test]
    public async Task QueryAsync_CompressedThenUncompressedClients_BothReadTheSameRows()
    {
        // The flag is per query on the wire, so a compressed and an uncompressed client must both work against
        // the same server without either leaving it in a state the other cannot use.
        await using var compressed = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = Lz4Compressor.Default });
        await using var plain = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = null });

        ulong compressedSum = await SumAsync(compressed);
        ulong plainSum = await SumAsync(plain);

        Assert.That(compressedSum, Is.EqualTo(plainSum));

        static async ValueTask<ulong> SumAsync(ClickHouseTcpClient client)
        {
            ulong sum = 0;
            await foreach (object[] row in client.QueryAsync("SELECT number FROM numbers(2000)", null, None))
            {
                sum += (ulong)row[0];
            }

            return sum;
        }
    }

    [Test]
    public async Task QueryAsync_CompressedWithZstdAgainstAnLz4Request_DecodesWhicheverCodecArrives()
    {
        // The client's codec chooses what it writes, never what it reads: the server picks its own from
        // network_compression_method. Asking for ZSTD while writing LZ4 exercises the reader's per-frame
        // dispatch rather than the configured codec.
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = Lz4Compressor.Default });
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["network_compression_method"] = "ZSTD" },
        };

        var values = new List<ulong>();
        await foreach (object[] row in client.QueryAsync("SELECT number FROM numbers(3000)", options, None))
        {
            values.Add((ulong)row[0]);
        }

        Assert.That(values, Has.Count.EqualTo(3000));
    }

    private static async ValueTask DropAsync(ClickHouseTcpClient client, string table)
    {
        try
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", null, None);
        }
        catch (Exception)
        {
            // The test's own assertions own the verdict; a failed cleanup must not mask them.
        }
    }

    private static string UniqueTableName() => $"tcp_compression_test_{Guid.NewGuid():N}";
}
