using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

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
[Category("Cloud")]
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
    public async Task InsertRowsAsync_Compressed_RoundTripsThroughSelect(IClickHouseCompressor codec)
    {
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String) ENGINE = Memory", null, None);

            object[][] rows = Enumerable.Range(0, 2000)
                .Select(i => new object[] { (ulong)i, $"name-{i}" })
                .ToArray();
            await client.InsertRowsAsync($"INSERT INTO {table} (id, name) VALUES", rows, null, None);

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
    public async Task InsertRowsAsync_ABlockLargerThanOneFrame_RoundTripsThroughSelect(IClickHouseCompressor codec)
    {
        // The write side's mirror: one block whose plaintext exceeds the frame target, so the client emits
        // several frames for it and the server must accept every one.
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { Compressor = codec });
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory", null, None);

            object[][] rows = Enumerable.Range(0, 200000).Select(i => new object[] { (ulong)i }).ToArray();
            await client.InsertRowsAsync($"INSERT INTO {table} (id) VALUES", rows, null, None);

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

    [TestCaseSource(nameof(Codecs))]
    public async Task MixedCompressedWorkload_ManySizesBackToBack_RunsEntirelyOnOneConnection(IClickHouseCompressor codec)
    {
        // The other tests each prove one operation is correct. This one proves the state an operation leaves
        // behind is correct: DDL, inserts and selects, from empty to several megabytes, run back to back over a
        // pool of one, so each starts from whatever the last left in the frame reader, the frame writer and
        // their buffers.
        //
        // The dial count is the assertion that carries the test. A connection the pool judges unusable is
        // discarded and the next operation opens a fresh one, so leftover plaintext or a desynchronized buffer
        // still produces correct rows and a passing test; only the count shows it happened.
        var options = TcpServerFixture.Options() with { Compressor = codec, MaxPoolSize = 1 };
        var factory = new DialCountingFactory(options);

        // A clock the test never advances: the pool's sweep timer is inert under it, so no idle or lifetime
        // reaping can retire the connection and inflate the dial count on a slow run.
        var pool = new ConnectionPool(options, factory, new ControlledTimeProvider());
        await using var client = new ClickHouseTcpClient(pool, options);

        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, payload String) ENGINE = Memory", null, None);

            // Sizes chosen to land either side of the frame layer's boundaries: no rows at all, a single value,
            // a body near the 16 KiB plaintext buffer, the 50,000-row block split, and a body the server cuts
            // into several frames. Payload widths vary per row, so values straddle the boundaries rather than
            // lining up with them.
            foreach (int size in new[] { 0, 1, 7, 2048, 2049, 60_000, 150_000 })
            {
                await client.ExecuteAsync($"TRUNCATE TABLE {table}", null, None);

                object[][] rows = Enumerable.Range(0, size)
                    .Select(i => new object[] { (ulong)i, Payload(i) })
                    .ToArray();
                await client.InsertRowsAsync($"INSERT INTO {table} (id, payload) VALUES", rows, null, None);

                ulong count = 0;
                ulong idSum = 0;
                int corrupt = 0;
                await foreach (object[] row in client.QueryAsync($"SELECT id, payload FROM {table} ORDER BY id", null, None))
                {
                    var id = (ulong)row[0];
                    if (!string.Equals((string)row[1], Payload((int)id), StringComparison.Ordinal))
                    {
                        corrupt++;
                    }

                    count++;
                    idSum += id;
                }

                Assert.Multiple(() =>
                {
                    Assert.That(count, Is.EqualTo((ulong)size), $"rows read back at size {size}");
                    Assert.That(idSum, Is.EqualTo(TriangleSum(size)), $"sum of ids at size {size}");
                    Assert.That(corrupt, Is.Zero, $"payloads that did not match their id at size {size}");
                });
            }

            // One value larger than a frame rather than many values across frames: this is the bulk read path,
            // which drains the buffered prefix and then reads the remainder straight out of the frames.
            string single = null;
            await foreach (object[] row in client.QueryAsync("SELECT repeat('ab', 1000000)", null, None))
            {
                single = (string)row[0];
            }

            Assert.Multiple(() =>
            {
                Assert.That(single, Has.Length.EqualTo(2_000_000), "length of the single large value");
                Assert.That(factory.Dials, Is.EqualTo(1), "connections opened for the whole workload");
                Assert.That(pool.IdleCount, Is.EqualTo(1), "connections idle once the workload finishes");
            });
        }
        finally
        {
            await DropAsync(client, table);
        }

        static string Payload(int i) => $"p-{i}-{new string('x', i % 37)}";

        static ulong TriangleSum(int n) => n == 0 ? 0UL : (ulong)n * (ulong)(n - 1) / 2UL;
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

    /// <summary>
    /// Opens real connections and counts them, so a test can assert the pool never replaced one. Reuse is
    /// otherwise invisible: the pool discards an unusable connection and dials again without telling the caller,
    /// and the operations still succeed.
    /// </summary>
    private sealed class DialCountingFactory : IConnectionFactory
    {
        private readonly TcpConnectionFactory inner;
        private int dials;

        public DialCountingFactory(ClickHouseTcpClientOptions options) => inner = new TcpConnectionFactory(options);

        /// <summary>How many connections the pool has opened through this factory.</summary>
        public int Dials => Volatile.Read(ref dials);

        public ValueTask<ClickHouseTcpConnection> CreateAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref dials);
            return inner.CreateAsync(cancellationToken);
        }

        public void Dispose() => inner.Dispose();
    }
}
