using System;
using System.IO.Compression;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Compression;
using K4os.Compression.LZ4;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures the cost of the <see cref="InsertOptions.Compressor"/> choice for binary inserts, so the
/// GZip-vs-uncompressed tradeoff can be quantified in each deployment.
///
/// The tradeoff is environment-dependent, which is why this must be run against both a local and a
/// cloud endpoint:
///  - <b>Local</b> (loopback / same host): bandwidth is effectively free, so GZip's CPU cost is pure
///    overhead — <c>None</c> is usually fastest.
///  - <b>Cloud</b> (WAN, TLS, metered/limited egress): the wire is the bottleneck, so shrinking the
///    payload with GZip typically wins despite the CPU cost, and <c>Optimal</c> may beat <c>Fastest</c>.
///
/// Point the run at each environment via the CLICKHOUSE_CONNECTION env var and compare:
///   CLICKHOUSE_CONNECTION="Host=localhost;User=default" dotnet run -c Release -- --filter *Compression*
///   CLICKHOUSE_CONNECTION="Host=&lt;cloud&gt;;...;Protocol=https" dotnet run -c Release -- --filter *Compression*
///
/// The table uses ENGINE Null so the server discards rows and the benchmark isolates client-side
/// serialization + compression + transport rather than storage.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class BinaryInsertCompressionBenchmark
{
    public enum InsertCompression
    {
        None,
        GzipFastest,
        GzipOptimal,
        BrotliFastest,
        BrotliOptimal,
#if LZ4_AVAILABLE
        Lz4Fastest,
        Lz4Max,
#endif
    }

    // Use the always-present `default` database rather than creating a custom one: on ClickHouse Cloud
    // a freshly created db can momentarily disappear from a replica (idle/scaling), which surfaces mid-run
    // as UNKNOWN_DATABASE and has nothing to do with the code under test.
    private const string TableName = "default.benchmark_compression_insert";
    private static readonly string[] Columns = { "Id", "Name", "Value", "Payload" };

    private ClickHouseClient client;
    private object[][] rows;

    [Params(500_000)]
    public int Count { get; set; }

    [Params(
        InsertCompression.None,
        InsertCompression.GzipFastest,
        InsertCompression.GzipOptimal,
        InsertCompression.BrotliFastest,
        InsertCompression.BrotliOptimal
#if LZ4_AVAILABLE
        ,
        InsertCompression.Lz4Fastest,
        InsertCompression.Lz4Max
#endif
        )]
    public InsertCompression Compression { get; set; }

    private static IClickHouseCompressor Map(InsertCompression compression) => compression switch
    {
        InsertCompression.None => null,
        InsertCompression.GzipFastest => GZipCompressor.Default,
        InsertCompression.GzipOptimal => new GZipCompressor(CompressionLevel.Optimal),
        InsertCompression.BrotliFastest => BrotliCompressor.Default,
        // NOTE: Brotli Optimal maps to a high quality level and is markedly slower than Fastest.
        InsertCompression.BrotliOptimal => new BrotliCompressor(CompressionLevel.Optimal),
#if LZ4_AVAILABLE
        InsertCompression.Lz4Fastest => Lz4Compressor.Default,
        // L12_MAX is the maximum level; far slower than fast mode for ~no extra ratio.
        InsertCompression.Lz4Max => new Lz4Compressor(LZ4Level.L12_MAX),
#endif
        _ => throw new ArgumentOutOfRangeException(nameof(compression)),
    };

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";

        client = new ClickHouseClient(connectionString);

        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE IF NOT EXISTS {TableName} (Id Int64, Name String, Value Float64, Payload String) ENGINE Null");

        // Pre-materialize rows once. The Payload column is semi-repetitive text so the data compresses
        // to a realistic (not pathological) ratio — neither incompressible noise nor all-zeros.
        rows = new object[Count][];
        for (int i = 0; i < Count; i++)
            rows[i] = new object[] { (long)i, MakeName(i), i * 0.5, MakePayload(i) };
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [Benchmark]
    public async Task<long> Insert()
    {
        var options = new InsertOptions
        {
            BatchSize = 100_000,
            MaxDegreeOfParallelism = 1,
            Compressor = Map(Compression),
        };
        return await client.InsertBinaryAsync(TableName, Columns, rows, options);
    }

    private static string MakeName(int i)
    {
        uint h = unchecked((uint)i * 2654435761u);
        return $"row-{i}-{h & 0xffffff:x6}";
    }

    // Mix of a fixed, highly compressible prefix and a varying suffix → representative ~2-4x ratio.
    private static string MakePayload(int i)
        => $"event=purchase;status=ok;region=us-east-1;user_id={i};session={((ulong)i * 6364136223846793005UL):x}";
}
