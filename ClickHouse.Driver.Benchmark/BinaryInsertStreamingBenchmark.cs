using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.IO;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures the cost of a single-batch binary insert on the object[] and POCO paths.
/// Targets issue #508: today the whole (gzipped) batch is materialized into a rented
/// <see cref="RecyclableMemoryStream"/> before the POST starts. This benchmark reports both
/// the BenchmarkDotNet time/allocation columns and a custom "peak pooled bytes" metric that
/// samples the injected <see cref="RecyclableMemoryStreamManager"/> in-use size during the run.
///
/// Before streaming: peak pooled bytes ~= the compressed batch size (grows with batch size).
/// After streaming:  peak pooled bytes ~= 0 (the payload never routes through the pool).
///
/// Peak values are written to <c>streaming-peak-memory.txt</c> in the working directory so a
/// before/after comparison survives across the two benchmark runs.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class BinaryInsertStreamingBenchmark
{
    private const string TableName = "test.benchmark_streaming_insert";
    private static readonly string[] Columns = { "Id", "Name", "Value", "X", "Y" };

    // Absolute path so the BDN child process (which runs in a temp dir that is cleaned up)
    // writes somewhere the parent run can read afterwards. Override with CH_BENCH_PEAK_FILE.
    private static readonly string PeakFile =
        Environment.GetEnvironmentVariable("CH_BENCH_PEAK_FILE")
        ?? Path.Combine(Path.GetTempPath(), "streaming-peak-memory.txt");

    private ClickHouseClient client;
    private RecyclableMemoryStreamManager manager;
    private PoolPeakSampler sampler;

    // Single large batch (BatchSize == Count, degree 1) so peak pooled bytes reflects the whole
    // materialized payload rather than one of several concurrently-serialized batches.
    [Params(200_000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";

        // Mirror the defaults ClickHouseClient uses for its shared manager so pool behavior matches production.
        manager = new RecyclableMemoryStreamManager(new RecyclableMemoryStreamManager.Options
        {
            BlockSize = 256 * 1024,
            MaximumSmallPoolFreeBytes = 128 * 1024 * 1024,
            MaximumLargePoolFreeBytes = 512 * 1024 * 1024,
        });

        client = new ClickHouseClient(connectionString)
        {
            MemoryStreamManager = manager,
        };

        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test");
        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE IF NOT EXISTS {TableName} (Id Int64, Name String, Value Float64, X Int64, Y Int64) ENGINE Null");

        client.RegisterBinaryInsertType<SensorRow>();

        sampler = new PoolPeakSampler(manager);
        sampler.Start();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        sampler?.Stop();
        var peak = sampler?.PeakBytes ?? 0;
        var line = $"{GetType().Name}\tCount={Count}\tPeakPooledBytes={peak}";
        Console.WriteLine(line);
        try
        {
            File.AppendAllText(PeakFile, line + Environment.NewLine);
        }
        catch
        {
            // Best effort; the console line is the primary record.
        }

        client?.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task<long> ObjectArray()
    {
        var options = new InsertOptions { BatchSize = Count, MaxDegreeOfParallelism = 1 };
        return await client.InsertBinaryAsync(TableName, Columns, GenerateObjectRows(Count), options);
    }

    [Benchmark]
    public async Task<long> Poco()
    {
        var options = new InsertOptions { BatchSize = Count, MaxDegreeOfParallelism = 1 };
        return await client.InsertBinaryAsync(TableName, GeneratePocoRows(Count), options);
    }

    private static IEnumerable<object[]> GenerateObjectRows(int count)
    {
        for (int i = 0; i < count; i++)
            yield return new object[] { (long)i, MakeName(i), i * 0.5, (long)(i ^ 0x5a5a), (long)(i * 3) };
    }

    private static IEnumerable<SensorRow> GeneratePocoRows(int count)
    {
        for (int i = 0; i < count; i++)
            yield return new SensorRow { Id = i, Name = MakeName(i), Value = i * 0.5, X = i ^ 0x5a5a, Y = i * 3 };
    }

    // Deterministic pseudo-random-ish string so the payload does not compress to almost nothing
    // (a constant/monotonic string would make peak pooled bytes unrealistically tiny).
    private static string MakeName(int i)
    {
        uint h = unchecked((uint)i * 2654435761u);
        return $"row-{i}-{h & 0xffffff:x6}";
    }

    public class SensorRow
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
        public long X { get; set; }
        public long Y { get; set; }
    }

    /// <summary>
    /// Background sampler that records the peak in-use bytes across both pools of a
    /// <see cref="RecyclableMemoryStreamManager"/> while a benchmark case runs.
    /// </summary>
    private sealed class PoolPeakSampler
    {
        private readonly RecyclableMemoryStreamManager manager;
        private Thread thread;
        private volatile bool running;
        private long peakBytes;

        public PoolPeakSampler(RecyclableMemoryStreamManager manager) => this.manager = manager;

        public long PeakBytes => Interlocked.Read(ref peakBytes);

        public void Start()
        {
            running = true;
            thread = new Thread(Loop) { IsBackground = true, Name = "PoolPeakSampler" };
            thread.Start();
        }

        public void Stop()
        {
            running = false;
            thread?.Join();
        }

        private void Loop()
        {
            while (running)
            {
                var inUse = manager.SmallPoolInUseSize + manager.LargePoolInUseSize;
                if (inUse > Interlocked.Read(ref peakBytes))
                    Interlocked.Exchange(ref peakBytes, inUse);

                // Tight enough to catch a short-lived materialized buffer without burning a core.
                Thread.Sleep(0);
            }
        }
    }
}
