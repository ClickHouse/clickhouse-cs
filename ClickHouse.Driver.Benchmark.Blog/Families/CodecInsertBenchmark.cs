using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Compression;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
#if CH_API_1_4
// The whole ClickHouse.Driver.Compression namespace is unreleased, so even the using has to be gated:
// an unguarded one fails to compile against every published package, regardless of what the arms do.
using ClickHouse.Driver.Compression;
#endif

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// Codec matrix, <b>insert direction</b>: the client compresses, the server decompresses.
/// </summary>
/// <remarks>
/// <para>
/// The mirror of <see cref="CodecReadBenchmark"/>, and deliberately a separate class. Here the client
/// pays compression and the server pays <b>de</b>compression — a much flatter curve than compression,
/// so a codec that costs the server dearly on reads may be nearly free on inserts. Pairing the two
/// matrices is the whole point; a single "codec X is faster" number is what this design refuses to
/// produce.
/// </para>
/// <para>
/// <b>Four numbers per cell</b>: <c>server_net_recv_bytes</c> (bytes on the wire, counted by the
/// server), <c>client_cpu_us</c>, <c>server_cpu_us</c>, and BenchmarkDotNet's wall clock.
/// </para>
/// <para>
/// <b>Client-side level sweep</b> for the two new codecs, per §2 of the plan: <c>Lz4Level</c>
/// Fast/Max, and zstd at 1/3/9. The old codecs run at their shipped default plus
/// <c>CompressionLevel.Optimal</c>, which for brotli is a high quality level and markedly slower —
/// that asymmetry is exactly why brotli's numbers look the way they do.
/// </para>
/// <para>
/// <b>Everything here needs the unreleased v1.4.0 surface.</b> <c>InsertOptions.Compressor</c> (#427)
/// and the built-in lz4 (#431) and zstd (#523/#526) codecs have not shipped, so against a published
/// package this class contributes a single default-compressor arm. That is a real constraint of comparison mode, not an
/// oversight — see docs/API-CORRIDOR.md.
/// </para>
/// <para>
/// Inserts go to <c>ENGINE = Null</c>: the server parses and discards the block, so the server CPU
/// reported is decompression plus parsing, not sorting and part writing. Any claim from this class is
/// about the transport, and must say so.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class CodecInsertBenchmark
{
    private const string Name = nameof(CodecInsertBenchmark);

    private readonly DeferredServerCost serverCost = new();
    private ClickHouseClient client;
    private string sink;

    /// <summary>A codec plus its client-side level, as one parameter. See <see cref="InsertArms"/>.</summary>
    public sealed record InsertArm(string Codec, string Level)
    {
        public override string ToString() => Level.Length == 0 ? Codec : $"{Codec}:{Level}";
    }

    public static IEnumerable<InsertArm> InsertArms()
    {
        yield return new InsertArm("none", string.Empty);

#if CH_API_1_4
        yield return new InsertArm("gzip", "fastest");
        yield return new InsertArm("gzip", "optimal");
        yield return new InsertArm("brotli", "fastest");
        // Brotli's Optimal maps to a high quality level and is dramatically slower than Fastest. Kept
        // precisely because that is the caveat worth publishing.
        yield return new InsertArm("brotli", "optimal");
        yield return new InsertArm("lz4", "fast");
        yield return new InsertArm("lz4", "max");
        yield return new InsertArm("zstd", "1");
        yield return new InsertArm("zstd", "3");
        yield return new InsertArm("zstd", "9");
#else
        // Published packages predate pluggable insert compression: the only available arm is whatever
        // the client does by default.
        yield return new InsertArm("client-default", string.Empty);
#endif
    }

    [ParamsSource(nameof(InsertArms))]
    public InsertArm Arm { get; set; }

    /// <summary>Rows per invocation. Smaller than the content-bound family: this matrix has ~10 cells.</summary>
    public int Rows => Math.Max(1, BenchProfile.ContentRows / 4);

    /// <summary>Default batch size. Batch-count effects are <see cref="HitsInsertBenchmark"/>'s job.</summary>
    private const int BatchSize = 100_000;

    [GlobalSetup]
    public async Task Setup()
    {
        client = BenchEnv.CreateClient();
        sink = $"{BenchEnv.SinkDb}.hits_sink_null";

        var exists = await client.ExecuteScalarAsync(
            $"SELECT count() FROM system.tables WHERE database = '{BenchEnv.SinkDb}' AND name = 'hits_sink_null'");
        if (Convert.ToInt64(exists, CultureInfo.InvariantCulture) == 0)
        {
            throw new InvalidOperationException(
                $"{sink} is missing. Run scripts/stage-datasets.sh.");
        }
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    [IterationSetup]
    public void IterationSetup() => SideMetrics.NextIteration();

    [IterationCleanup]
    public void IterationCleanup() => serverCost.Drain(client);

    [Benchmark]
    public async Task<long> Insert()
    {
        var queryId = ServerMetrics.NewQueryId("insert-" + Arm.Codec);

        var options = new InsertOptions
        {
            BatchSize = BatchSize,
            MaxDegreeOfParallelism = 1,
            QueryId = queryId,
#if CH_API_1_4
            Compressor = Map(Arm),
#endif
        };

        var cpu = CpuProbe.Start();
        var written = await client.InsertBinaryAsync(sink, HitsRow.Columns, StreamRows(Rows), options);

        var elapsed = cpu.ElapsedMicroseconds;
        var args = string.Create(
            CultureInfo.InvariantCulture,
            $"direction=insert;codec={Arm.Codec};level={Arm.Level};rows={Rows}");

        SideMetrics.Record(Name, args, "rows", written);
        SideMetrics.Record(Name, args, "client_cpu_us", cpu.TotalMicroseconds);
        SideMetrics.Record(Name, args, "elapsed_us", elapsed);
        SideMetrics.Record(Name, args, "client_cpu_per_row_us", written > 0 ? cpu.TotalMicroseconds / written : 0);

        var expectedBatches = (long)Math.Ceiling(Rows / (double)BatchSize);
        serverCost.Enqueue(Name, args, queryId, expectedBatches);

        return written;
    }

#if CH_API_1_4
    private static IClickHouseCompressor Map(InsertArm arm) => (arm.Codec, arm.Level) switch
    {
        ("none", _) => null,
        ("gzip", "optimal") => new GZipCompressor(CompressionLevel.Optimal),
        ("gzip", _) => GZipCompressor.Default,
        ("brotli", "optimal") => new BrotliCompressor(CompressionLevel.Optimal),
        ("brotli", _) => BrotliCompressor.Default,
        ("lz4", "max") => new Lz4Compressor(Lz4Level.Max),
        ("lz4", _) => Lz4Compressor.Default,
        ("zstd", "1") => new ZstdCompressor(level: 1),
        ("zstd", "9") => new ZstdCompressor(level: 9),
        ("zstd", _) => ZstdCompressor.Default,
        _ => throw new ArgumentOutOfRangeException(nameof(arm), arm, "No compressor mapping for this arm."),
    };
#endif

    /// <summary>
    /// Streamed rather than pre-materialized: 250k pre-built 105-column <c>object[]</c> rows would be
    /// hundreds of megabytes of live boxes, and holding them would distort every allocation and GC
    /// number in the run.
    /// </summary>
    private static IEnumerable<object[]> StreamRows(int count)
    {
        for (var i = 0; i < count; i++)
            yield return HitsRow.ToObjectArray(HitsRow.Create(i));
    }
}
