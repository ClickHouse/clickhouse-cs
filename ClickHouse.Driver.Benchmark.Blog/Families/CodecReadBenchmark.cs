using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace ClickHouse.Driver.Benchmark.Blog.Families;

/// <summary>
/// Codec matrix, <b>read direction</b>: the server compresses, the client decompresses.
/// </summary>
/// <remarks>
/// <para>
/// This is one of two matrices, and conflating it with the other is the most common way a "codec X is
/// faster" claim goes wrong. On a read the server's cost is <b>compression</b>; on an insert it is
/// <b>decompression</b>. Those are completely different cost curves — compression level barely moves
/// decompression cost, and the codec that wins one direction need not win the other. See
/// <see cref="CodecInsertBenchmark"/> for the other half.
/// </para>
/// <para>
/// <b>Four numbers per cell</b>, all recorded to the side-channel CSV:
/// </para>
/// <list type="bullet">
///   <item><c>server_net_send_bytes</c> — bytes on the wire, measured by the server itself.</item>
///   <item><c>client_cpu_us</c> — what decoding cost the application host.</item>
///   <item><c>server_cpu_us</c> — <c>OSCPUVirtualTimeMicroseconds</c>. Transport-independent, so it
///     cannot be confounded by network conditions, and on a managed service it is what the user pays
///     for.</item>
///   <item>Wall clock — BenchmarkDotNet's own column.</item>
/// </list>
/// <para>
/// <b>The level sweep is where the story lives.</b> <c>http_zlib_compression_level</c> governs the
/// server's compression effort for every codec, not just zlib. lz4 only engages LZ4-HC from level 3,
/// so at level 1 the comparison against zstd flatters lz4's CPU and penalises its ratio; at level 9
/// brotli's server cost becomes extreme. Sweeping 1/3/9 turns both of those from footnotes into one
/// figure.
/// </para>
/// <para>
/// <b>Varying the response codec needs <c>AcceptEncoding</c>, which is unreleased.</b> It arrived with
/// #490/#526, <i>not</i> in v1.3.0 as the benchmark plan's corridor table states — the 1.3.0 tag has no
/// such property. So against any published package every arm collapses onto the client default, and
/// the cross-version compression claim is necessarily default-vs-default. Which is what users actually
/// experience anyway, so it makes the better chart.
/// </para>
/// </remarks>
[Config(typeof(LongRunConfig))]
public class CodecReadBenchmark
{
    private const string Name = nameof(CodecReadBenchmark);

    private readonly DeferredServerCost serverCost = new();
    private readonly Dictionary<string, ClickHouseClient> clients = [];

    /// <summary>
    /// A (codec, server compression level) pair. Modelled as one parameter rather than two so
    /// meaningless combinations — a level sweep over the uncompressed arm — never get run.
    /// </summary>
    public sealed record ReadArm(string Codec, int ZlibLevel)
    {
        /// <summary>Shown verbatim in the BenchmarkDotNet table, so keep it short and sortable.</summary>
        public override string ToString() =>
            Codec == "identity" ? "identity" : $"{Codec}@L{ZlibLevel}";
    }

    /// <summary>
    /// Codecs at their shipped default level, plus a 1/3/9 sweep. <c>identity</c> is the control and
    /// appears once, because the compression level does not apply to it.
    /// </summary>
    /// <remarks>
    /// <c>brotli</c> is decodable but deliberately not advertised by the driver's default
    /// <c>Accept-Encoding</c>; it is included here because "we can decode it and here is what it
    /// costs" is a fair thing to publish, and because it is the level-9 cautionary tale.
    /// </remarks>
    public static IEnumerable<ReadArm> ReadArms()
    {
        yield return new ReadArm("identity", 0);

        foreach (var level in new[] { 1, 3, 9 })
        {
            yield return new ReadArm("gzip", level);
            yield return new ReadArm("deflate", level);
            yield return new ReadArm("br", level);
            yield return new ReadArm("lz4", level);
            yield return new ReadArm("zstd", level);
        }
    }

    [ParamsSource(nameof(ReadArms))]
    public ReadArm Arm { get; set; }

    /// <summary>
    /// Rows read per invocation. Scaled by the profile, and much smaller than the content-bound family:
    /// the matrix has ~16 cells and every one of them runs.
    /// </summary>
    public int Rows => Math.Max(1, BenchProfile.ContentRows / 4);

    [GlobalSetup]
    public void Setup()
    {
        // One client per codec, built once. Rebuilding a client per iteration would fold connection
        // setup into a measurement that is supposed to be about decode cost.
        foreach (var arm in ReadArms())
        {
            if (clients.ContainsKey(arm.Codec))
                continue;

            clients[arm.Codec] = CreateClient(arm.Codec);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (var client in clients.Values)
            client.Dispose();

        clients.Clear();
    }

    [IterationSetup]
    public void IterationSetup() => SideMetrics.NextIteration();

    [IterationCleanup]
    public void IterationCleanup() => serverCost.Drain(clients["identity"]);

    /// <summary>
    /// Scans <see cref="Rows"/> wide rows with the arm's codec and server compression level.
    /// </summary>
    /// <remarks>
    /// The reader is drained, not merely opened: an arm that abandoned the body would skip the
    /// decompression this benchmark exists to measure and would look dramatically fastest.
    /// </remarks>
    [Benchmark]
    public async Task<long> Scan()
    {
        var client = clients[Arm.Codec];
        var queryId = ServerMetrics.NewQueryId("read-" + Arm.Codec);
        var options = new QueryOptions { QueryId = queryId };

        if (Arm.Codec != "identity")
        {
            options = new QueryOptions
            {
                QueryId = queryId,
                CustomSettings = new Dictionary<string, object>
                {
                    ["http_zlib_compression_level"] = Arm.ZlibLevel,
                },
            };
        }

        var cpu = CpuProbe.Start();
        long rows = 0;

        using (var reader = await client.ExecuteReaderAsync(
            $"SELECT * FROM {BenchEnv.Hits} LIMIT {Rows}",
            options: options))
        {
            while (reader.Read())
                rows++;
        }

        var elapsed = cpu.ElapsedMicroseconds;
        var args = string.Create(
            CultureInfo.InvariantCulture,
            $"direction=read;codec={Arm.Codec};level={Arm.ZlibLevel};rows={Rows}");

        SideMetrics.Record(Name, args, "rows", rows);
        SideMetrics.Record(Name, args, "client_cpu_us", cpu.TotalMicroseconds);
        SideMetrics.Record(Name, args, "elapsed_us", elapsed);
        SideMetrics.Record(Name, args, "client_cpu_per_row_us", rows > 0 ? cpu.TotalMicroseconds / rows : 0);

        serverCost.Enqueue(Name, args, queryId);
        return rows;
    }

    private static ClickHouseClient CreateClient(string codec)
    {
        if (codec == "identity")
        {
            return BenchEnv.CreateClient(new ClickHouseClientSettings(BenchEnv.ConnectionString)
            {
                UseCompression = false,
            });
        }

#if CH_API_1_4
        // AcceptEncoding is what makes a per-codec read arm possible at all, and it is UNRELEASED.
        // Gating this on CH_API_1_3 is what the corridor check caught: the 1.3.0 package builds fine
        // until this line, then fails with "ClickHouseClientSettings does not contain a definition for
        // AcceptEncoding".
        return BenchEnv.CreateClient(new ClickHouseClientSettings(BenchEnv.ConnectionString)
        {
            UseCompression = true,
            AcceptEncoding = codec,
        });
#else
        // Against any published package the codec cannot be chosen, only compression on/off. Every
        // compressed arm therefore collapses onto the client default, and those results must be read
        // as default-vs-default rather than codec-by-codec.
        return BenchEnv.CreateClient(new ClickHouseClientSettings(BenchEnv.ConnectionString)
        {
            UseCompression = true,
        });
#endif
    }
}
