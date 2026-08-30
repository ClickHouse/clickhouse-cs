using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Runs many trivial queries over both transports, so per-request overhead is measured where nothing
/// else can hide it.
/// </summary>
/// <remarks>
/// <para>
/// Every other cross-transport class reads or writes enough rows that decoding dominates. Here the
/// server does no work, so what is left is the request itself: HTTP headers and response framing
/// against the native protocol's packets on an already-open connection.
/// </para>
/// <para>
/// Both clients are warmed in <see cref="Setup"/>, so neither pays a dial inside the measurement.
/// The queries run one after another: this is latency per request, not throughput under load —
/// <see cref="TcpPoolConcurrency"/> covers the concurrent case.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.Cross)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TransportLatency
{
    private const string Sql = "SELECT 1";

    private ClickHouseClient httpClient;
    private ClickHouseTcpClient tcpClient;

    [Params(500)]
    public int RoundTrips { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        httpClient = new ClickHouseClient(BenchmarkServer.HttpUncompressed);
        tcpClient = BenchmarkServer.CreateUncompressedTcpClient();

        await httpClient.ExecuteScalarAsync(Sql);
        await tcpClient.ExecuteScalarAsync(Sql);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        httpClient?.Dispose();
        tcpClient?.Dispose();
    }

    /// <summary>HTTP: one request and response per query, on a pooled socket.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<object> Http()
    {
        object last = null;
        for (int i = 0; i < RoundTrips; i++)
        {
            last = await httpClient.ExecuteScalarAsync(Sql);
        }

        return last;
    }

    /// <summary>Native protocol: query and data packets on a pooled connection.</summary>
    [Benchmark]
    public async Task<object> Tcp()
    {
        object last = null;
        for (int i = 0; i < RoundTrips; i++)
        {
            last = await tcpClient.ExecuteScalarAsync(Sql);
        }

        return last;
    }
}
