using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Separates what a native operation pays for the connection from what it pays for the query, so the
/// value of holding one client is a number rather than advice.
/// </summary>
/// <remarks>
/// <para>
/// The warm arms run on a client whose pool already holds a connection: they measure a checkout plus
/// the round trips. The cold arm builds a client, uses it once and disposes it, so it also pays the
/// dial, the TLS negotiation if enabled, and the protocol handshake.
/// </para>
/// <para>
/// Each arm repeats its operation <see cref="Operations"/> times. One of these operations takes about
/// a millisecond, which is small enough that a single-operation measurement would report the
/// harness's own per-iteration cost alongside it.
/// </para>
/// <para>
/// Over loopback the dial is close to free and the handshake is not, so the gap here is a floor for
/// what a per-operation client costs on a real network, not an estimate of it.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpConnectionCost
{
    private ClickHouseTcpClient warmClient;

    [Params(100)]
    public int Operations { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        warmClient = BenchmarkServer.CreateTcpClient();

        // Leaves a connection in the pool, so the warm arms never pay a dial.
        await warmClient.PingAsync();
    }

    [GlobalCleanup]
    public void Cleanup() => warmClient?.Dispose();

    /// <summary>A pool checkout and one round trip, with no query to run.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task WarmPing()
    {
        for (int i = 0; i < Operations; i++)
        {
            await warmClient.PingAsync();
        }
    }

    /// <summary>The same checkout, running the smallest query the server will answer.</summary>
    [Benchmark]
    public async Task<object> WarmScalar()
    {
        object last = null;
        for (int i = 0; i < Operations; i++)
        {
            last = await warmClient.ExecuteScalarAsync("SELECT 1");
        }

        return last;
    }

    /// <summary>A client per operation: dial, handshake, one round trip, teardown.</summary>
    [Benchmark]
    public async Task ColdClientPing()
    {
        for (int i = 0; i < Operations; i++)
        {
            await using var client = BenchmarkServer.CreateTcpClient();
            await client.PingAsync();
        }
    }
}
