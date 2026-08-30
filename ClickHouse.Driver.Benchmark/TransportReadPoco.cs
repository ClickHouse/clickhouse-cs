using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Reads the same rows into the same objects over both transports, which is the question an
/// application mapping results to types actually asks.
/// </summary>
/// <remarks>
/// <para>
/// The third arm builds the same objects from the native block tier, so the table separates two
/// choices that are easy to confuse: which transport to use, and whether to leave the row tier once
/// on it.
/// </para>
/// <para>
/// Both clients run at their own defaults, as in <see cref="TransportRead"/>. This is
/// characterization: the arms move together when the server or the runner changes.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.Cross)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TransportReadPoco
{
    private ClickHouseClient httpClient;
    private ClickHouseTcpClient tcpClient;

    [Params(500_000)]
    public int Count { get; set; }

    private string Sql =>
        "SELECT toUInt64(number) AS Id, concat('city', toString(number % 100)) AS City, " +
        $"toFloat64(number) / 7 AS Temperature FROM system.numbers LIMIT {Count}";

    [GlobalSetup]
    public void Setup()
    {
        httpClient = new ClickHouseClient(BenchmarkServer.Http);
        httpClient.RegisterPocoType<Reading>();
        tcpClient = BenchmarkServer.CreateTcpClient();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        httpClient?.Dispose();
        tcpClient?.Dispose();
    }

    /// <summary>HTTP, through the client's POCO reader.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<double> Http()
    {
        double total = 0;
        await foreach (Reading row in httpClient.QueryAsync<Reading>(Sql))
        {
            total += (double)row.Id + row.City.Length + row.Temperature;
        }

        return total;
    }

    /// <summary>Native protocol, through its POCO reader.</summary>
    [Benchmark]
    public async Task<double> TcpPoco()
    {
        double total = 0;
        await foreach (Reading row in tcpClient.QueryAsync<Reading>(Sql))
        {
            total += (double)row.Id + row.City.Length + row.Temperature;
        }

        return total;
    }

    /// <summary>Native protocol, building the same objects from columnar spans.</summary>
    [Benchmark]
    public async Task<double> TcpBlocksToPoco()
    {
        double total = 0;
        await foreach (Block block in tcpClient.StreamAsync(Sql))
        {
            var ids = block.Column<ulong>("Id").Values;
            var cities = block.Column<string>("City").Values;
            var temperatures = block.Column<double>("Temperature").Values;

            for (int row = 0; row < block.RowCount; row++)
            {
                var reading = new Reading
                {
                    Id = ids[row],
                    City = cities[row],
                    Temperature = temperatures[row],
                };

                total += (double)reading.Id + reading.City.Length + reading.Temperature;
            }
        }

        return total;
    }

    public class Reading
    {
        public ulong Id { get; set; }

        public string City { get; set; }

        public double Temperature { get; set; }
    }
}
