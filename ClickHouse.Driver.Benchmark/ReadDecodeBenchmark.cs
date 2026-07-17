using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Measures per-row deserialization allocations for the fixed-size binary types whose
/// <c>Read</c> path was moved from <c>ReadBytes(int)</c> (a per-row heap <c>byte[]</c>) to a
/// stack-allocated / pooled scratch buffer (#669).
///
/// The absolute wall time is dominated by the HTTP round-trip and is noisy on a loopback server,
/// but the <see cref="MemoryDiagnoser"/> allocation and Gen0/1/2 columns are deterministic and are
/// the signal of interest here: the removed <c>byte[]</c> per row (×<see cref="Count"/>) shows up
/// as a clean drop in "Allocated" and in gen0 collection frequency.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class ReadDecodeBenchmark
{
    private readonly Consumer consumer = new Consumer();
    private ClickHouseConnection connection;

    [Params(200000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ?? "Host=localhost";
        connection = new ClickHouseConnection(new ClickHouseClientSettings(connectionString));
    }

    [GlobalCleanup]
    public void Cleanup() => connection?.Dispose();

    private async Task ReadAll(string valueExpr)
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT {valueExpr} FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            consumer.Consume(reader.GetValue(0));
    }

    // AbstractBigIntegerType.Read — signed path
    [Benchmark(Baseline = true)]
    public Task Int128() => ReadAll("toInt128(number)");

    [Benchmark]
    public Task Int256() => ReadAll("toInt256(number)");

    // AbstractBigIntegerType.Read — unsigned path (was byte-by-byte into byte[Size+1])
    [Benchmark]
    public Task UInt256() => ReadAll("toUInt256(number)");

    // DecimalType.Read — Size > 8 mantissa path (BigInteger)
    [Benchmark]
    public Task Decimal128() => ReadAll("toDecimal128(number, 4)");

    [Benchmark]
    public Task Decimal256() => ReadAll("toDecimal256(number, 4)");

    // IPv4Type.Read / IPv6Type.Read
    [Benchmark]
    public Task IPv4() => ReadAll("toIPv4('192.168.0.1')");

    [Benchmark]
    public Task IPv6() => ReadAll("toIPv6('2001:db8::1')");

    // FixedStringType.Read default path — small (stackalloc) vs large (ArrayPool)
    [Benchmark]
    public Task FixedStringSmall() => ReadAll("toFixedString('hello', 16)");

    [Benchmark]
    public Task FixedStringLarge() => ReadAll("toFixedString(repeat('x', 400), 512)");
}
