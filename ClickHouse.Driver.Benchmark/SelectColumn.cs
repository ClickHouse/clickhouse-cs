using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

[BenchmarkCategory(BenchmarkCategories.HttpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class SelectColumn
{
    private readonly ClickHouseConnection connection;

    // Reads Decimal columns as System.Decimal. The default connection reads them as ClickHouseDecimal,
    // which is a different decoder.
    private readonly ClickHouseConnection systemDecimalConnection;

    [Params(500000)]
    public int Count { get; set; }

    public SelectColumn()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        connection = new ClickHouseConnection(connectionString);
        systemDecimalConnection = new ClickHouseConnection(new ClickHouseClientSettings(connectionString) { UseCustomDecimals = false });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
        systemDecimalConnection.Dispose();
    }

    private Task RunNumericBenchmark(string expression) => RunNumericBenchmark(connection, expression);

    private async Task RunNumericBenchmark(ClickHouseConnection target, string expression)
    {
        using var reader = await target.ExecuteReaderAsync($"SELECT {expression} FROM system.numbers LIMIT {Count}");
        while (reader.Read()) ;
    }

    [Benchmark]
    public async Task SelectInt32() => await RunNumericBenchmark("toInt32(number)");

    [Benchmark]
    public async Task SelectUInt32() => await RunNumericBenchmark("toUInt32(number)");

    [Benchmark]
    public async Task SelectInt64() => await RunNumericBenchmark("toInt64(number)");

    [Benchmark]
    public async Task SelectUInt64() => await RunNumericBenchmark("toUInt64(number)");

    [Benchmark]
    public async Task SelectFloat32() => await RunNumericBenchmark("toFloat32(number)");

    [Benchmark]
    public async Task SelectFloat64() => await RunNumericBenchmark("toFloat64(number)");

    [Benchmark]
    public async Task SelectDecimal64() => await RunNumericBenchmark("toDecimal64(number,5)");

    [Benchmark]
    public async Task SelectDecimal128() => await RunNumericBenchmark("toDecimal128(number,5)");

    [Benchmark]
    public async Task SelectDecimal256() => await RunNumericBenchmark("toDecimal256(number,5)");

    [Benchmark]
    public async Task SelectDecimal64AsSystemDecimal() => await RunNumericBenchmark(systemDecimalConnection, "toDecimal64(number,5)");

    [Benchmark]
    public async Task SelectDecimal128AsSystemDecimal() => await RunNumericBenchmark(systemDecimalConnection, "toDecimal128(number,5)");

    [Benchmark]
    public async Task SelectDecimal256AsSystemDecimal() => await RunNumericBenchmark(systemDecimalConnection, "toDecimal256(number,5)");

    [Benchmark]
    public async Task SelectDate() => await RunNumericBenchmark("toDate(18942+number)");

    [Benchmark]
    public async Task SelectDate32() => await RunNumericBenchmark("toDate32(18942+number)");

    [Benchmark]
    public async Task SelectDateTime() => await RunNumericBenchmark("toDateTime(18942+number,'UTC')");

    [Benchmark]
    public async Task SelectString() => await RunNumericBenchmark("concat('test',toString(number))");

    [Benchmark]
    public async Task SelectArray() => await RunNumericBenchmark("array(1, number, 3)");

    [Benchmark]
    public async Task SelectNullableInt32() => await RunNumericBenchmark("CAST(toInt32(number) AS Nullable(Int32))");

    [Benchmark]
    public async Task SelectTuple() => await RunNumericBenchmark("tuple(number, toString(number))");
}
