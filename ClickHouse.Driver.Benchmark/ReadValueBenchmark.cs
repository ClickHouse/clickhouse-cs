using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Passthrough converter that returns all values unchanged.
/// Tests the overhead of a virtual call on every GetValue.
/// </summary>
internal sealed class PassthroughConverter : IReadValueConverter
{
    public object ConvertValue(object value, string columnName, string clickHouseType) => value;
    public T ConvertValue<T>(T value, string columnName, string clickHouseType) => value;
}

/// <summary>
/// Selective converter that only transforms DateTime values (sets Kind to Utc).
/// Tests the overhead of type-checking + conditional transformation.
/// </summary>
internal sealed class DateTimeKindConverter : IReadValueConverter
{
    public object ConvertValue(object value, string columnName, string clickHouseType)
    {
        if (value is DateTime dt)
            return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        return value;
    }

    public T ConvertValue<T>(T value, string columnName, string clickHouseType)
    {
        if (typeof(T) == typeof(DateTime) && value is DateTime dt)
            return (T)(object)DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        return value;
    }
}

[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class ReadValueBenchmark
{
    private ClickHouseConnection noConverterConn;
    private ClickHouseConnection passthroughConn;
    private ClickHouseConnection selectiveConn;

    [Params(500000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ?? "Host=localhost";

        // No converter (null) - tests null-check overhead
        noConverterConn = new ClickHouseConnection(new ClickHouseClientSettings(connectionString));

        // Passthrough converter - tests virtual call overhead
        passthroughConn = new ClickHouseConnection(new ClickHouseClientSettings(connectionString)
        {
            ReadValueConverter = new PassthroughConverter(),
        });

        // Selective converter - only transforms DateTime
        selectiveConn = new ClickHouseConnection(new ClickHouseClientSettings(connectionString)
        {
            ReadValueConverter = new DateTimeKindConverter(),
        });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        noConverterConn?.Dispose();
        passthroughConn?.Dispose();
        selectiveConn?.Dispose();
    }

    // ==========================================
    // Int32 - GetValue (boxed)
    // ==========================================

    [Benchmark(Baseline = true)]
    public async Task Int32_GetValue_NoConverter()
    {
        using var reader = await noConverterConn.ExecuteReaderAsync($"SELECT toInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    [Benchmark]
    public async Task Int32_GetValue_Passthrough()
    {
        using var reader = await passthroughConn.ExecuteReaderAsync($"SELECT toInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    [Benchmark]
    public async Task Int32_GetValue_Selective()
    {
        using var reader = await selectiveConn.ExecuteReaderAsync($"SELECT toInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    // ==========================================
    // Int32 - GetFieldValue<T> (generic)
    // ==========================================

    [Benchmark]
    public async Task Int32_GetFieldValue_NoConverter()
    {
        using var reader = await noConverterConn.ExecuteReaderAsync($"SELECT toInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<int>(0);
    }

    [Benchmark]
    public async Task Int32_GetFieldValue_Passthrough()
    {
        using var reader = await passthroughConn.ExecuteReaderAsync($"SELECT toInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<int>(0);
    }

    [Benchmark]
    public async Task Int32_GetFieldValue_Selective()
    {
        using var reader = await selectiveConn.ExecuteReaderAsync($"SELECT toInt32(number) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<int>(0);
    }

    // ==========================================
    // DateTime - GetValue (boxed)
    // ==========================================

    [Benchmark]
    public async Task DateTime_GetValue_NoConverter()
    {
        using var reader = await noConverterConn.ExecuteReaderAsync($"SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    [Benchmark]
    public async Task DateTime_GetValue_Passthrough()
    {
        using var reader = await passthroughConn.ExecuteReaderAsync($"SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    [Benchmark]
    public async Task DateTime_GetValue_Selective()
    {
        using var reader = await selectiveConn.ExecuteReaderAsync($"SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    // ==========================================
    // DateTime - GetFieldValue<T> (generic)
    // ==========================================

    [Benchmark]
    public async Task DateTime_GetFieldValue_NoConverter()
    {
        using var reader = await noConverterConn.ExecuteReaderAsync($"SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<DateTime>(0);
    }

    [Benchmark]
    public async Task DateTime_GetFieldValue_Passthrough()
    {
        using var reader = await passthroughConn.ExecuteReaderAsync($"SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<DateTime>(0);
    }

    [Benchmark]
    public async Task DateTime_GetFieldValue_Selective()
    {
        using var reader = await selectiveConn.ExecuteReaderAsync($"SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<DateTime>(0);
    }

    // ==========================================
    // String - GetValue (boxed)
    // ==========================================

    [Benchmark]
    public async Task String_GetValue_NoConverter()
    {
        using var reader = await noConverterConn.ExecuteReaderAsync($"SELECT concat('test',toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    [Benchmark]
    public async Task String_GetValue_Passthrough()
    {
        using var reader = await passthroughConn.ExecuteReaderAsync($"SELECT concat('test',toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    [Benchmark]
    public async Task String_GetValue_Selective()
    {
        using var reader = await selectiveConn.ExecuteReaderAsync($"SELECT concat('test',toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetValue(0);
    }

    // ==========================================
    // String - GetFieldValue<T> (generic)
    // ==========================================

    [Benchmark]
    public async Task String_GetFieldValue_NoConverter()
    {
        using var reader = await noConverterConn.ExecuteReaderAsync($"SELECT concat('test',toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<string>(0);
    }

    [Benchmark]
    public async Task String_GetFieldValue_Passthrough()
    {
        using var reader = await passthroughConn.ExecuteReaderAsync($"SELECT concat('test',toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<string>(0);
    }

    [Benchmark]
    public async Task String_GetFieldValue_Selective()
    {
        using var reader = await selectiveConn.ExecuteReaderAsync($"SELECT concat('test',toString(number)) FROM system.numbers LIMIT {Count}");
        while (reader.Read())
            _ = reader.GetFieldValue<string>(0);
    }
}
