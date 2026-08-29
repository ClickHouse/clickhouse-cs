namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates reading data into strongly-typed POCO objects.
/// Register a type once, then stream rows via QueryAsync&lt;T&gt; or materialize the
/// current row of a ClickHouseDataReader with MapTo&lt;T&gt;.
/// </summary>
public static class PocoSelect
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        await BasicQueryAsync(client);
        await AttributeMappingQuery(client);
        await ReaderMapTo(client);
    }

    public class SensorReading
    {
        public ulong Id { get; set; }
        public required string SensorName { get; set; }
        public double Value { get; set; }
        public DateTime Timestamp { get; set; }
    }

    /// <summary>
    /// Streams query results as a registered POCO type via IAsyncEnumerable&lt;T&gt;.
    /// Property names are matched to column names case-sensitively.
    /// </summary>
    private static async Task BasicQueryAsync(ClickHouseClient client)
    {
        Console.WriteLine("1. Basic QueryAsync<T>:");

        await client.ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS example_pocoread_sensors
            (
                Id UInt64,
                SensorName String,
                Value Float64,
                Timestamp DateTime
            )
            ENGINE = MergeTree()
            ORDER BY Id
        ");

        // RegisterPocoType<T> sets up both the insert and read mappings on the client.
        client.RegisterPocoType<SensorReading>();

        var seedRows = Enumerable.Range(1, 5).Select(i => new SensorReading
        {
            Id = (ulong)i,
            SensorName = $"sensor_{i % 3}",
            Value = i * 0.1,
            Timestamp = DateTime.UtcNow.AddSeconds(-i),
        });
        await client.InsertBinaryAsync("example_pocoread_sensors", seedRows);

        Console.WriteLine($"   {"Id",-3} {"Sensor",-12} {"Value",-7} {"Timestamp"}");
        Console.WriteLine($"   {"---",-3} {"------",-12} {"-----",-7} {"---------"}");
        await foreach (var row in client.QueryAsync<SensorReading>(
            "SELECT * FROM example_pocoread_sensors ORDER BY Id"))
        {
            Console.WriteLine($"   {row.Id,-3} {row.SensorName,-12} {row.Value,-7:F2} {row.Timestamp:O}");
        }
        Console.WriteLine();

        await client.ExecuteNonQueryAsync("DROP TABLE IF EXISTS example_pocoread_sensors");
    }

    public class AuditEvent
    {
        [ClickHouseColumn(Name = "event_id")]
        public ulong Id { get; set; }

        [ClickHouseColumn(Name = "event_type")]
        public required string Type { get; set; }

        [ClickHouseColumn(Name = "payload")]
        public required string Payload { get; set; }

        [ClickHouseColumn(Name = "created_at")]
        public DateTime CreatedAt { get; set; }

        [ClickHouseNotMapped]
        public string? LocalSentinel { get; set; } // not bound on the read path
    }

    /// <summary>
    /// Demonstrates that [ClickHouseColumn(Name = "...")] and [ClickHouseNotMapped]
    /// apply on the read path the same way they apply on InsertBinaryAsync&lt;T&gt;.
    /// </summary>
    private static async Task AttributeMappingQuery(ClickHouseClient client)
    {
        Console.WriteLine("2. QueryAsync<T> with [ClickHouseColumn(Name)] aliases:");

        await client.ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS example_pocoread_audit
            (
                event_id UInt64,
                event_type LowCardinality(String),
                payload String,
                created_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY event_id
        ");

        client.RegisterPocoType<AuditEvent>();

        var seed = new[]
        {
            new AuditEvent { Id = 1, Type = "login",  Payload = """{"user":"alice"}""", CreatedAt = DateTime.UtcNow },
            new AuditEvent { Id = 2, Type = "logout", Payload = """{"user":"bob"}""",   CreatedAt = DateTime.UtcNow },
        };
        await client.InsertBinaryAsync("example_pocoread_audit", seed);

        await foreach (var ev in client.QueryAsync<AuditEvent>(
            "SELECT * FROM example_pocoread_audit ORDER BY event_id"))
        {
            Console.WriteLine($"   #{ev.Id} [{ev.Type}] {ev.Payload}");
            // ev.LocalSentinel stays at default (null) — it was excluded by [ClickHouseNotMapped].
        }
        Console.WriteLine();

        await client.ExecuteNonQueryAsync("DROP TABLE IF EXISTS example_pocoread_audit");
    }

    /// <summary>
    /// Lower-level ADO.NET path: open a reader, advance with Read(), and materialize the
    /// current row with MapTo&lt;T&gt;. Useful when you want both raw reader access and
    /// occasional POCO materialization on the same row.
    /// </summary>
    private static async Task ReaderMapTo(ClickHouseClient client)
    {
        Console.WriteLine("3. ClickHouseDataReader.MapTo<T> on the current row:");

        client.RegisterPocoType<SensorReading>();

        using var reader = await client.ExecuteReaderAsync(@"
            SELECT toUInt64(number + 1)             AS Id,
                   concat('sensor_', toString(number % 3)) AS SensorName,
                   toFloat64(number) * 0.1          AS Value,
                   now() - toIntervalSecond(number) AS Timestamp
            FROM numbers(3)
        ");

        while (reader.Read())
        {
            // GetValue / typed accessors still work on the same row.
            var rawId = reader.GetFieldValue<ulong>(0);

            // MapTo materializes the current row into a fresh SensorReading instance —
            // it does not advance the reader.
            var row = reader.MapTo<SensorReading>();

            Console.WriteLine($"   raw[0]={rawId} | poco={{ Id={row.Id}, SensorName={row.SensorName}, Value={row.Value:F2} }}");
        }

        Console.WriteLine();
    }
}
