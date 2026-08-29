namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates inserting data using strongly-typed POCO objects instead of object[] arrays.
/// Register a type once, then pass IEnumerable&lt;T&gt; directly to InsertBinaryAsync.
/// </summary>
public static class PocoInsert
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        await BasicPocoInsert(client);
        await AttributeMappingInsert(client);
        await ExplicitTypesInsert(client);
    }

    // Simple POCO with no annotations
    public class SensorReading
    {
        public ulong Id { get; set; }
        public required string SensorName { get; set; }
        public double Value { get; set; }
        public DateTime Timestamp { get; set; }
    }
    
    /// <summary>
    /// Insert POCO object with no annotations, column names are matched with case sensitivity.
    /// </summary>
    private static async Task BasicPocoInsert(ClickHouseClient client)
    {
        Console.WriteLine("1. Basic POCO insert:");

        await client.ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS example_poco_sensors
            (
                Id UInt64,
                SensorName String,
                Value Float64,
                Timestamp DateTime
            )
            ENGINE = MergeTree()
            ORDER BY Id
        ");

        client.RegisterBinaryInsertType<SensorReading>();

        var readings = Enumerable.Range(1, 1000).Select(i => new SensorReading
        {
            Id = (ulong)i,
            SensorName = $"sensor_{i % 10}",
            Value = Random.Shared.NextDouble() * 100,
            Timestamp = DateTime.UtcNow.AddSeconds(-i),
        });

        var rowsInserted = await client.InsertBinaryAsync("example_poco_sensors", readings);
        Console.WriteLine($"   Inserted {rowsInserted} sensor readings");

        var count = await client.ExecuteScalarAsync("SELECT count() FROM example_poco_sensors");
        Console.WriteLine($"   Verified: {count} rows in table\n");

        await client.ExecuteNonQueryAsync("DROP TABLE IF EXISTS example_poco_sensors");
    }
    
    // A POCO with attribute customization
    public class AuditEvent
    {
        [ClickHouseColumn(Name = "event_id")]
        public ulong Id { get; set; }

        [ClickHouseColumn(Name = "event_type", Type = "LowCardinality(String)")]
        public required string Type { get; set; }

        [ClickHouseColumn(Name = "payload")]
        public required string Payload { get; set; }

        [ClickHouseColumn(Name = "created_at")]
        public DateTime CreatedAt { get; set; }

        [ClickHouseNotMapped]
        public required string InternalTag { get; set; } // Excluded from insert
    }

    /// <summary>
    /// Use [ClickHouseColumn] to specify column name and type, and [ClickHouseNotMapped] to exclude properties.
    /// </summary>
    private static async Task AttributeMappingInsert(ClickHouseClient client)
    {
        Console.WriteLine("2. POCO insert with attribute mapping:");

        await client.ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS example_poco_audit
            (
                event_id UInt64,
                event_type LowCardinality(String),
                payload String,
                created_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY event_id
        ");

        client.RegisterBinaryInsertType<AuditEvent>();

        var events = new[]
        {
            new AuditEvent
            {
                Id = 1, Type = "login", Payload = """{"user": "alice"}""",
                CreatedAt = DateTime.UtcNow, InternalTag = "ignored",
            },
            new AuditEvent
            {
                Id = 2, Type = "logout", Payload = """{"user": "bob"}""",
                CreatedAt = DateTime.UtcNow, InternalTag = "also ignored",
            },
        };

        var rowsInserted = await client.InsertBinaryAsync("example_poco_audit", events);
        Console.WriteLine($"   Inserted {rowsInserted} audit events");
        Console.WriteLine("   (InternalTag was excluded via [ClickHouseNotMapped])");

        var count = await client.ExecuteScalarAsync("SELECT count() FROM example_poco_audit");
        Console.WriteLine($"   Verified: {count} rows in table\n");

        await client.ExecuteNonQueryAsync("DROP TABLE IF EXISTS example_poco_audit");
    }

    // A POCO with explicit types on every property skips the schema probe query entirely
    public class MetricPoint
    {
        [ClickHouseColumn(Type = "UInt64")]
        public ulong Id { get; set; }

        [ClickHouseColumn(Type = "String")]
        public required string Name { get; set; }

        [ClickHouseColumn(Type = "Float64")]
        public double Value { get; set; }
    }
    
    /// <summary>
    /// When ALL properties have [ClickHouseColumn(Type = "...")], the driver knows the column
    /// types at compile time and skips the SELECT ... WHERE 1=0 schema probe query.
    /// </summary>
    private static async Task ExplicitTypesInsert(ClickHouseClient client)
    {
        Console.WriteLine("3. POCO insert with explicit types (no schema probe):");

        await client.ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS example_poco_metrics
            (
                Id UInt64,
                Name String,
                Value Float64
            )
            ENGINE = MergeTree()
            ORDER BY Id
        ");

        client.RegisterBinaryInsertType<MetricPoint>();

        var metrics = Enumerable.Range(1, 500).Select(i => new MetricPoint
        {
            Id = (ulong)i,
            Name = $"cpu.usage.core{i % 8}",
            Value = Random.Shared.NextDouble() * 100,
        });

        var rowsInserted = await client.InsertBinaryAsync("example_poco_metrics", metrics);
        Console.WriteLine($"   Inserted {rowsInserted} metric points (schema probe skipped)");

        var count = await client.ExecuteScalarAsync("SELECT count() FROM example_poco_metrics");
        Console.WriteLine($"   Verified: {count} rows in table\n");

        await client.ExecuteNonQueryAsync("DROP TABLE IF EXISTS example_poco_metrics");
    }
}
