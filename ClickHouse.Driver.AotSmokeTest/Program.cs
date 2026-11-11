using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Runtime.CompilerServices;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.AotSmokeTest;
using static ClickHouse.Driver.AotSmokeTest.Checks;

var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
if (string.IsNullOrEmpty(connectionString))
{
    Console.Error.WriteLine("Set CLICKHOUSE_CONNECTION to an HTTP connection string, e.g. Host=localhost;Password=...");
    return 2;
}

// False in the published binary, and under `dotnet run` too: PublishAot makes the SDK set the same feature switches
// for a JIT run, so both run the driver's AOT code paths. Only an app without PublishAot sees true.
var aot = !RuntimeFeature.IsDynamicCodeSupported;
Console.WriteLine(aot ? "Dynamic code is not supported: checking the native AOT behavior" : "Dynamic code is supported");

using var client = new ClickHouseClient(connectionString);
var checks = new Checks();
var suffix = Guid.NewGuid().ToString("N");

await checks.Run("Ping", async () => True(await client.PingAsync(), "ping returned false"));

foreach (var (expression, expected) in ReadCases.All)
{
    await checks.Run($"Read {expression}", async () =>
    {
        using var reader = await client.ExecuteReaderAsync($"SELECT {expression}");
        True(reader.Read(), "no row");
        var value = reader.GetValue(0);
        Equal(expected, Show(value));

        // The reader's metadata has to describe the value it returns, which native AOT represents differently
        var fieldType = reader.GetFieldType(0);
        if (value is not DBNull && fieldType != typeof(object))
            Equal(fieldType.ToString(), value.GetType().ToString(), "GetFieldType");
    });
}

await checks.Run("Read Map(String, String) keeps its CLR type", async () =>
{
    var value = await client.ExecuteScalarAsync("SELECT map('a', 'b')::Map(String, String)");
    True(value is Dictionary<string, string>, $"got {value.GetType()}");
});

await checks.Run("Read Point keeps its CLR type", async () =>
{
    var value = await client.ExecuteScalarAsync("SELECT (1.5, 2.5)::Point");
    True(value is Tuple<double, double>, $"got {value.GetType()}");
});

await checks.Run("Read a multidimensional array", async () =>
{
    using var reader = await client.ExecuteReaderAsync("SELECT [[1, 2], [3, 4]]::Array(Array(Int32))");
    True(reader.Read(), "no row");
    var value = reader.GetFieldValue<int[,]>(0);
    Equal("2x2 [1, 2, 3, 4]", $"{value.GetLength(0)}x{value.GetLength(1)} {Show(value)}");
});

await checks.Run("GetSchemaTable", async () =>
{
    using var reader = await client.ExecuteReaderAsync("SELECT 1::UInt8 AS a, 'x' AS b, [1]::Array(Int32) AS c");
    var schema = reader.GetSchemaTable();
    Equal("a,b,c", string.Join(",", schema.Rows.Cast<System.Data.DataRow>().Select(r => r["ColumnName"])));
    Equal("System.Byte", schema.Rows[0]["DataType"].ToString(), "DataType");
});

await checks.Run("QueryStats from the summary header", async () =>
{
    using var connection = client.CreateConnection();
    using var command = connection.CreateCommand();
    command.CommandText = "SELECT count() FROM numbers(1000)";
    await command.ExecuteScalarAsync();
    True(command.QueryStats is { ReadRows: 1000 }, $"got {command.QueryStats}");
});

await checks.Run("Parameters", async () =>
{
    var parameters = new ClickHouseParameterCollection
    {
        new ClickHouseDbParameter { ParameterName = "i", Value = 5 },
        new ClickHouseDbParameter { ParameterName = "a", Value = new[] { 1, 2 } },
        new ClickHouseDbParameter { ParameterName = "m", Value = new Dictionary<string, int> { ["k"] = 3 } },
        new ClickHouseDbParameter { ParameterName = "t", Value = Tuple.Create(4, "five") },
        new ClickHouseDbParameter { ParameterName = "d", Value = 6.5m },
        new ClickHouseDbParameter { ParameterName = "j", Value = "{\"x\": 7}" },
    };
    using var reader = await client.ExecuteReaderAsync(
        "SELECT {i:Int32}, arraySum({a:Array(Int32)}), {m:Map(String, Int32)}['k'], " +
        "tupleElement({t:Tuple(Int32, String)}, 2), {d:Decimal(9, 2)}, toString({j:JSON}.x)",
        parameters);
    True(reader.Read(), "no row");
    var row = Enumerable.Range(0, reader.FieldCount).Select(i => Show(reader.GetValue(i)));
    Equal("5|3|3|five|6.50|7", string.Join("|", row));
});

await checks.Run("JSON parameter from an object", async () =>
{
    var parameters = new ClickHouseParameterCollection
    {
        new ClickHouseDbParameter { ParameterName = "j", Value = new JsonDoc { A = 1, B = "x" } },
    };
    Task Query() => client.ExecuteScalarAsync("SELECT toString({j:JSON}.B)", parameters);

    if (aot)
    {
        // Reflection-based System.Text.Json is off in a native AOT app; a string or JsonNode is the way in
        var error = await Throws<InvalidOperationException>(Query);
        True(error.Message.Contains("Reflection-based serialization has been disabled"), error.Message);
    }
    else
    {
        Equal("x", Show(await client.ExecuteScalarAsync("SELECT toString({j:JSON}.B)", parameters)));
    }
});

var table = $"aot_smoke_{suffix}";
await checks.Run("Create table", () => client.ExecuteNonQueryAsync($"""
    CREATE TABLE {table}
    (
        Id UInt64,
        S String,
        N Nullable(Int32),
        A Array(Int32),
        M Map(String, Int32),
        Ms Map(String, String),
        T Tuple(Int32, String),
        P Point,
        D Decimal(18, 4),
        Dt DateTime64(3, 'UTC'),
        U UUID,
        Ip IPv4,
        V Variant(String, UInt64),
        Lc LowCardinality(String)
    )
    ENGINE = Memory
    """));

var columns = new[] { "Id", "S", "N", "A", "M", "Ms", "T", "P", "D", "Dt", "U", "Ip", "V", "Lc" };
var guid = Guid.Parse("3f2504e0-4f89-11d3-9a0c-0305e82c3301");
var time = new DateTime(2026, 1, 2, 3, 4, 5, 123, DateTimeKind.Utc);

await checks.Run("Insert object[] rows", async () =>
{
    var rows = new[]
    {
        new object[]
        {
            1UL, "a", 1, new[] { 1, 2 }, new Dictionary<string, int> { ["k"] = 1 },
            new List<KeyValuePair<string, string>> { new("x", "y") }, Tuple.Create(1, "t"), Tuple.Create(1.5, 2.5),
            1.25m, time, guid, IPAddress.Parse("1.2.3.4"), "variant", "lc",
        },
        new object[]
        {
            2UL, "b", null, Array.Empty<int>(), new Dictionary<string, int>(),
            new Dictionary<string, string>(), new object[] { 2, "u" }, Tuple.Create(0.0, 0.0),
            0m, time, Guid.Empty, IPAddress.Parse("0.0.0.0"), 9UL, string.Empty,
        },
    };
    Equal("2", (await client.InsertBinaryAsync(table, columns, rows)).ToString(), "rows written");
});

await checks.Run("Insert a KeyValuePair sequence into a Map", async () =>
{
    object[] Row(object map) =>
    [
        3UL, "c", null, Array.Empty<int>(), map, new Dictionary<string, string>(), Tuple.Create(3, "v"),
        Tuple.Create(0.0, 0.0), 0m, time, guid, IPAddress.Loopback, "w", "lc",
    ];

    // Native AOT reads a pair's key and value only for the pair types it compiled ahead of time
    var pairs = new[] { new KeyValuePair<string, int>("k", 3) };
    if (aot)
    {
        var error = await Throws<Exception>(() => client.InsertBinaryAsync(table, columns, [Row(pairs)]));
        True(error.InnerException is NotSupportedException { Message: var message } && message.Contains("IDictionary"), error.ToString());
        await client.InsertBinaryAsync(table, columns, [Row(new List<KeyValuePair<object, object>> { new("k", 3) })]);
    }
    else
    {
        await client.InsertBinaryAsync(table, columns, [Row(pairs)]);
    }
});

client.RegisterPocoType<PocoRow>();

await checks.Run("Insert POCO rows", async () =>
{
    var row = new PocoRow
    {
        Id = 4, S = "d", N = 4, A = [4], M = new Dictionary<string, int> { ["k"] = 4 },
        Ms = new Dictionary<string, string> { ["p"] = "q" }, T = Tuple.Create(4, "x"), P = Tuple.Create(4.0, 4.5),
        D = new ClickHouse.Driver.Numerics.ClickHouseDecimal(4.5m), Dt = time, U = guid, Ip = IPAddress.Parse("4.4.4.4"), V = 4UL, Lc = "lc",
    };
    Equal("1", (await client.InsertBinaryAsync(table, [row])).ToString(), "rows written");
});

await checks.Run("Read the table back", async () =>
{
    using var reader = await client.ExecuteReaderAsync($"SELECT * FROM {table} ORDER BY Id");
    var rows = new List<string>();
    while (reader.Read())
        rows.Add(string.Join(" | ", Enumerable.Range(0, reader.FieldCount).Select(i => Show(reader.GetValue(i)))));

    Equal(string.Join("\n", ExpectedRows), string.Join("\n", rows));
});

await checks.Run("Read the table as POCOs", async () =>
{
    var rows = new List<string>();
    await foreach (var row in client.QueryAsync<PocoRow>($"SELECT * FROM {table} ORDER BY Id"))
    {
        rows.Add(string.Join(" | ", Show(row.Id), Show(row.S), Show(row.N), Show(row.A), Show(row.M), Show(row.Ms),
            Show(row.T), Show(row.P), Show(row.D), Show(row.Dt), Show(row.U), Show(row.Ip), Show(row.V), Show(row.Lc)));
    }

    Equal(string.Join("\n", ExpectedRows), string.Join("\n", rows));
});

await checks.Run("Map a POCO property with a CLR type native AOT cannot build", async () =>
{
    client.RegisterPocoType<TypedMapRow>();
    async Task<int> CountRows()
    {
        var count = 0;
        await foreach (var _ in client.QueryAsync<TypedMapRow>($"SELECT Id, M FROM {table} ORDER BY Id"))
            count++;
        return count;
    }

    if (aot)
    {
        // A Map(String, Int32) reads as a Dictionary<object, object>, which the property cannot hold
        var error = await Throws<InvalidOperationException>(CountRows);
        True(error.Message.Contains("TypedMapRow"), error.Message);
    }
    else
    {
        Equal("4", (await CountRows()).ToString(), "rows");
    }
});

await checks.Run("Map a POCO property that a Variant value does not fit", async () =>
{
    // Row 1 holds a String in V. The column's CLR type is object, so the setter itself has to reject it,
    // which under native AOT is a reflection setter rather than a compiled one.
    client.RegisterPocoType<TypedVariantRow>();
    var error = await Throws<InvalidOperationException>(async () =>
    {
        await foreach (var _ in client.QueryAsync<TypedVariantRow>($"SELECT Id, V FROM {table} ORDER BY Id"))
        {
        }
    });
    True(error.Message.Contains("TypedVariantRow.V") && error.Message.Contains("System.String"), error.Message);
});

await checks.Run("Map a Dynamic value onto an enum property", async () =>
{
    // The value is a boxed Int32; the compiled setter's unboxing accepts it for an int-backed enum, and so must
    // the reflection setter native AOT uses
    client.RegisterPocoType<EnumRow>();
    var rows = new List<string>();
    await foreach (var row in client.QueryAsync<EnumRow>("SELECT toInt32(1)::Dynamic AS E"))
        rows.Add(row.E.ToString());
    Equal("One", string.Join(";", rows));
});

var variantTable = $"aot_smoke_variant_{suffix}";
await checks.Run("Write Tuple and Map values into Variant columns, and write back what was read", async () =>
{
    await client.ExecuteNonQueryAsync($"""
        CREATE TABLE {variantTable}
        (
            Id UInt64,
            Vt Variant(String, Tuple(Int32, String)),
            Vm Variant(String, Map(String, Int32)),
            -- Under native AOT both members read as LargeTuple, so arity and elements pick the member
            Vp Variant(Tuple(Int32, String), Tuple(String, String))
        )
        ENGINE = Memory
        """);
    string[] variantColumns = ["Id", "Vt", "Vm", "Vp"];
    await client.InsertBinaryAsync(variantTable, variantColumns,
    [
        [1UL, Tuple.Create(1, "a"), new Dictionary<string, int> { ["k"] = 1 }, Tuple.Create("x", "y")],
        [2UL, "s", "t", Tuple.Create(2, "z")],
    ]);

    // Native AOT reads these as a LargeTuple and a Dictionary<object, object>
    using (var reader = await client.ExecuteReaderAsync($"SELECT Vt, Vm, Vp FROM {variantTable} WHERE Id = 1"))
    {
        True(reader.Read(), "no row");
        await client.InsertBinaryAsync(variantTable, variantColumns, [[3UL, reader.GetValue(0), reader.GetValue(1), reader.GetValue(2)]]);
    }

    using var all = await client.ExecuteReaderAsync($"SELECT Id, Vt, Vm, Vp, variantType(Vp) FROM {variantTable} ORDER BY Id");
    var rows = new List<string>();
    while (all.Read())
        rows.Add(string.Join(" ", Enumerable.Range(0, all.FieldCount).Select(i => Show(all.GetValue(i)))));
    Equal(
        "1 (1, a) {k: 1} (x, y) Tuple(String, String);2 s t (2, z) Tuple(Int32, String);3 (1, a) {k: 1} (x, y) Tuple(String, String)",
        string.Join(";", rows));
});

var jsonTable = $"aot_smoke_json_{suffix}";
await checks.Run("Create JSON table", () =>
    client.ExecuteNonQueryAsync($"CREATE TABLE {jsonTable} (Id UInt64, J JSON) ENGINE = Memory"));

await checks.Run("Insert JSON as a string and a JsonNode", async () =>
{
    var node = System.Text.Json.Nodes.JsonNode.Parse("{\"A\": 2}");
    await client.InsertBinaryAsync(jsonTable, ["Id", "J"], [[1UL, "{\"A\": 1}"], [2UL, node]]);
});

await checks.Run("Insert JSON from a POCO", async () =>
{
    object[][] rows = [[3UL, new JsonDoc { A = 3, B = "x" }]];
    if (aot)
    {
        // The default string write mode serializes a POCO with reflection-based System.Text.Json
        var error = await Throws<Exception>(() => client.InsertBinaryAsync(jsonTable, ["Id", "J"], rows));
        True(error.ToString().Contains("Reflection-based serialization has been disabled"), error.ToString());

        // The binary write mode writes a registered POCO from its properties instead
        using var binaryClient = new ClickHouseClient(connectionString + ";JsonWriteMode=Binary");
        RegisterJsonDoc(binaryClient);
        await binaryClient.InsertBinaryAsync(jsonTable, ["Id", "J"], rows);
    }
    else
    {
        await client.InsertBinaryAsync(jsonTable, ["Id", "J"], rows);
    }
});

await checks.Run("Read the JSON table back", async () =>
{
    using var reader = await client.ExecuteReaderAsync($"SELECT J FROM {jsonTable} ORDER BY Id");
    var rows = new List<string>();
    while (reader.Read())
        rows.Add(Show(reader.GetValue(0)));
    Equal("{\"A\":1} {\"A\":2} {\"A\":3,\"B\":\"x\"}", string.Join(" ", rows));
});

foreach (var name in new[] { table, variantTable, jsonTable })
    await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {name}");

Console.WriteLine();
Console.WriteLine($"{checks.Passed} passed, {checks.Failures.Count} failed");
foreach (var failure in checks.Failures)
    Console.WriteLine($"  {failure}");
return checks.Failures.Count == 0 ? 0 : 1;

// JsonDoc has no nested object properties, which is what the warning is about
[UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "JsonDoc has no nested object properties.")]
static void RegisterJsonDoc(ClickHouseClient client) => client.RegisterJsonSerializationType<JsonDoc>();

internal partial class Program
{
    private static readonly string[] ExpectedRows =
    [
        "1 | a | 1 | [1, 2] | {k: 1} | {x: y} | (1, t) | (1.5, 2.5) | 1.2500 | 2026-01-02 03:04:05.1230000 Utc | 3f2504e0-4f89-11d3-9a0c-0305e82c3301 | 1.2.3.4 | variant | lc",
        "2 | b | NULL | [] | {} | {} | (2, u) | (0, 0) | 0.0000 | 2026-01-02 03:04:05.1230000 Utc | 00000000-0000-0000-0000-000000000000 | 0.0.0.0 | 9 | ",
        "3 | c | NULL | [] | {k: 3} | {} | (3, v) | (0, 0) | 0.0000 | 2026-01-02 03:04:05.1230000 Utc | 3f2504e0-4f89-11d3-9a0c-0305e82c3301 | 127.0.0.1 | w | lc",
        "4 | d | 4 | [4] | {k: 4} | {p: q} | (4, x) | (4, 4.5) | 4.5000 | 2026-01-02 03:04:05.1230000 Utc | 3f2504e0-4f89-11d3-9a0c-0305e82c3301 | 4.4.4.4 | 4 | lc",
    ];
}

/// <summary>A row of the smoke table. Map and Tuple columns are typed by interface, which both runtimes fill.</summary>
internal sealed class PocoRow
{
    public ulong Id { get; set; }

    public string S { get; set; }

    public int? N { get; set; }

    public int[] A { get; set; }

    public IDictionary M { get; set; }

    public IDictionary Ms { get; set; }

    public ITuple T { get; set; }

    public Tuple<double, double> P { get; set; }

    public ClickHouse.Driver.Numerics.ClickHouseDecimal D { get; set; }

    public DateTime Dt { get; set; }

    public Guid U { get; set; }

    public IPAddress Ip { get; set; }

    public object V { get; set; }

    public string Lc { get; set; }
}

internal sealed class TypedMapRow
{
    public ulong Id { get; set; }

    public Dictionary<string, int> M { get; set; }
}

internal sealed class TypedVariantRow
{
    public ulong Id { get; set; }

    public ulong V { get; set; }
}

internal enum Level
{
    Zero,
    One,
    Two,
}

internal sealed class EnumRow
{
    public Level E { get; set; }
}

internal sealed class JsonDoc
{
    public int A { get; set; }

    public string B { get; set; }
}
