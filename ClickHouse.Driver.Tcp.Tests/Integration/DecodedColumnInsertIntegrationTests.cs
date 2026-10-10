using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// A column that a query read, inserted into a table whose column has another scale, timezone, enum definition,
/// precision or width. The server reads back the values of the source: the same instants, durations and labels.
/// </summary>
[TestFixture]
[Category("Integration")]
public class DecodedColumnInsertIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly Dictionary<string, string> TimeSettings = new(StringComparer.Ordinal)
    {
        ["enable_time_time64_type"] = "1",
        ["allow_experimental_time_time64_type"] = "1",
    };

    public static IEnumerable<TestCaseData> Cases()
    {
        yield return Case(
            "DateTime64(3, 'UTC')",
            "DateTime64(6, 'UTC')",
            ("'2024-01-02 03:04:05.678'", "2024-01-02 03:04:05.678000"),
            ("'1969-12-31 23:59:59.999'", "1969-12-31 23:59:59.999000"));
        yield return Case("DateTime64(6, 'UTC')", "DateTime64(3, 'UTC')", ("'2024-01-02 03:04:05.678000'", "2024-01-02 03:04:05.678"));
        yield return Case("DateTime64(3, 'UTC')", "DateTime64(3, 'Asia/Tokyo')", ("'2024-01-02 03:04:05.678'", "2024-01-02 12:04:05.678"));
        yield return Case("DateTime('UTC')", "DateTime('Asia/Tokyo')", ("'2024-01-02 03:04:05'", "2024-01-02 12:04:05"));
        yield return Case("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 2, 'b' = 1)", ("'a'", "a"), ("'b'", "b"));
        yield return Case("Enum8('a' = 1, 'b' = 2)", "Enum16('b' = 1000, 'a' = -1000)", ("'b'", "b"), ("'a'", "a"));
        yield return Case("Nullable(DateTime64(3, 'UTC'))", "Nullable(DateTime64(6, 'UTC'))", ("'2024-01-02 03:04:05.678'", "2024-01-02 03:04:05.678000"), ("NULL", null));
        yield return Case("Array(DateTime64(3, 'UTC'))", "Array(DateTime64(6, 'UTC'))", ("['2024-01-02 03:04:05.678']", "['2024-01-02 03:04:05.678000']"), ("[]", "[]"));
        yield return Case("Map(String, Enum8('a' = 1, 'b' = 2))", "Map(String, Enum8('a' = 2, 'b' = 1))", ("{'k': 'b'}", "{'k':'b'}"));
        yield return Case(
            "Tuple(DateTime64(3, 'UTC'), Enum8('a' = 1, 'b' = 2))",
            "Tuple(DateTime64(6, 'UTC'), Enum8('b' = 1, 'a' = 2))",
            ("('2024-01-02 03:04:05.678', 'a')", "('2024-01-02 03:04:05.678000','a')"));
        yield return Case("Decimal(9, 2)", "Decimal(18, 4)", ("1.23", "1.23"), ("-4.5", "-4.5"));
    }

    // A part whose values mean other values next to parts that keep their meaning, which hold bytes that are not UTF-8: the
    // SQL literal of the source row, the expression that the target reads back, and its text.
    public static IEnumerable<TestCaseData> PartCases()
    {
        yield return PartCase(
            "Tuple(DateTime64(3, 'UTC'), String)",
            "Tuple(DateTime64(6, 'UTC'), String)",
            "('2024-01-02 03:04:05.678', unhex('FF'))",
            "concat(toString(value.1), ' ', hex(value.2))",
            "2024-01-02 03:04:05.678000 FF");
        yield return PartCase(
            "Map(String, DateTime64(3, 'UTC'))",
            "Map(String, DateTime64(6, 'UTC'))",
            "map(unhex('FF'), toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC'), unhex('C328'), toDateTime64('2024-01-02 03:04:05.679', 3, 'UTC'))",
            "concat(hex(arrayStringConcat(mapKeys(value), ',')), ' ', toString(mapValues(value)))",
            "FF2CC328 ['2024-01-02 03:04:05.678000','2024-01-02 03:04:05.679000']");
        yield return PartCase(
            "Map(Enum8('a' = 1, 'b' = 2), String)",
            "Map(Enum8('a' = 2, 'b' = 1), String)",
            "map('b', unhex('FF'))",
            "concat(toString(mapKeys(value)), ' ', hex(mapValues(value)[1]))",
            "['b'] FF");
        yield return PartCase(
            "Array(Tuple(DateTime64(3, 'UTC'), String))",
            "Array(Tuple(DateTime64(6, 'UTC'), String))",
            "[('2024-01-02 03:04:05.678', unhex('C328')), ('2024-01-02 03:04:05.679', unhex('FF'))]",
            "concat(toString(arrayMap(x -> x.1, value)), ' ', hex(arrayStringConcat(arrayMap(x -> x.2, value), ',')))",
            "['2024-01-02 03:04:05.678000','2024-01-02 03:04:05.679000'] C3282CFF");
        yield return PartCase(
            "Tuple(DateTime64(3, 'UTC'), LowCardinality(String), FixedString(2))",
            "Tuple(DateTime64(6, 'UTC'), LowCardinality(String), FixedString(2))",
            "('2024-01-02 03:04:05.678', unhex('FF'), unhex('C328'))",
            "concat(toString(value.1), ' ', hex(value.2), ' ', hex(value.3))",
            "2024-01-02 03:04:05.678000 FF C328");
    }

    // A String that a query read, into another type of a string: the SQL literal of the source row, the expression that
    // the target reads back, and its text.
    public static IEnumerable<TestCaseData> StringCases()
    {
        yield return PartCase("String", "LowCardinality(String)", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "LowCardinality(Nullable(String))", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "Nullable(String)", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "FixedString(1)", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "Nullable(FixedString(1))", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "LowCardinality(FixedString(1))", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("LowCardinality(String)", "String", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("Nullable(String)", "LowCardinality(Nullable(String))", "NULL", "ifNull(hex(value), 'NULL')", "NULL");
        yield return PartCase("Array(String)", "Array(LowCardinality(String))", "[unhex('FF'), unhex('C328')]", "hex(arrayStringConcat(value, ','))", "FF2CC328");
        yield return PartCase(
            "Map(String, String)",
            "Map(LowCardinality(String), Nullable(String))",
            "map(unhex('FF'), unhex('C328'))",
            "concat(hex(mapKeys(value)[1]), ' ', hex(mapValues(value)[1]))",
            "FF C328");
        yield return PartCase("Tuple(String, Int32)", "Tuple(FixedString(1), Int32)", "(unhex('FF'), 7)", "concat(hex(value.1), ' ', toString(value.2))", "FF 7");
        yield return PartCase(
            "Tuple(DateTime64(3, 'UTC'), String)",
            "Tuple(DateTime64(6, 'UTC'), LowCardinality(String))",
            "('2024-01-02 03:04:05.678', unhex('FF'))",
            "concat(toString(value.1), ' ', hex(value.2))",
            "2024-01-02 03:04:05.678000 FF");
    }

    public static IEnumerable<TestCaseData> TimeCases()
    {
        yield return Case("Time64(3)", "Time64(6)", ("'01:02:03.456'", "01:02:03.456000"), ("'-00:00:00.001'", "-00:00:00.001000"));
    }

    [TestCaseSource(nameof(Cases))]
    public Task InsertAsync_ColumnReadAsARelatedType_StoresTheValuesOfTheSource(string source, string target, string[] literals, string[] expected)
        => AssertStoresTheValuesAsync(source, target, literals, expected, settings: null);

    [TestCaseSource(nameof(TimeCases))]
    [RequiresServerFeature(TcpFeature.Time)]
    public Task InsertAsync_Time64ColumnReadAsAnotherScale_StoresTheValuesOfTheSource(string source, string target, string[] literals, string[] expected)
        => AssertStoresTheValuesAsync(source, target, literals, expected, TimeSettings);

    /// <summary>
    /// A column that a query read, with a part whose values mean other values in the target and parts that hold bytes that
    /// are not UTF-8: the server stores the converted values and the bytes of the other parts.
    /// </summary>
    [TestCaseSource(nameof(PartCases))]
    public Task InsertAsync_ColumnReadAsARelatedComposite_StoresTheBytesOfTheOtherParts(string source, string target, string literal, string readBack, string expected)
        => AssertReadsBackAsync(source, target, literal, readBack, expected);

    /// <summary>
    /// A <c>String</c> column that a query read, or a <c>String</c> part of one, into another type of a string
    /// (<c>LowCardinality</c>, <c>Nullable</c>, <c>FixedString</c>, also in <c>Array</c>, <c>Map</c> and <c>Tuple</c>): the
    /// server stores its bytes, also bytes that are not UTF-8.
    /// </summary>
    [TestCaseSource(nameof(StringCases))]
    public Task InsertAsync_StringReadIntoAnotherTypeOfAString_StoresItsBytes(string source, string target, string literal, string readBack, string expected)
        => AssertReadsBackAsync(source, target, literal, readBack, expected);

    // Reads the source row through one connection, inserts its block through another, and reads back the expression.
    private static async Task AssertReadsBackAsync(string source, string target, string literal, string readBack, string expected)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, new[] { literal }, settings: null);

            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", cancellationToken: None))
                {
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, cancellationToken: None);
                }
            }

            var stored = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT {readBack} FROM {targetTable} ORDER BY id", cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    stored.Add((string)block[0].GetValue(row));
                }
            }

            Assert.That(stored, Is.EqualTo(new[] { expected }));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A column that a query read, with a NULL row that hides a value that the target cannot hold (<c>nullIf</c> keeps the
    /// value under the NULL): the insert stores the NULL and does not convert or check the hidden value. The source is
    /// checked to hold the hidden value first.
    /// </summary>
    [TestCase(
        "Nullable(Tuple(DateTime64(3, 'UTC'), Decimal(18, 2)))",
        "Nullable(Tuple(DateTime64(6, 'UTC'), Decimal(9, 2)))",
        "tuple(toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC'), toDecimal64('1234567890.12', 2))",
        "tuple(toDateTime64('2024-01-02 03:04:05.679', 3, 'UTC'), toDecimal64('1.23', 2))",
        "('2024-01-02 03:04:05.679000',1.23)")]
    [TestCase("Nullable(Decimal(18, 2))", "Nullable(Decimal(9, 2))", "toDecimal64('1234567890.12', 2)", "toDecimal64('1.23', 2)", "1.23")]
    public async Task InsertAsync_ColumnReadWithAValueHiddenUnderNull_StoresTheNull(string source, string target, string hidden, string value, string expected)
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["allow_experimental_nullable_tuple_type"] = "1" };
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {sourceTable} (id UInt32, value {source}) ENGINE = Memory", settings);
            await ExecuteAsync(connection, $"CREATE TABLE {targetTable} (id UInt32, value {target}) ENGINE = Memory", settings);
            await ExecuteAsync(connection, $"INSERT INTO {sourceTable} (id, value) SELECT 0, nullIf({hidden}, {hidden}) UNION ALL SELECT 1, {value}", settings);

            var held = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT toString(assumeNotNull(value)) FROM {sourceTable} WHERE id = 0", settings: settings, cancellationToken: None))
            {
                held.Add((string)block[0].GetValue(0));
            }

            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: settings, cancellationToken: None))
                {
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: settings, cancellationToken: None);
                }
            }

            var stored = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT ifNull(toString(value), 'NULL') FROM {targetTable} ORDER BY id", settings: settings, cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    stored.Add((string)block[0].GetValue(row));
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(held, Has.Count.EqualTo(1).And.Some.Contains("1234567890.12"), "the NULL row of the source hides the value");
                Assert.That(stored, Is.EqualTo(new[] { "NULL", expected }));
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A value that the target cannot hold (a label that it does not declare, an instant finer than its scale), and a
    /// column that no CLR type converts (a scale above 7), are refused, and nothing is stored.
    /// </summary>
    [TestCase("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 1)", "'b'", "'b' is not a label of 'Enum8('a' = 1)'")]
    [TestCase("DateTime64(6, 'UTC')", "DateTime64(3, 'UTC')", "'2024-01-02 03:04:05.678901'", "without losing precision")]
    [TestCase("DateTime64(9, 'UTC')", "DateTime64(6, 'UTC')", "'2024-01-02 03:04:05.678901234'", "is written only into a column of the same scale")]
    public async Task InsertAsync_ColumnReadAsARelatedTypeThatTheTargetCannotHold_IsRefused(string source, string target, string literal, string message)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, new[] { literal }, settings: null);

            ArgumentException refusal = null;
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", cancellationToken: None))
                {
                    refusal = Assert.ThrowsAsync<ArgumentException>(
                        async () => await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, cancellationToken: None));
                }
            }

            // A write that fails inside the block ends its connection, so count on another one.
            await using ClickHouseTcpConnection counter = await TcpServerFixture.ConnectAsync(None);
            List<string> stored = await ReadStringsAsync(counter, targetTable, settings: null);

            Assert.Multiple(() =>
            {
                Assert.That(refusal?.Message, Does.Contain(message));
                Assert.That(stored, Is.Empty, "nothing may land from a column that could not be written");
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    private static TestCaseData PartCase(string source, string target, string literal, string readBack, string expected)
        => new TestCaseData(source, target, literal, readBack, expected).SetArgDisplayNames(source, target);

    // One row for each pair: the SQL literal that the source table holds, and the text that the target reads back.
    private static TestCaseData Case(string source, string target, params (string Literal, string Expected)[] rows)
        => new TestCaseData(source, target, Array.ConvertAll(rows, row => row.Literal), Array.ConvertAll(rows, row => row.Expected)).SetArgDisplayNames(source, target);

    // Reads the source table through one connection and inserts each block, as it is decoded, through another.
    private static async Task AssertStoresTheValuesAsync(string source, string target, string[] literals, string[] expected, IReadOnlyDictionary<string, string> settings)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, literals, settings);

            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: settings, cancellationToken: None))
                {
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: settings, cancellationToken: None);
                }
            }

            Assert.That(await ReadStringsAsync(connection, targetTable, settings), Is.EqualTo(expected));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    // The two tables, and the source rows: one row for each literal, in order.
    private static async Task CreateAsync(
        ClickHouseTcpConnection connection,
        string sourceTable,
        string targetTable,
        string source,
        string target,
        string[] literals,
        IReadOnlyDictionary<string, string> settings)
    {
        await ExecuteAsync(connection, $"CREATE TABLE {sourceTable} (id UInt32, value {source}) ENGINE = Memory", settings);
        await ExecuteAsync(connection, $"CREATE TABLE {targetTable} (id UInt32, value {target}) ENGINE = Memory", settings);
        string rows = string.Join(", ", System.Linq.Enumerable.Select(literals, (literal, id) => $"({id}, {literal})"));
        await ExecuteAsync(connection, $"INSERT INTO {sourceTable} (id, value) VALUES {rows}", settings);
    }

    private static async Task<List<string>> ReadStringsAsync(ClickHouseTcpConnection connection, string table, IReadOnlyDictionary<string, string> settings)
    {
        var values = new List<string>();
        await foreach (Block block in connection.QueryAsync($"SELECT toString(value) FROM {table} ORDER BY id", settings: settings, cancellationToken: None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                values.Add((string)block[0].GetValue(row));
            }
        }

        return values;
    }

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql, IReadOnlyDictionary<string, string> settings = null)
    {
        await foreach (Block block in connection.QueryAsync(sql, settings: settings, cancellationToken: None))
        {
            _ = block;
        }
    }

    private static string UniqueTableName() => $"tcp_decoded_insert_test_{Guid.NewGuid():N}";
}
