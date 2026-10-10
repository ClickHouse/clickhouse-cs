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
