using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The write rules of D6 in every insert tier, against a server: <c>InsertAsync</c> takes the CLR types that the POCO
/// write plan takes (an enum as its ordinal, a value type into a nullable type, a nullable value type into a type with no
/// NULL, a cast); the untyped rows take every CLR type that a composite type is written from
/// (ClickHouse/integrations#800); the POCO write plan takes the writes of the converter layer; and a NULL that the column
/// cannot hold fails each tier as its documentation says. Each insert goes out two rows to a block, so every block after
/// the first is a slice that starts after earlier rows.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WriteRulesIntegrationTests
{
    private const string NoNull = " is null at row 2 of the insert, but it cannot hold null. Make the column Nullable(...), or leave out the rows with no value.";

    private static readonly CancellationToken None = CancellationToken.None;

    // The server refuses a LowCardinality of a number or a calendar type without this setting.
    private static readonly ClickHouseTcpQueryOptions CreateOptions = new()
    {
        Settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["allow_suspicious_low_cardinality_types"] = "1" },
    };

    private static readonly DateTime First = new(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);

    private static readonly DateTime Second = new(2024, 6, 15, 12, 0, 1, DateTimeKind.Utc);

    private static readonly DateTime Third = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    internal enum Level : sbyte
    {
        A = 1,
        B = 2,
    }

    internal enum Code
    {
        Low = -1,
        High = 5,
    }

    /// <summary>
    /// The three shapes of ClickHouse/integrations#800: untyped values of a CLR type that the composite type is written
    /// from, but that is not the codec's preferred write type.
    /// </summary>
    [TestCase("Array(DateTime('UTC'))")]
    [TestCase("LowCardinality(DateTime('UTC'))")]
    [TestCase("Array(Nullable(DateTime('UTC')))")]
    public async Task InsertRowsAsync_UntypedValuesOfAConvenienceTypeIntoAComposite_RoundTrip(string type)
    {
        (object[] values, Type elementType, string[] expected) = type switch
        {
            "Array(DateTime('UTC'))" => (
                new object[] { new[] { First, Second }, Array.Empty<DateTime>(), new[] { Third }, new[] { First } },
                typeof(DateTime[]),
                new[] { "['2024-06-15 12:00:00','2024-06-15 12:00:01']", "[]", "['2024-01-01 00:00:00']", "['2024-06-15 12:00:00']" }),
            "LowCardinality(DateTime('UTC'))" => (
                new object[] { First, Second, First, Third },
                typeof(DateTime),
                new[] { "2024-06-15 12:00:00", "2024-06-15 12:00:01", "2024-06-15 12:00:00", "2024-01-01 00:00:00" }),
            _ => (
                new object[] { new DateTime?[] { First, null }, new DateTime?[] { null }, Array.Empty<DateTime?>(), new DateTime?[] { Third } },
                typeof(DateTime?[]),
                new[] { "['2024-06-15 12:00:00',NULL]", "[NULL]", "[]", "['2024-01-01 00:00:00']" }),
        };

        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt32, value {type}) ENGINE = Memory", CreateOptions, None);
            var rows = new List<object[]>();
            for (int i = 0; i < values.Length; i++)
            {
                rows.Add(new[] { (object)(uint)i, values[i] });
            }

            await client.InsertRowsAsync($"INSERT INTO {table} (id, value) VALUES", rows, new ClickHouseTcpInsertOptions { MaxRowsPerBlock = 2 }, None);
            string[] stored = await ReadAsync(client, $"SELECT toString(value) FROM {table} ORDER BY id");

            Assert.Multiple(() =>
            {
                Assert.That(ClickHouseTcpTypes.CanWrite(type, elementType), Is.True);
                Assert.That(stored, Is.EqualTo(expected));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    private static IEnumerable<TestCaseData> RuleColumns()
    {
        yield return Case("Enum8('a' = 1, 'b' = 2)", new[] { Level.A, Level.B, Level.A }, "toString(value)", "a", "b", "a")
            .SetName("{m}(an enum as its ordinal)");
        yield return Case("Int32", new[] { Code.Low, Code.High, (Code)7 }, "toString(value)", "-1", "5", "7")
            .SetName("{m}(an enum as its ordinal, Int32)");
        yield return Case("Nullable(Enum8('a' = 1, 'b' = 2))", new Level?[] { Level.B, null, Level.A }, "ifNull(toString(value), 'NULL')", "b", "NULL", "a")
            .SetName("{m}(a nullable enum as its nullable ordinal)");
        yield return Case("Nullable(Int32)", new[] { 1, 2, 3 }, "ifNull(toString(value), 'NULL')", "1", "2", "3")
            .SetName("{m}(a value type into a nullable type)");
        yield return Case("LowCardinality(Nullable(Int32))", new[] { 4, 4, 5 }, "ifNull(toString(value), 'NULL')", "4", "4", "5")
            .SetName("{m}(a value type into a nullable dictionary)");
        yield return Case("Int32", new int?[] { 1, 2, 3 }, "toString(value)", "1", "2", "3")
            .SetName("{m}(a nullable value type with no NULL)");
        yield return Case("Variant(String, UInt64)", new[] { "x", "y", "z" }, "concat(toString(variantType(value)), ':', toString(value))", "String:x", "String:y", "String:z")
            .SetName("{m}(a cast to object)");
        yield return Case("Array(Int32)", new[] { new[] { Code.Low, Code.High }, Array.Empty<Code>(), new[] { (Code)7 } }, "toString(value)", "[-1,5]", "[]", "[7]")
            .SetName("{m}(an enum array as an array of its underlying type)");
    }

    /// <summary>
    /// A column of a CLR type that the column type is written from only through a write rule of D6: the insert stores the
    /// values that the POCO write plan stores for a property of that type, and <c>CanWrite</c> says true.
    /// </summary>
    [TestCaseSource(nameof(RuleColumns))]
    public async Task InsertAsync_ColumnThatAWriteRuleWrites_StoresTheValues(string type, Func<string, IColumn> build, string readBack, string[] expected)
    {
        IColumn column = build("value");
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt32, value {type}) ENGINE = Memory", CreateOptions, None);
            await client.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES",
                new[] { ClickHouseTcpColumn.Create("id", new uint[] { 0, 1, 2 }), column },
                new ClickHouseTcpInsertOptions { MaxRowsPerBlock = 2 },
                None);
            string[] stored = await ReadAsync(client, $"SELECT {readBack} FROM {table} ORDER BY id");

            Assert.Multiple(() =>
            {
                Assert.That(ClickHouseTcpTypes.CanWrite(type, column.ElementType), Is.True);
                Assert.That(stored, Is.EqualTo(expected));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// The writes of the converter layer reach the POCO write plan: <c>LowCardinality(String)</c> from <see cref="T:byte[]"/>
    /// (ClickHouse/integrations#792), <c>FixedString(N)</c> from <see cref="string"/>, and the nullable rules for a CLR
    /// type that the column type is written from but that is not the codec's preferred write type.
    /// </summary>
    [Test]
    public async Task InsertRowsAsync_PropertiesThatTheConverterLayerWrites_StoreTheValues()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (id UInt32, category LowCardinality(String), code FixedString(4), seen LowCardinality(DateTime('UTC')), stamp LowCardinality(Nullable(DateTime('UTC')))) ENGINE = Memory",
                CreateOptions,
                None);
            var rows = new[]
            {
                new ConvertedRow { Id = 0, Category = new byte[] { 0x41, 0xFF }, Code = "ab", Seen = First, Stamp = Second },
                new ConvertedRow { Id = 1, Category = new byte[] { 0x42 }, Code = "é", Seen = Second, Stamp = First },
                new ConvertedRow { Id = 2, Category = new byte[] { 0x41, 0xFF }, Code = "abcd", Seen = First, Stamp = Third },
            };

            await client.InsertRowsAsync($"INSERT INTO {table} (id, category, code, seen, stamp) VALUES", rows, new ClickHouseTcpInsertOptions { MaxRowsPerBlock = 2 }, None);

            Assert.That(
                await ReadAsync(client, $"SELECT concat(hex(category), '|', hex(code), '|', toString(seen), '|', toString(stamp)) FROM {table} ORDER BY id"),
                Is.EqualTo(new[]
                {
                    "41FF|61620000|2024-06-15 12:00:00|2024-06-15 12:00:01",
                    "42|C3A90000|2024-06-15 12:00:01|2024-06-15 12:00:00",
                    "41FF|61626364|2024-06-15 12:00:00|2024-01-01 00:00:00",
                }));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// A nullable property with a NULL into a column that cannot hold it: the gather finds the NULL before its block is
    /// written, so the blocks before it are stored, the message names the property, the column and the row, and the
    /// connection stays usable.
    /// </summary>
    [Test]
    public async Task InsertAsync_NullablePropertyWithANullIntoATypeWithNoNull_ThrowsAtTheGatherAndKeepsTheConnection()
    {
        var rows = new[] { new IdValue { Id = 0, Value = 1 }, new IdValue { Id = 1, Value = 2 }, new IdValue { Id = 2, Value = null } };
        using var buffer = PocoRowBuffer<IdValue>.Create(rows, "rows", blockRows: 2, None);
        var registry = new PocoTypeRegistry();

        await AssertGatherFailureAsync(
            (connection, sql) => connection.InsertAsync(sql, rows.Length, schema => registry.WritePlanFor<IdValue>(schema).CreateSource(buffer, 2), maxRowsPerBlock: 2, cancellationToken: None),
            "Property 'IdValue.Value' is null at row 2 of the insert, but it maps to column 'value' (Int32), which cannot hold null. Make the column Nullable(...), or leave out the rows with no value.");
    }

    /// <summary>The same for an untyped NULL into a value-type column.</summary>
    [Test]
    public async Task InsertAsync_UntypedNullIntoATypeWithNoNull_ThrowsAtTheGatherAndKeepsTheConnection()
    {
        object[][] rows = { new object[] { 0u, 1 }, new object[] { 1u, 2 }, new object[] { 2u, null } };
        using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", blockRows: 2, None);

        await AssertGatherFailureAsync(
            (connection, sql) => connection.InsertAsync(sql, rows.Length, schema => UntypedRowColumns.CreateSource(schema, buffer, 2), maxRowsPerBlock: 2, cancellationToken: None),
            "Column 1 ('value', Int32)" + NoNull);
    }

    /// <summary>
    /// A nullable column with a NULL into a column type that cannot hold it, through <c>InsertAsync</c>: the write of the
    /// block that holds the NULL stops at the NULL, before the bytes of the column, and names the column, its type and
    /// the row. As for every value that a columnar insert cannot write, the insert ends its connection.
    /// </summary>
    [Test]
    public async Task InsertAsync_NullableColumnWithANullIntoATypeWithNoNull_ThrowsNamingTheRowAndEndsTheConnection()
    {
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt32, value Int32) ENGINE = MergeTree ORDER BY id");

            InvalidOperationException thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await connection.InsertAsync(
                $"INSERT INTO {table} (id, value) VALUES",
                new IColumn[] { ClickHouseTcpColumn.Create("id", new uint[] { 0, 1, 2 }), ClickHouseTcpColumn.Create("value", new int?[] { 1, 2, null }) },
                maxRowsPerBlock: 2,
                cancellationToken: None));

            // The insert that failed ends its connection, so count on a new one.
            await using ClickHouseTcpConnection counter = await TcpServerFixture.ConnectAsync(None);
            string[] count = await ReadAsync(counter, $"SELECT toString(count()) FROM {table}");

            Assert.Multiple(() =>
            {
                Assert.That(thrown.Message, Is.EqualTo("Column 'value' (Int32)" + NoNull));
                Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
                Assert.That(count, Is.EqualTo(new[] { "2" }).Or.EqualTo(new[] { "0" }), "the first block can arrive before the failure; the failing one does not");
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {table}");
        }
    }

    private static TestCaseData Case<T>(string type, T[] values, string readBack, params string[] expected)
        => new(type, (Func<string, IColumn>)(name => ClickHouseTcpColumn.Create(name, values)), readBack, expected);

    // Runs a row insert of rows (0, 1), (1, 2), (2, NULL) into a value-type column, two rows to a block, on one connection.
    private static async Task AssertGatherFailureAsync(Func<ClickHouseTcpConnection, string, ValueTask> insert, string message)
    {
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        string table = UniqueTableName();
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (id UInt32, value Int32) ENGINE = MergeTree ORDER BY id");

            InvalidOperationException thrown = Assert.ThrowsAsync<InvalidOperationException>(async () => await insert(connection, $"INSERT INTO {table} (id, value) VALUES"));
            TcpConnectionState state = connection.State;
            string[] count = await ReadAsync(connection, $"SELECT toString(count()) FROM {table}");

            Assert.Multiple(() =>
            {
                Assert.That(thrown.Message, Is.EqualTo(message));
                Assert.That(state, Is.EqualTo(TcpConnectionState.Ready), "the connection stays at a block boundary");
                Assert.That(count, Is.EqualTo(new[] { "2" }), "the block before the NULL");
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    private static async Task<string[]> ReadAsync(ClickHouseTcpClient client, string sql)
    {
        var values = new List<string>();
        await foreach (Block block in client.StreamAsync(sql, cancellationToken: None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                values.Add((string)block[0].GetValue(row));
            }
        }

        return values.ToArray();
    }

    private static async Task<string[]> ReadAsync(ClickHouseTcpConnection connection, string sql)
    {
        var values = new List<string>();
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                values.Add((string)block[0].GetValue(row));
            }
        }

        return values.ToArray();
    }

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql)
    {
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            _ = block;
        }
    }

    private static string UniqueTableName() => $"tcp_write_rules_test_{Guid.NewGuid():N}";

    internal sealed class IdValue
    {
        public uint Id { get; set; }

        public int? Value { get; set; }
    }

    internal sealed class ConvertedRow
    {
        public uint Id { get; set; }

        public byte[] Category { get; set; }

        public string Code { get; set; }

        public DateTime? Seen { get; set; }

        public DateTime Stamp { get; set; }
    }
}
