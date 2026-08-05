using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public class InsertBinarySchemaTests : AbstractConnectionTestFixture
{
    /// <summary>
    /// Creates the table and returns its database-qualified name. Tests that set
    /// <see cref="InsertOptions.Database"/> must hand the API under test the bare name
    /// (<see cref="TestUtilities.BareTableName"/>) — a qualified name resolves the database on its
    /// own, so the override would be a no-op and the schema cache would be keyed on a dotted table
    /// name no real caller produces. The qualified name stays in DDL and read-back queries.
    /// </summary>
    private async Task<string> CreateSimpleTestTableAsync([CallerMemberName] string testName = null)
    {
        var tableName = CreateTableName(testName);
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (id UInt64, value String)
            ENGINE = MergeTree() ORDER BY id");
        return tableName;
    }

    private static IEnumerable<object[]> GenerateTestRows(int count, ulong startId = 1)
    {
        for (ulong i = 0; i < (ulong)count; i++)
            yield return new object[] { startId + i, $"Value_{startId + i}" };
    }

    /// <summary>
    /// Counts the schema-probe queries (<c>WHERE 1=0</c>) logged under <paramref name="queryIdPrefix"/>.
    /// </summary>
    /// <param name="queryIdPrefix">Query-id prefix the insert under test was given.</param>
    /// <param name="expectedCount">
    /// How many probe queries the caller expects, so the lookup keeps waiting until they are all
    /// visible. A caller expecting none still gets the full wait before the count is reported.
    /// </param>
    /// <returns>The number of probe queries visible in <c>system.query_log</c>.</returns>
    private Task<ulong> CountSchemaProbeQueriesAsync(string queryIdPrefix, ulong expectedCount = 1) =>
        QueryLog.CountAsync(
            client,
            $"SELECT count() FROM system.query_log " +
            $"WHERE query_id LIKE '{queryIdPrefix}%' " +
            $"AND query LIKE '%WHERE 1=0%' " +
            $"AND type = 'QueryFinish'",
            minimumCount: expectedCount);

    [Test]
    public async Task InsertBinaryAsync_WithColumnTypes_ShouldSkipSchemaQuery()
    {
        var tableName = await CreateSimpleTestTableAsync();
        var bareTableName = TestUtilities.BareTableName(tableName);
        var queryId = $"test_col_types_skip_{Guid.NewGuid():N}";
        var options = new InsertOptions
        {
            Database = "test",
            QueryId = queryId,
            ColumnTypes = new Dictionary<string, string>
            {
                ["id"] = "UInt64",
                ["value"] = "String",
            },
        };

        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            GenerateTestRows(5).ToList(),
            options);

        var probeCount = await CountSchemaProbeQueriesAsync(queryId);
        Assert.That(probeCount, Is.EqualTo(0UL),
            "No schema probe query should be sent when ColumnTypes is provided");
    }

    [Test]
    public async Task InsertBinaryAsync_WithColumnTypes_ShouldRoundTripData()
    {
        var tableName = await CreateSimpleTestTableAsync();
        var bareTableName = TestUtilities.BareTableName(tableName);
        var options = new InsertOptions
        {
            Database = "test",
            ColumnTypes = new Dictionary<string, string>
            {
                ["id"] = "UInt64",
                ["value"] = "String",
            },
        };

        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            new List<object[]>
            {
                new object[] { 1UL, "hello" },
                new object[] { 2UL, "world" },
            },
            options);

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT id, value FROM {tableName} ORDER BY id");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
        Assert.That(reader.GetString(1), Is.EqualTo("hello"));

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
        Assert.That(reader.GetString(1), Is.EqualTo("world"));

        Assert.That(reader.Read(), Is.False);
    }

    [Test]
    public void InsertBinaryAsync_WithColumnTypes_MissingColumn_ShouldThrow()
    {
        var options = new InsertOptions
        {
            ColumnTypes = new Dictionary<string, string>
            {
                ["id"] = "UInt64",
                // "value" is missing
            },
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.InsertBinaryAsync(
                "nonexistent",
                new[] { "id", "value" },
                new List<object[]> { new object[] { 1UL, "test" } },
                options));

        Assert.That(ex.Message, Does.Contain("value"));
    }

    [Test]
    public void InsertBinaryAsync_WithColumnTypes_NullColumns_ShouldThrow()
    {
        var options = new InsertOptions
        {
            ColumnTypes = new Dictionary<string, string>
            {
                ["id"] = "UInt64",
            },
        };

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.InsertBinaryAsync(
                "nonexistent",
                null,
                new List<object[]> { new object[] { 1UL } },
                options));
    }

    [Test]
    public void InsertBinaryAsync_WithColumnTypes_InvalidType_ShouldThrow()
    {
        var options = new InsertOptions
        {
            ColumnTypes = new Dictionary<string, string>
            {
                ["id"] = "NotAValidClickHouseType",
            },
        };

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.InsertBinaryAsync(
                "nonexistent",
                new[] { "id" },
                new List<object[]> { new object[] { 1UL } },
                options));
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_ShouldQueryOnce()
    {
        var tableName = await CreateSimpleTestTableAsync();
        var bareTableName = TestUtilities.BareTableName(tableName);
        var queryId = $"test_cache_once_{Guid.NewGuid():N}";
        var options = new InsertOptions
        {
            Database = "test",
            QueryId = queryId,
            UseSchemaCache = true,
        };

        // First insert — should trigger schema probe
        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            GenerateTestRows(3).ToList(),
            options);

        // Second insert — should reuse cached schema
        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            GenerateTestRows(3, startId: 100).ToList(),
            options);

        var probeCount = await CountSchemaProbeQueriesAsync(queryId, expectedCount: 1);
        Assert.That(probeCount, Is.EqualTo(1UL),
            "Only one schema probe query should be sent when UseSchemaCache is true");
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_DifferentColumns_ShouldQueryOnce()
    {
        var tableName = CreateTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (id UInt64, value String, extra String DEFAULT 'x')
            ENGINE = MergeTree() ORDER BY id");

        var bareTableName = TestUtilities.BareTableName(tableName);
        var queryId = $"test_cache_diffcols_{Guid.NewGuid():N}";
        var options = new InsertOptions
        {
            Database = "test",
            QueryId = queryId,
            UseSchemaCache = true,
        };

        // Insert with columns [id, value]
        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            new List<object[]> { new object[] { 1UL, "a" } },
            options);

        // Insert with columns [id, extra] — same table, should reuse cached schema
        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "extra" },
            new List<object[]> { new object[] { 2UL, "b" } },
            options);

        var probeCount = await CountSchemaProbeQueriesAsync(queryId, expectedCount: 1);
        Assert.That(probeCount, Is.EqualTo(1UL),
            "Cache is per-table — different column subsets should share the same cached schema");
    }

    [Test]
    public async Task InsertBinaryAsync_WithColumnTypesAndSchemaCache_ShouldPreferColumnTypes()
    {
        var tableName = await CreateSimpleTestTableAsync();
        var bareTableName = TestUtilities.BareTableName(tableName);
        var queryId = $"test_types_priority_{Guid.NewGuid():N}";
        var options = new InsertOptions
        {
            Database = "test",
            QueryId = queryId,
            ColumnTypes = new Dictionary<string, string>
            {
                ["id"] = "UInt64",
                ["value"] = "String",
            },
            UseSchemaCache = true,
        };

        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            GenerateTestRows(3).ToList(),
            options);

        var probeCount = await CountSchemaProbeQueriesAsync(queryId);
        Assert.That(probeCount, Is.EqualTo(0UL),
            "ColumnTypes should take priority over UseSchemaCache — no schema query expected");
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_SubsetThenSuperset_ShouldRoundTripData()
    {
        var tableName = CreateTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (id UInt64, value String, extra String DEFAULT 'default_val')
            ENGINE = MergeTree() ORDER BY id");

        var bareTableName = TestUtilities.BareTableName(tableName);
        var options = new InsertOptions
        {
            Database = "test",
            UseSchemaCache = true,
        };

        // First insert uses only [id, value] — cache should be populated with SELECT *
        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            new List<object[]> { new object[] { 1UL, "first" } },
            options);

        // Second insert uses [id, value, extra] — must work from the same cached schema
        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value", "extra" },
            new List<object[]> { new object[] { 2UL, "second", "custom_val" } },
            options);

        // Verify both rows round-tripped correctly
        using var reader = await client.ExecuteReaderAsync(
            $"SELECT id, value, extra FROM {tableName} ORDER BY id");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
        Assert.That(reader.GetString(1), Is.EqualTo("first"));
        Assert.That(reader.GetString(2), Is.EqualTo("default_val"));

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
        Assert.That(reader.GetString(1), Is.EqualTo("second"));
        Assert.That(reader.GetString(2), Is.EqualTo("custom_val"));

        Assert.That(reader.Read(), Is.False);
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_NullColumns_ShouldInsertAllColumns()
    {
        var tableName = CreateTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (id UInt64, value String)
            ENGINE = MergeTree() ORDER BY id");

        var bareTableName = TestUtilities.BareTableName(tableName);
        var options = new InsertOptions
        {
            Database = "test",
            UseSchemaCache = true,
        };

        // Insert with null columns — should use all columns from cached SELECT *
        await client.InsertBinaryAsync(
            bareTableName,
            null,
            new List<object[]> { new object[] { 1UL, "hello" }, new object[] { 2UL, "world" } },
            options);

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT id, value FROM {tableName} ORDER BY id");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
        Assert.That(reader.GetString(1), Is.EqualTo("hello"));

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
        Assert.That(reader.GetString(1), Is.EqualTo("world"));

        Assert.That(reader.Read(), Is.False);
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_NullColumns_ShouldPreserveServerColumnOrder()
    {
        // All columns are the same type (String), so a wrong order would silently swap values
        // rather than causing a type error
        var tableName = CreateTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (first String, second String, third String)
            ENGINE = MergeTree() ORDER BY first");

        var bareTableName = TestUtilities.BareTableName(tableName);
        var options = new InsertOptions
        {
            Database = "test",
            UseSchemaCache = true,
        };

        await client.InsertBinaryAsync(
            bareTableName,
            null,
            new List<object[]> { new object[] { "aaa", "bbb", "ccc" } },
            options);

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT first, second, third FROM {tableName}");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("aaa"), "first column");
        Assert.That(reader.GetString(1), Is.EqualTo("bbb"), "second column");
        Assert.That(reader.GetString(2), Is.EqualTo("ccc"), "third column");
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_OptionsDatabaseOverridesClientDatabase()
    {
        // Create a table in the "test" database
        var tableName = CreateTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (id UInt64, value String)
            ENGINE = MergeTree() ORDER BY id");

        // The insert has to receive the unqualified name — a qualified one would resolve the
        // database on its own and the override under test would never be exercised.
        var bareTableName = TestUtilities.BareTableName(tableName);

        // Create a client whose Settings.Database = "default"
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.Database = "default";
        using var clientWithDefaultDb = new ClickHouseClient(new ClickHouseClientSettings(builder));

        var options = new InsertOptions
        {
            // InsertOptions.Database should override the client's "default" database
            Database = "test",
            UseSchemaCache = true,
        };

        await clientWithDefaultDb.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            new List<object[]> { new object[] { 1UL, "overridden" } },
            options);

        var value = await client.ExecuteScalarAsync(
            $"SELECT value FROM {tableName} WHERE id = 1");
        Assert.That(value, Is.EqualTo("overridden"),
            "InsertOptions.Database should override ClickHouseClientSettings.Database");
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_FallsBackToClientDatabase()
    {
        // Create a table in the "test" database
        var tableName = CreateTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (id UInt64, value String)
            ENGINE = MergeTree() ORDER BY id");

        // The insert has to receive the unqualified name, otherwise the fallback under test
        // would be bypassed by the qualification.
        var bareTableName = TestUtilities.BareTableName(tableName);

        // Create a client whose Settings.Database = "test"
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.Database = "test";
        using var clientWithTestDb = new ClickHouseClient(new ClickHouseClientSettings(builder));

        // No Database on InsertOptions — should fall back to client's "test"
        var options = new InsertOptions { UseSchemaCache = true };

        await clientWithTestDb.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            new List<object[]> { new object[] { 1UL, "fallback" } },
            options);

        var value = await client.ExecuteScalarAsync(
            $"SELECT value FROM {tableName} WHERE id = 1");
        Assert.That(value, Is.EqualTo("fallback"),
            "Should fall back to ClickHouseClientSettings.Database when InsertOptions.Database is null");
    }

    [Test]
    public async Task InsertBinaryAsync_DefaultBehavior_ShouldQueryEveryTime()
    {
        var tableName = await CreateSimpleTestTableAsync();
        var bareTableName = TestUtilities.BareTableName(tableName);
        var queryId = $"test_default_{Guid.NewGuid():N}";

        // Two inserts with no schema options
        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            GenerateTestRows(3).ToList(),
            new InsertOptions { Database = "test", QueryId = queryId });

        await client.InsertBinaryAsync(
            bareTableName,
            new[] { "id", "value" },
            GenerateTestRows(3, startId: 100).ToList(),
            new InsertOptions { Database = "test", QueryId = queryId });

        var probeCount = await CountSchemaProbeQueriesAsync(queryId, expectedCount: 2);
        Assert.That(probeCount, Is.EqualTo(2UL),
            "Default behavior should query schema on every insert");
    }
}
