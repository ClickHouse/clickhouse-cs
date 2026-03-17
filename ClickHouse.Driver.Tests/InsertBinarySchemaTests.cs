using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public class InsertBinarySchemaTests : AbstractConnectionTestFixture
{
    private string CreateTestTableName([CallerMemberName] string testName = null)
        => SanitizeTableName($"test_schema_{testName}_{Guid.NewGuid():N}");

    private async Task<string> CreateSimpleTestTableAsync([CallerMemberName] string testName = null)
    {
        var tableName = CreateTestTableName(testName);
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS test.{tableName}
            (id UInt64, value String)
            ENGINE = MergeTree() ORDER BY id");
        return tableName;
    }

    private static IEnumerable<object[]> GenerateTestRows(int count, ulong startId = 1)
    {
        for (ulong i = 0; i < (ulong)count; i++)
            yield return new object[] { startId + i, $"Value_{startId + i}" };
    }

    private async Task<ulong> CountSchemaProbeQueriesAsync(string queryIdPrefix)
    {
        await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
        var count = await client.ExecuteScalarAsync(
            $"SELECT count() FROM system.query_log " +
            $"WHERE query_id LIKE '{queryIdPrefix}%' " +
            $"AND query LIKE '%WHERE 1=0%' " +
            $"AND type = 'QueryFinish'");
        return (ulong)count;
    }

    [Test]
    public async Task InsertBinaryAsync_WithColumnTypes_ShouldSkipSchemaQuery()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
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
                tableName,
                new[] { "id", "value" },
                GenerateTestRows(5).ToList(),
                options);

            var probeCount = await CountSchemaProbeQueriesAsync(queryId);
            Assert.That(probeCount, Is.EqualTo(0UL),
                "No schema probe query should be sent when ColumnTypes is provided");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithColumnTypes_ShouldRoundTripData()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
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
                tableName,
                new[] { "id", "value" },
                new List<object[]>
                {
                    new object[] { 1UL, "hello" },
                    new object[] { 2UL, "world" },
                },
                options);

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT id, value FROM test.{tableName} ORDER BY id");

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
            Assert.That(reader.GetString(1), Is.EqualTo("hello"));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
            Assert.That(reader.GetString(1), Is.EqualTo("world"));

            Assert.That(reader.Read(), Is.False);
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
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
        try
        {
            var queryId = $"test_cache_once_{Guid.NewGuid():N}";
            var options = new InsertOptions
            {
                Database = "test",
                QueryId = queryId,
                UseSchemaCache = true,
            };

            // First insert — should trigger schema probe
            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "value" },
                GenerateTestRows(3).ToList(),
                options);

            // Second insert — should reuse cached schema
            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "value" },
                GenerateTestRows(3, startId: 100).ToList(),
                options);

            var probeCount = await CountSchemaProbeQueriesAsync(queryId);
            Assert.That(probeCount, Is.EqualTo(1UL),
                "Only one schema probe query should be sent when UseSchemaCache is true");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_DifferentColumns_ShouldQueryOnce()
    {
        var tableName = CreateTestTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS test.{tableName}
            (id UInt64, value String, extra String DEFAULT 'x')
            ENGINE = MergeTree() ORDER BY id");
        try
        {
            var queryId = $"test_cache_diffcols_{Guid.NewGuid():N}";
            var options = new InsertOptions
            {
                Database = "test",
                QueryId = queryId,
                UseSchemaCache = true,
            };

            // Insert with columns [id, value]
            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "value" },
                new List<object[]> { new object[] { 1UL, "a" } },
                options);

            // Insert with columns [id, extra] — same table, should reuse cached schema
            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "extra" },
                new List<object[]> { new object[] { 2UL, "b" } },
                options);

            var probeCount = await CountSchemaProbeQueriesAsync(queryId);
            Assert.That(probeCount, Is.EqualTo(1UL),
                "Cache is per-table — different column subsets should share the same cached schema");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithColumnTypesAndSchemaCache_ShouldPreferColumnTypes()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
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
                tableName,
                new[] { "id", "value" },
                GenerateTestRows(3).ToList(),
                options);

            var probeCount = await CountSchemaProbeQueriesAsync(queryId);
            Assert.That(probeCount, Is.EqualTo(0UL),
                "ColumnTypes should take priority over UseSchemaCache — no schema query expected");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithSchemaCache_SubsetThenSuperset_ShouldRoundTripData()
    {
        var tableName = CreateTestTableName();
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS test.{tableName}
            (id UInt64, value String, extra String DEFAULT 'default_val')
            ENGINE = MergeTree() ORDER BY id");
        try
        {
            var options = new InsertOptions
            {
                Database = "test",
                UseSchemaCache = true,
            };

            // First insert uses only [id, value] — cache should be populated with SELECT *
            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "value" },
                new List<object[]> { new object[] { 1UL, "first" } },
                options);

            // Second insert uses [id, value, extra] — must work from the same cached schema
            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "value", "extra" },
                new List<object[]> { new object[] { 2UL, "second", "custom_val" } },
                options);

            // Verify both rows round-tripped correctly
            using var reader = await client.ExecuteReaderAsync(
                $"SELECT id, value, extra FROM test.{tableName} ORDER BY id");

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
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_DefaultBehavior_ShouldQueryEveryTime()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
            var queryId = $"test_default_{Guid.NewGuid():N}";

            // Two inserts with no schema options
            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "value" },
                GenerateTestRows(3).ToList(),
                new InsertOptions { Database = "test", QueryId = queryId });

            await client.InsertBinaryAsync(
                tableName,
                new[] { "id", "value" },
                GenerateTestRows(3, startId: 100).ToList(),
                new InsertOptions { Database = "test", QueryId = queryId });

            var probeCount = await CountSchemaProbeQueriesAsync(queryId);
            Assert.That(probeCount, Is.EqualTo(2UL),
                "Default behavior should query schema on every insert");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }
}
