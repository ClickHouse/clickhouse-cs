using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

[TestFixture]
public class InsertBinaryPocoTests : AbstractConnectionTestFixture
{
    private string CreateTestTableName([CallerMemberName] string testName = null)
        => SanitizeTableName($"test_poco_{testName}_{Guid.NewGuid():N}");

    private class SimplePoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    private class PocoWithColumnNames
    {
        [ClickHouseColumn(Name = "id")]
        public ulong UserId { get; set; }

        [ClickHouseColumn(Name = "value")]
        public string UserName { get; set; }
    }

    private class PocoWithNotMapped
    {
        public ulong Id { get; set; }
        public string Value { get; set; }

        [ClickHouseNotMapped]
        public string InternalState { get; set; }
    }

    private class PocoWithExplicitTypes
    {
        [ClickHouseColumn(Type = "UInt64")]
        public ulong Id { get; set; }

        [ClickHouseColumn(Type = "String")]
        public string Value { get; set; }
    }

    private class PocoWithPartialTypes
    {
        [ClickHouseColumn(Type = "UInt64")]
        public ulong Id { get; set; }

        // No explicit type — requires schema probe
        public string Value { get; set; }
    }

    private class PocoWithWriteOnlyProperty
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
        public string WriteOnly { set { } }
    }

    private class PocoWithIndexer
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
        public string this[int index] => null;
    }

    private class PocoWithNullable
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
        public int? OptionalScore { get; set; }
    }

    // Properties declared in reverse order of the table columns (Value, Id)
    private class ReversedPropertyOrderPoco
    {
        public string Value { get; set; }
        public ulong Id { get; set; }
    }

    // Declares Value as UInt64 but the property is actually a string — type mismatch at serialization time
    private class PocoWithWrongExplicitType
    {
        [ClickHouseColumn(Type = "UInt64")]
        public ulong Id { get; set; }

        [ClickHouseColumn(Type = "UInt64")]
        public string Value { get; set; }
    }

    // Deliberately never registered — used only by the unregistered type test
    private class UnregisteredPoco
    {
        public ulong Id { get; set; }
    }

    [Test]
    public void InsertBinaryAsync_WithUnregisteredType_ShouldThrow()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await client.InsertBinaryAsync<UnregisteredPoco>("test_table", Array.Empty<UnregisteredPoco>()));

        Assert.That(ex.Message, Does.Contain("UnregisteredPoco"));
        Assert.That(ex.Message, Does.Contain("RegisterBinaryInsertType"));
    }

    [Test]
    public void InsertBinaryAsync_WithNullTable_ShouldThrow()
    {
        client.RegisterBinaryInsertType<SimplePoco>();

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await client.InsertBinaryAsync<SimplePoco>(null, Array.Empty<SimplePoco>()));
    }

    [Test]
    public void InsertBinaryAsync_WithNullRows_ShouldThrow()
    {
        client.RegisterBinaryInsertType<SimplePoco>();

        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await client.InsertBinaryAsync<SimplePoco>("test_table", (IEnumerable<SimplePoco>)null));
    }

    [Test]
    public async Task InsertBinaryAsync_WithWrongExplicitType_ShouldThrowSerializationException()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value UInt64)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithWrongExplicitType>();

            var rows = new[]
            {
                new PocoWithWrongExplicitType { Id = 1, Value = "not_a_number" },
            };

            // The type mismatch (string → UInt64) should fail during serialization
            // and be wrapped in ClickHouseBulkCopySerializationException with row context
            var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
                await client.InsertBinaryAsync(tableName, rows, new InsertOptions { Database = "test" }));

            Assert.That(ex.Row, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Not.Null);
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithPoco_ShouldRoundTripData()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<SimplePoco>();

            var rows = Enumerable.Range(1, 10).Select(i => new SimplePoco
            {
                Id = (ulong)i,
                Value = $"Value_{i}",
            });

            var inserted = await client.InsertBinaryAsync(
                tableName,
                rows,
                new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(10));

            var count = await client.ExecuteScalarAsync(
                $"SELECT count() FROM test.{tableName}");
            Assert.That(count, Is.EqualTo(10UL));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName} ORDER BY Id",
                options: new QueryOptions { Database = "test" });

            var results = new List<(ulong Id, string Value)>();
            while (reader.Read())
            {
                results.Add(((ulong)reader.GetValue(0), (string)reader.GetValue(1)));
            }

            Assert.That(results, Has.Count.EqualTo(10));
            Assert.That(results[0].Id, Is.EqualTo(1UL));
            Assert.That(results[0].Value, Is.EqualTo("Value_1"));
            Assert.That(results[9].Id, Is.EqualTo(10UL));
            Assert.That(results[9].Value, Is.EqualTo("Value_10"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithReversedPropertyOrder_ShouldMapCorrectly()
    {
        var tableName = CreateTestTableName();
        try
        {
            // Table columns: Id first, Value second
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            // POCO declares Value first, Id second — opposite of table order
            client.RegisterBinaryInsertType<ReversedPropertyOrderPoco>();

            var rows = new[]
            {
                new ReversedPropertyOrderPoco { Id = 1, Value = "first" },
                new ReversedPropertyOrderPoco { Id = 2, Value = "second" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(2));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName} ORDER BY Id",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("first"));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(2UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("second"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithColumnNameAttribute_ShouldMapCorrectly()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (id UInt64, value String)
                ENGINE = MergeTree() ORDER BY id");

            client.RegisterBinaryInsertType<PocoWithColumnNames>();

            var rows = new[]
            {
                new PocoWithColumnNames { UserId = 1, UserName = "Alice" },
                new PocoWithColumnNames { UserId = 2, UserName = "Bob" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(2));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT id, value FROM test.{tableName} ORDER BY id",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("Alice"));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(2UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("Bob"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithNotMapped_ShouldExcludeProperty()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithNotMapped>();

            var rows = new[]
            {
                new PocoWithNotMapped { Id = 1, Value = "test", InternalState = "should_be_ignored" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(1));

            var count = await client.ExecuteScalarAsync(
                $"SELECT count() FROM test.{tableName}");
            Assert.That(count, Is.EqualTo(1UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithWriteOnlyProperty_ShouldExcludeIt()
    {
        var tableName = CreateTestTableName();
        try
        {
            // Table includes a WriteOnly column with a DEFAULT — if the property were
            // mistakenly included, the insert would fail or write wrong data.
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String, WriteOnly String DEFAULT 'default_value')
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithWriteOnlyProperty>();

            var rows = new[]
            {
                new PocoWithWriteOnlyProperty { Id = 1, Value = "test", WriteOnly = "ignored" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test", Format = RowBinaryFormat.RowBinaryWithDefaults });

            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value, WriteOnly FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("test"));
            Assert.That(reader.GetValue(2), Is.EqualTo("default_value"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithIndexer_ShouldExcludeIt()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithIndexer>();

            var rows = new[]
            {
                new PocoWithIndexer { Id = 1, Value = "test" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("test"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithExplicitTypes_ShouldSkipSchemaProbe()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithExplicitTypes>();

            var queryId = $"test_poco_explicit_skip_{Guid.NewGuid():N}";
            var rows = new[]
            {
                new PocoWithExplicitTypes { Id = 42, Value = "explicit" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test", QueryId = queryId });

            Assert.That(inserted, Is.EqualTo(1));

            // Verify data round-tripped correctly
            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(42UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("explicit"));

            // Verify no schema probe query was sent
            await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
            var probeCount = await client.ExecuteScalarAsync(
                $"SELECT count() FROM system.query_log " +
                $"WHERE query_id LIKE '{queryId}%' " +
                $"AND query LIKE '%WHERE 1=0%' " +
                $"AND type = 'QueryFinish'");
            Assert.That(probeCount, Is.EqualTo(0UL),
                "No schema probe query should be sent when all properties have explicit types");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithPartialTypes_ShouldProbeForMissing()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithPartialTypes>();

            var rows = new[]
            {
                new PocoWithPartialTypes { Id = 99, Value = "partial" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(99UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("partial"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithNullableProperties_ShouldInsertNulls()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String, OptionalScore Nullable(Int32))
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithNullable>();

            var rows = new[]
            {
                new PocoWithNullable { Id = 1, Value = "with_score", OptionalScore = 100 },
                new PocoWithNullable { Id = 2, Value = "no_score", OptionalScore = null },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(2));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value, OptionalScore FROM test.{tableName} ORDER BY Id",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(2), Is.EqualTo(100));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(2UL));
            Assert.That(reader.IsDBNull(2), Is.True);
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithMultipleBatches_ShouldInsertAll()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<SimplePoco>();

            var rows = Enumerable.Range(1, 500).Select(i => new SimplePoco
            {
                Id = (ulong)i,
                Value = $"Value_{i}",
            });

            var inserted = await client.InsertBinaryAsync(
                tableName,
                rows,
                new InsertOptions { Database = "test", BatchSize = 100 });

            Assert.That(inserted, Is.EqualTo(500));

            var count = await client.ExecuteScalarAsync(
                $"SELECT count() FROM test.{tableName}");
            Assert.That(count, Is.EqualTo(500UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }
}
