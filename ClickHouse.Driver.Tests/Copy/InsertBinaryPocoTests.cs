using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
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

        // No explicit type, requires schema probe
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

    // Internal getter, should be excluded (property visible via public setter, but getter isn't public)
    private class PocoWithInternalGetter
    {
        public ulong Id { get; set; }
        public string Value { internal get; set; }
    }

    // Protected getter
    private class PocoWithProtectedGetter
    {
        public ulong Id { get; set; }
        public string Value { protected get; set; }
    }

    // Private getter
    private class PocoWithPrivateGetter
    {
        public ulong Id { get; set; }
        public string Value { private get; set; }
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

    // Declares Value as UInt64 but the property is actually a string, type mismatch at serialization time
    private class PocoWithWrongExplicitType
    {
        [ClickHouseColumn(Type = "UInt64")]
        public ulong Id { get; set; }

        [ClickHouseColumn(Type = "UInt64")]
        public string Value { get; set; }
    }

    // Deliberately never registered
    private class UnregisteredPoco
    {
        public ulong Id { get; set; }
    }

    private class NoCtorPoco
    {
        public NoCtorPoco(ulong id, string value)
        {
            Id = id;
            Value = value;
        }

        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    // Same shape as NoCtorPoco, but a distinct type to keep the registration-atomicity test
    // isolated from the happy-path insert test in this shared fixture.
    private class AtomicityNoCtorPoco
    {
        public AtomicityNoCtorPoco(ulong id, string value)
        {
            Id = id;
            Value = value;
        }

        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    // Exercises the box-free write fast path (issue #505) for every supported scalar type, plus a
    // nullable value type (Nullable(Int32)) which also fast-paths: the null marker is written directly
    // and a present value recurses into the Int32 fast path, so no boxing occurs for either state.
    private class AllScalarsPoco
    {
        public sbyte I8 { get; set; }
        public short I16 { get; set; }
        public int I32 { get; set; }
        public long I64 { get; set; }
        public byte U8 { get; set; }
        public ushort U16 { get; set; }
        public uint U32 { get; set; }
        public ulong U64 { get; set; }
        public float F32 { get; set; }
        public double F64 { get; set; }
        public bool Flag { get; set; }
        public string Text { get; set; }
        public int? OptionalScore { get; set; }
    }

    [Test]
    public async Task InsertBinaryAsync_AllScalarTypes_RoundTripsViaFastPath()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE IF NOT EXISTS test.{tableName} (" +
                "I8 Int8, I16 Int16, I32 Int32, I64 Int64, " +
                "U8 UInt8, U16 UInt16, U32 UInt32, U64 UInt64, " +
                "F32 Float32, F64 Float64, Flag Bool, Text String, OptionalScore Nullable(Int32)) " +
                "ENGINE = MergeTree() ORDER BY I64");

            client.RegisterBinaryInsertType<AllScalarsPoco>();

            var row = new AllScalarsPoco
            {
                I8 = -42,
                I16 = -12345,
                I32 = -1_234_567_890,
                I64 = -1_234_567_890_123L,
                U8 = 200,
                U16 = 54321,
                U32 = 4_000_000_000u,
                U64 = 18_000_000_000_000_000_000ul,
                F32 = 3.14159f,
                F64 = 2.718281828459045,
                Flag = true,
                Text = "hello ünïcode 🚀",
                OptionalScore = null,
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { row }, new InsertOptions { Database = "test" });
            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT I8, I16, I32, I64, U8, U16, U32, U64, F32, F64, Flag, Text, OptionalScore FROM test.{tableName}");
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<sbyte>(0), Is.EqualTo(row.I8));
            Assert.That(reader.GetFieldValue<short>(1), Is.EqualTo(row.I16));
            Assert.That(reader.GetFieldValue<int>(2), Is.EqualTo(row.I32));
            Assert.That(reader.GetFieldValue<long>(3), Is.EqualTo(row.I64));
            Assert.That(reader.GetFieldValue<byte>(4), Is.EqualTo(row.U8));
            Assert.That(reader.GetFieldValue<ushort>(5), Is.EqualTo(row.U16));
            Assert.That(reader.GetFieldValue<uint>(6), Is.EqualTo(row.U32));
            Assert.That(reader.GetFieldValue<ulong>(7), Is.EqualTo(row.U64));
            Assert.That(reader.GetFieldValue<float>(8), Is.EqualTo(row.F32));
            Assert.That(reader.GetFieldValue<double>(9), Is.EqualTo(row.F64));
            Assert.That(reader.GetFieldValue<bool>(10), Is.EqualTo(row.Flag));
            Assert.That(reader.GetFieldValue<string>(11), Is.EqualTo(row.Text));
            Assert.That(reader.IsDBNull(12), Is.True);
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    // Exercises the Stage 2 box-free write fast path (issue #505) for the bespoke value types
    // (Guid, DateTime family, decimal, BigInteger) plus their Nullable<T> wrappers.
    private class AllValueTypesPoco
    {
        public Guid Uid { get; set; }
        public DateTime Dt { get; set; }
        public DateTime Dt64 { get; set; }
        public DateTime D { get; set; }
        public decimal Dec { get; set; }
        public BigInteger Big { get; set; }
        public Guid? OptionalUid { get; set; }
        public DateTime? OptionalDt { get; set; }
        public decimal? OptionalDec { get; set; }
    }

    [Test]
    public async Task InsertBinaryAsync_AllValueTypes_RoundTripsViaFastPath()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE IF NOT EXISTS test.{tableName} (" +
                "Uid UUID, Dt DateTime, Dt64 DateTime64(3), D Date, Dec Decimal(18, 4), Big Int128, " +
                "OptionalUid Nullable(UUID), OptionalDt Nullable(DateTime), OptionalDec Nullable(Decimal(18, 4))) " +
                "ENGINE = MergeTree() ORDER BY Uid");

            client.RegisterBinaryInsertType<AllValueTypesPoco>();

            var row = new AllValueTypesPoco
            {
                Uid = Guid.Parse("11223344-5566-7788-99aa-bbccddeeff00"),
                Dt = new DateTime(2024, 3, 14, 15, 9, 26, DateTimeKind.Unspecified),
                Dt64 = new DateTime(2024, 3, 14, 15, 9, 26, 123, DateTimeKind.Unspecified),
                D = new DateTime(2024, 3, 14, 0, 0, 0, DateTimeKind.Unspecified),
                Dec = 123456.7891m,
                Big = new BigInteger(123456789012345L),
                OptionalUid = null,
                OptionalDt = new DateTime(2020, 1, 1, 12, 0, 0, DateTimeKind.Unspecified),
                OptionalDec = null,
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { row }, new InsertOptions { Database = "test" });
            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Uid, Dt, Dt64, D, Dec, Big, OptionalUid, OptionalDt, OptionalDec FROM test.{tableName}");
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<Guid>(0), Is.EqualTo(row.Uid));
            Assert.That(reader.GetFieldValue<DateTime>(1), Is.EqualTo(row.Dt));
            Assert.That(reader.GetFieldValue<DateTime>(2), Is.EqualTo(row.Dt64));
            Assert.That(reader.GetFieldValue<DateTime>(3), Is.EqualTo(row.D));
            Assert.That(Convert.ToDecimal(reader.GetValue(4)), Is.EqualTo(row.Dec));
            Assert.That(reader.GetFieldValue<BigInteger>(5), Is.EqualTo(row.Big));
            Assert.That(reader.IsDBNull(6), Is.True);
            Assert.That(reader.GetFieldValue<DateTime>(7), Is.EqualTo(row.OptionalDt.Value));
            Assert.That(reader.IsDBNull(8), Is.True);
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    private enum Priority { Low = 1, High = 9 }

    // Exercises the generous coercion fast path: a property whose CLR type is not the column's exact framework
    // type but is still writeable (int→Int64, DateTimeOffset/DateOnly→DateTime/Date, enum→Int32).
    private class CoercedPoco
    {
        public int WideId { get; set; }
        public DateTimeOffset When { get; set; }
        public Priority Level { get; set; }
        public DateOnly Day { get; set; }
    }

    [Test]
    public async Task InsertBinaryAsync_CoercedValueTypes_RoundTripsViaFastPath()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE IF NOT EXISTS test.{tableName} " +
                "(WideId Int64, When DateTime, Level Int32, Day Date) ENGINE = MergeTree() ORDER BY WideId");

            client.RegisterBinaryInsertType<CoercedPoco>();

            var row = new CoercedPoco
            {
                WideId = 123456,
                When = new DateTimeOffset(2024, 3, 14, 15, 9, 26, TimeSpan.FromHours(2)),
                Level = Priority.High,
                Day = new DateOnly(2024, 3, 14),
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { row }, new InsertOptions { Database = "test" });
            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT WideId, When, Level, Day FROM test.{tableName}");
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<long>(0), Is.EqualTo(row.WideId));
            Assert.That(reader.GetFieldValue<DateTime>(1), Is.EqualTo(row.When.UtcDateTime));
            Assert.That(reader.GetFieldValue<int>(2), Is.EqualTo((int)row.Level));
            Assert.That(reader.GetFieldValue<DateTime>(3).Date, Is.EqualTo(new DateTime(2024, 3, 14)));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
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
    public async Task InsertBinaryAsync_NoParameterlessConstructor_InsertsSuccessfully()
    {
        // RegisterBinaryInsertType<T> does not need to construct instances, so the absence of a public parameterless ctor must not block insert.
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE IF NOT EXISTS test.{tableName} (Id UInt64, Value String) ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<NoCtorPoco>();

            var inserted = await client.InsertBinaryAsync(
                tableName,
                new[] { new NoCtorPoco(7, "kept") },
                new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}");
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(7UL));
            Assert.That(reader.GetFieldValue<string>(1), Is.EqualTo("kept"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public void RegisterPocoType_FailedReadValidation_LeavesInsertUnregistered_InsertBinaryThrows()
    {
        // NoCtorPoco passes insert validation but fails read validation (no public parameterless ctor). Insert shouldn't work.
        Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<AtomicityNoCtorPoco>());

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await client.InsertBinaryAsync<AtomicityNoCtorPoco>(
                "test_table",
                new[] { new AtomicityNoCtorPoco(1, "x") }));

        Assert.That(ex.Message, Does.Contain("AtomicityNoCtorPoco"));
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
    public async Task InsertBinaryAsync_WithInternalGetter_ShouldExcludeProperty()
    {
        var tableName = CreateTestTableName();
        try
        {
            // Table has a Value column with DEFAULT — if the internal-getter property
            // were included, it would either serialize wrong or fail.
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String DEFAULT 'default_val')
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithInternalGetter>();

            var rows = new[]
            {
                new PocoWithInternalGetter { Id = 1, Value = "hidden" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test", Format = RowBinaryFormat.RowBinaryWithDefaults });

            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("default_val"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithProtectedGetter_ShouldExcludeProperty()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String DEFAULT 'default_val')
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithProtectedGetter>();

            var rows = new[]
            {
                new PocoWithProtectedGetter { Id = 1, Value = "hidden" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test", Format = RowBinaryFormat.RowBinaryWithDefaults });

            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("default_val"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithPrivateGetter_ShouldExcludeProperty()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String DEFAULT 'default_val')
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithPrivateGetter>();

            var rows = new[]
            {
                new PocoWithPrivateGetter { Id = 1, Value = "hidden" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test", Format = RowBinaryFormat.RowBinaryWithDefaults });

            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("default_val"));
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
    public async Task InsertBinaryAsync_WithUserProvidedColumnTypes_ShouldTakePrecedenceOverAttributes()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            // PocoWithExplicitTypes has [ClickHouseColumn(Type = "UInt64")] and [ClickHouseColumn(Type = "String")]
            // but we override via InsertOptions.ColumnTypes, these should take precedence
            client.RegisterBinaryInsertType<PocoWithExplicitTypes>();

            var rows = new[]
            {
                new PocoWithExplicitTypes { Id = 1, Value = "overridden" },
            };

            var options = new InsertOptions
            {
                Database = "test",
                ColumnTypes = new Dictionary<string, string>
                {
                    ["Id"] = "UInt64",
                    ["Value"] = "String",
                },
            };

            var inserted = await client.InsertBinaryAsync(tableName, rows, options);
            Assert.That(inserted, Is.EqualTo(1));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value FROM test.{tableName}",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("overridden"));
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
    public async Task InsertBinaryAsync_AfterAlterTableAddsColumn_ShouldInsertSuccessfully()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, Value String)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<SimplePoco>();

            var firstBatch = new[]
            {
                new SimplePoco { Id = 1, Value = "before_alter" },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, firstBatch, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(1));

            // Add a new column with a default value
            await client.ExecuteNonQueryAsync(
                $"ALTER TABLE test.{tableName} ADD COLUMN Extra String DEFAULT 'default_extra'");

            // Insert again with the same POCO (which doesn't have the new column)
            var secondBatch = new[]
            {
                new SimplePoco { Id = 2, Value = "after_alter" },
            };

            inserted = await client.InsertBinaryAsync(
                tableName, secondBatch, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(1));

            // Verify both rows exist and the new column got its default value
            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, Value, Extra FROM test.{tableName} ORDER BY Id",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("before_alter"));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(2UL));
            Assert.That(reader.GetValue(1), Is.EqualTo("after_alter"));
            Assert.That(reader.GetValue(2), Is.EqualTo("default_extra"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    private class PocoWithReservedWords
    {
        public ulong Id { get; set; }

        [ClickHouseColumn(Name = "index")]
        public uint Index { get; set; }

        [ClickHouseColumn(Name = "key")]
        public string Key { get; set; }

        [ClickHouseColumn(Name = "order")]
        public int Order { get; set; }
    }

    [Test]
    public async Task InsertBinaryAsync_WithReservedWordColumns_ShouldRoundTripData()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS test.{tableName}
                (Id UInt64, `index` UInt32, `key` String, `order` Int32)
                ENGINE = MergeTree() ORDER BY Id");

            client.RegisterBinaryInsertType<PocoWithReservedWords>();

            var rows = new[]
            {
                new PocoWithReservedWords { Id = 1, Index = 10, Key = "first", Order = 100 },
                new PocoWithReservedWords { Id = 2, Index = 20, Key = "second", Order = 200 },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, rows, new InsertOptions { Database = "test" });

            Assert.That(inserted, Is.EqualTo(2));

            using var reader = await client.ExecuteReaderAsync(
                $"SELECT Id, `index`, `key`, `order` FROM test.{tableName} ORDER BY Id",
                options: new QueryOptions { Database = "test" });

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetValue(1), Is.EqualTo(10u));
            Assert.That(reader.GetValue(2), Is.EqualTo("first"));
            Assert.That(reader.GetValue(3), Is.EqualTo(100));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(2UL));
            Assert.That(reader.GetValue(1), Is.EqualTo(20u));
            Assert.That(reader.GetValue(2), Is.EqualTo("second"));
            Assert.That(reader.GetValue(3), Is.EqualTo(200));
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
