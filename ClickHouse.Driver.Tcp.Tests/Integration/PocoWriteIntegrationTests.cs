using System;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Exercises typed and untyped row inserts against a real server, including mapping failures and connection reuse.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PocoWriteIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly IReadOnlyDictionary<string, string> TimeSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["enable_time_time64_type"] = "1",
        ["allow_experimental_time_time64_type"] = "1",
    };

    private static readonly IReadOnlyDictionary<string, string> AmsterdamSessionSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["session_timezone"] = "Europe/Amsterdam",
    };

    [TestCaseSource(typeof(InsertRoundTripCase), nameof(InsertRoundTripCase.Cases))]
    public async Task InsertRowsAsync_EveryCorpusType_WritesThePropertyIntoTheColumn(InsertRoundTripCase testCase)
    {
        await using var client = TcpServerFixture.CreateClient();
        var queryOptions = new ClickHouseTcpQueryOptions { Settings = testCase.Settings };
        var insertOptions = new ClickHouseTcpInsertOptions { Settings = testCase.Settings };
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value {testCase.ClickHouseType}) ENGINE = Memory", queryOptions, None);

            IColumn insert = testCase.BuildInsertColumn("value");
            string sql = $"INSERT INTO {table} (value) VALUES";

            // Nested and composites containing it require a specialized column shape that row gathering cannot build.
            if (testCase.ClickHouseType.Contains("Nested(", StringComparison.Ordinal))
            {
                InvalidOperationException refusal = Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await InsertColumnAsRowsAsync(client, sql, insertOptions, insert, ElementTypeOf(insert)));

                Assert.That(refusal.Message, Does.Contain("columnar API"));
                return;
            }

            await InsertColumnAsRowsAsync(client, sql, insertOptions, insert, ElementTypeOf(insert));

            IColumn expected = testCase.BuildExpectedColumn("value");
            object[] read = await ReadColumnAsRowsAsync(client, $"SELECT value FROM {table}", queryOptions, ElementTypeOf(expected));

            Assert.That(read, Has.Length.EqualTo(expected.RowCount), "row count");
            for (int row = 0; row < expected.RowCount; row++)
            {
                Assert.That(read[row], Is.EqualTo(expected.GetValue(row)), $"row {row}");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_PocoOverSeveralColumns_RoundTripsThroughQueryAsync()
    {
        // Property order differs from column order to verify name-based mapping in both directions.
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String, score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var written = new List<Account>
            {
                new Account { UserName = "ada", Id = 1, Score = 99.5 },
                new Account { UserName = "grace", Id = 2, Score = null },
            };

            await client.InsertRowsAsync($"INSERT INTO {table} (id, user_name, score) VALUES", written, cancellationToken: None);

            List<Account> read = await client.QueryAsync<Account>($"SELECT id, user_name, score FROM {table} ORDER BY id", cancellationToken: None).ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(Array.ConvertAll(read.ToArray(), row => row.Id), Is.EqualTo(new ulong[] { 1, 2 }));
                Assert.That(Array.ConvertAll(read.ToArray(), row => row.UserName), Is.EqualTo(new[] { "ada", "grace" }));
                Assert.That(Array.ConvertAll(read.ToArray(), row => row.Score), Is.EqualTo(new double?[] { 99.5, null }));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_CalendarAndEnumProperties_WriteThroughTheCodecsConversions()
    {
        // Use caller-facing calendar types to exercise the codecs' conversions.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = TimeSettings };
        string table = CreateTableName();
        try
        {
            const string columns = "stamp, precise, day, clock, level, maybe_stamp, maybe_precise";
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (stamp DateTime('UTC'), precise DateTime64(3, 'UTC'), day Date, clock Time, " +
                $"level Enum8('low' = -1, 'high' = 127), maybe_stamp Nullable(DateTime('UTC')), maybe_precise Nullable(DateTime64(3, 'UTC'))) ENGINE = Memory",
                options,
                None);

            var stamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var precise = new DateTimeOffset(2024, 1, 15, 10, 30, 0, 250, TimeSpan.Zero);
            var written = new[]
            {
                new CalendarRow
                {
                    Stamp = stamp,
                    Precise = precise,
                    Day = new DateOnly(2024, 1, 15),
                    Clock = new TimeSpan(10, 30, 0),
                    Level = Level.High,

                    // Exercise both present and null calendar values.
                    MaybeStamp = stamp,
                    MaybePrecise = precise,
                },
                new CalendarRow
                {
                    Stamp = stamp,
                    Precise = precise,
                    Day = new DateOnly(2024, 1, 15),
                    Clock = new TimeSpan(10, 30, 0),
                    Level = Level.Low,
                    MaybeStamp = null,
                    MaybePrecise = null,
                },
            };

            await client.InsertRowsAsync(
                $"INSERT INTO {table} ({columns}) VALUES",
                written,
                new ClickHouseTcpInsertOptions { Settings = TimeSettings },
                None);

            List<CalendarRow> read = await client
                .QueryAsync<CalendarRow>($"SELECT {columns} FROM {table} ORDER BY level", options, None)
                .ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(read[1].Stamp, Is.EqualTo(stamp));
                Assert.That(read[1].Precise, Is.EqualTo(precise));
                Assert.That(read[1].Day, Is.EqualTo(written[0].Day));
                Assert.That(read[1].Clock, Is.EqualTo(written[0].Clock));
                Assert.That(read[1].Level, Is.EqualTo(Level.High));
                Assert.That(read[1].MaybeStamp, Is.EqualTo(stamp));
                Assert.That(read[1].MaybePrecise, Is.EqualTo(precise));
                Assert.That(read[0].MaybeStamp, Is.Null);
                Assert.That(read[0].MaybePrecise, Is.Null);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [TestCase("DateTime")]
    [TestCase("DateTime64(3)")]
    public async Task InsertAsync_CustomGenericColumnArrayWithSessionTimezone_StoresUnspecifiedWallClockInSessionTimezone(string clickHouseType)
    {
        await AssertSessionTimezoneInsertAsync(
            clickHouseType,
            (client, sql, options, value) => client.InsertAsync(
                sql,
                new[] { new ExternalColumn<DateTime>("value", clickHouseType, value) },
                options,
                None).AsTask());
    }

    [TestCase("DateTime")]
    [TestCase("DateTime64(3)")]
    public async Task InsertRowsAsync_PocoWithSessionTimezone_StoresUnspecifiedWallClockInSessionTimezone(string clickHouseType)
    {
        await AssertSessionTimezoneInsertAsync(
            clickHouseType,
            (client, sql, options, value) => client.InsertRowsAsync(
                sql,
                new[] { new Row<DateTime> { Value = value } },
                options,
                None).AsTask());
    }

    [TestCase("DateTime")]
    [TestCase("DateTime64(3)")]
    public async Task InsertRowsAsync_UntypedRowsWithSessionTimezone_StoresUnspecifiedWallClockInSessionTimezone(string clickHouseType)
    {
        await AssertSessionTimezoneInsertAsync(
            clickHouseType,
            (client, sql, options, value) => client.InsertRowsAsync(
                sql,
                new[] { new object[] { value } },
                options,
                None).AsTask());
    }

    [Test]
    public async Task InsertRowsAsync_ArrayOfDateTimeFromCalendarRows_RoundTripsThroughTheLiftedElements()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Array(DateTime('UTC'))) ENGINE = Memory", cancellationToken: None);

            var first = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var second = new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);
            var written = new[]
            {
                new Row<DateTime[]> { Value = new[] { first, second } },
                new Row<DateTime[]> { Value = Array.Empty<DateTime>() },
            };

            await client.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", written, cancellationToken: None);

            List<object[]> raw = await client
                .QueryAsync($"SELECT toUInt32(value[1]), length(value) FROM {table} ORDER BY length(value)", cancellationToken: None)
                .ToListAsync();
            List<Row<DateTime[]>> read = await client
                .QueryAsync<Row<DateTime[]>>($"SELECT value FROM {table} ORDER BY length(value)", cancellationToken: None)
                .ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(read[1].Value, Is.EqualTo(new[] { first, second }));
                Assert.That(read[0].Value, Is.Empty);

                // Also verify the canonical encoded value.
                Assert.That(Convert.ToUInt32(raw[1][0]), Is.EqualTo(1_705_314_600u));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_DeeplyNestedCompositeProperties_RoundTripThroughTheLiftedElements()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            const string columns = "pairs, buckets, span, maybe";
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (" +
                $"pairs Array(Array(Tuple(DateTime('UTC'), String))), " +
                $"buckets Map(String, Array(DateTime('UTC'))), " +
                $"span Tuple(Array(DateTime('UTC')), Nullable(Int32)), " +
                $"maybe Array(Nullable(DateTime('UTC')))) ENGINE = Memory",
                cancellationToken: None);

            var first = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var second = new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);
            var written = new[]
            {
                new NestedCompositeRow
                {
                    Pairs = new[]
                    {
                        new[] { new ValueTuple<DateTime, string>(first, "a"), new ValueTuple<DateTime, string>(second, "b") },
                        Array.Empty<ValueTuple<DateTime, string>>(),
                    },
                    Buckets = new[]
                    {
                        new KeyValuePair<string, DateTime[]>("x", new[] { first, second }),
                        new KeyValuePair<string, DateTime[]>("y", Array.Empty<DateTime>()),
                    },
                    Span = new ValueTuple<DateTime[], int?>(new[] { second }, null),
                    Maybe = new DateTime?[] { first, null, second },
                },
            };

            await client.InsertRowsAsync($"INSERT INTO {table} ({columns}) VALUES", written, cancellationToken: None);

            List<NestedCompositeRow> read = await client
                .QueryAsync<NestedCompositeRow>($"SELECT {columns} FROM {table}", cancellationToken: None)
                .ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(read, Has.Count.EqualTo(1));
                Assert.That(read[0].Pairs[0], Is.EqualTo(written[0].Pairs[0]));
                Assert.That(read[0].Pairs[1], Is.Empty);
                Assert.That(read[0].Buckets, Is.EquivalentTo(written[0].Buckets));
                Assert.That(read[0].Span.Item1, Is.EqualTo(new[] { second }));
                Assert.That(read[0].Span.Item2, Is.Null);
                Assert.That(read[0].Maybe, Is.EqualTo(new DateTime?[] { first, null, second }));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    [RequiresServerFeature(TcpFeature.NullableTuple)]
    public async Task InsertRowsAsync_NullableTupleWithLiftedFields_RoundTripsWhenEnabled()
    {
        await using var client = TcpServerFixture.CreateClient();
        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["enable_nullable_tuple_type"] = "1",
        };
        var queryOptions = new ClickHouseTcpQueryOptions { Settings = settings };
        var insertOptions = new ClickHouseTcpInsertOptions { Settings = settings };
        string table = CreateTableName();
        try
        {
            const string type = "Nullable(Tuple(DateTime('UTC'), String))";
            await client.ExecuteAsync($"CREATE TABLE {table} (value {type}) ENGINE = Memory", queryOptions, None);

            var present = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var written = new[]
            {
                new Row<(DateTime, string)?> { Value = (present, "present") },
                new Row<(DateTime, string)?> { Value = null },
            };
            await client.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", written, insertOptions, None);

            List<Row<(DateTime, string)?>> read = await client
                .QueryAsync<Row<(DateTime, string)?>>($"SELECT value FROM {table} ORDER BY isNull(value)", queryOptions, None)
                .ToListAsync();

            Assert.That(read.Select(row => row.Value), Is.EqualTo(written.Select(row => row.Value)));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", queryOptions, None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_LowCardinalityOverACalendarInner_RoundTripsFromTheCalendarProperty()
    {
        await using var client = TcpServerFixture.CreateClient();

        // ClickHouse requires this setting for LowCardinality(DateTime).
        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["allow_suspicious_low_cardinality_types"] = "1",
        };
        var options = new ClickHouseTcpQueryOptions { Settings = settings };
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (value LowCardinality(DateTime('UTC'))) ENGINE = Memory",
                options,
                None);

            var repeated = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var other = new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);

            var written = new[]
            {
                new Row<DateTime> { Value = repeated },
                new Row<DateTime> { Value = other },
                new Row<DateTime> { Value = repeated },
            };

            await client.InsertRowsAsync(
                $"INSERT INTO {table} (value) VALUES",
                written,
                new ClickHouseTcpInsertOptions { Settings = settings },
                None);

            List<Row<DateTime>> read = await client
                .QueryAsync<Row<DateTime>>($"SELECT value FROM {table} ORDER BY value", options, None)
                .ToListAsync();
            List<object[]> distinct = await client
                .QueryAsync($"SELECT uniqExact(value), toUInt32(min(value)) FROM {table}", options, None)
                .ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(read.Select(r => r.Value), Is.EqualTo(new[] { repeated, other, repeated }.OrderBy(v => v)));
                Assert.That(Convert.ToUInt64(distinct[0][0]), Is.EqualTo(2ul), "two distinct values, so the dictionary really deduplicated");
                Assert.That(Convert.ToUInt32(distinct[0][1]), Is.EqualTo(1_705_314_600u), "the wire carries epoch seconds");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_NullableLowCardinalityDateTimesWithEqualClrTicks_StoresDistinctInstants()
    {
        await using var client = TcpServerFixture.CreateClient();
        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["allow_suspicious_low_cardinality_types"] = "1",
        };
        var queryOptions = new ClickHouseTcpQueryOptions { Settings = settings };
        var insertOptions = new ClickHouseTcpInsertOptions { Settings = settings };
        string table = CreateTableName();
        try
        {
            const string type = "LowCardinality(Nullable(DateTime('America/New_York')))";
            await client.ExecuteAsync($"CREATE TABLE {table} (value {type}) ENGINE = Memory", queryOptions, None);

            long ticks = new DateTime(2024, 1, 15, 12, 0, 0).Ticks;
            var written = new[]
            {
                new Row<DateTime?> { Value = new DateTime(ticks, DateTimeKind.Utc) },
                new Row<DateTime?> { Value = null },
                new Row<DateTime?> { Value = new DateTime(ticks, DateTimeKind.Unspecified) },
            };
            await client.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", written, insertOptions, None);

            List<object[]> encoded = await client
                .QueryAsync($"SELECT toUInt32(value) FROM {table} WHERE value IS NOT NULL ORDER BY value", queryOptions, None)
                .ToListAsync();

            Assert.That(encoded.Select(row => Convert.ToUInt32(row[0])).Distinct().ToArray(), Has.Length.EqualTo(2));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_NamedColumnSubset_LeavesTheRestToTheirDefaults()
    {
        // The INSERT column list controls which properties are used.
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String DEFAULT 'unset', score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var written = new[] { new Account { Id = 7, UserName = "ada", Score = 1.5 } };
            await client.InsertRowsAsync($"INSERT INTO {table} (id) VALUES", written, cancellationToken: None);

            List<Account> read = await client.QueryAsync<Account>($"SELECT id, user_name, score FROM {table}", cancellationToken: None).ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(read[0].Id, Is.EqualTo(7UL));
                Assert.That(read[0].UserName, Is.EqualTo("unset"));
                Assert.That(read[0].Score, Is.Null);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_TargetColumnWithNoProperty_ThrowsAndLeavesTheClientUsable()
    {
        // A mapping failure after the sample block must leave the connection reusable.
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String, score Nullable(Float64), extra String) ENGINE = Memory", cancellationToken: None);

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} VALUES", new[] { new Account { Id = 1 } }, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("extra").And.Contain("Account"));

            // The connection survived: the same client runs the next statement.
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_NullPropertyIntoNonNullableColumn_ThrowsNamingTheRowAndInsertsNothing()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new Row<int?> { Value = 1 }, new Row<int?> { Value = null } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("row 1").And.Contain("value"));

            // The gather runs before any row goes out, so the failing insert is all-or-nothing.
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_NullStringPropertyIntoANonNullableColumn_ThrowsAndLeavesTheClientUsable()
    {
        // Reject the null before writing so the insert remains atomic and the connection reusable.
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String, score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new Account { Id = 1, UserName = "ada" }, new Account { Id = 2, UserName = null } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} (id, user_name, score) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("row 1").And.Contain("user_name"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_UntypedNullIntoANonNullableStringColumn_ThrowsAndLeavesTheClientUsable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (name String) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new object[] { "ada" }, new object[] { null } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} (name) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("row 1").And.Contain("name"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_MaterializedListAcrossSeveralBlocks_WritesEveryRow()
    {
        // Exercise list normalization and multi-block slicing together.
        const int rowCount = 5_000;
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            await client.InsertRowsAsync(
                $"INSERT INTO {table} (value) VALUES",
                new List<Row<int>>(Counting(rowCount)),
                new ClickHouseTcpInsertOptions { MaxRowsPerBlock = 1_000 },
                None);

                List<object[]> aggregates = await client.QueryAsync($"SELECT count(), sum(value) FROM {table}", cancellationToken: None).ToListAsync();
            object[] countAndSum = aggregates[0];

            Assert.Multiple(() =>
            {
                Assert.That(countAndSum[0], Is.EqualTo((ulong)rowCount));
                Assert.That(countAndSum[1], Is.EqualTo((long)rowCount * (rowCount - 1) / 2));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_ZeroRows_IsNoOp()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            await client.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", Array.Empty<Row<int>>(), cancellationToken: None);

            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_AttributedProperties_RenameAndExclude()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (event_time DateTime('UTC')) ENGINE = Memory", cancellationToken: None);

            var written = new[]
            {
                new AttributedRow { Timestamp = new DateTime(2024, 3, 1, 12, 0, 0, DateTimeKind.Utc), Ignored = "not a column" },
            };

            await client.InsertRowsAsync($"INSERT INTO {table} (event_time) VALUES", written, cancellationToken: None);

            List<AttributedRow> read = await client.QueryAsync<AttributedRow>($"SELECT event_time FROM {table}", cancellationToken: None).ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(read[0].Timestamp, Is.EqualTo(written[0].Timestamp));
                Assert.That(read[0].Ignored, Is.Null);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_TypeThatCannotBeMaterialized_StillInserts()
    {
        // Writes need getters but do not need a constructor.
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String) ENGINE = Memory", cancellationToken: None);

            await client.InsertRowsAsync(
                $"INSERT INTO {table} (id, user_name) VALUES",
                new[] { new ImmutableAccount(3, "ada") },
                cancellationToken: None);

            List<object[]> read = await client.QueryAsync($"SELECT id, user_name FROM {table}", cancellationToken: None).ToListAsync();

            Assert.That(read[0], Is.EqualTo(new object[] { 3UL, "ada" }));

            // The other half of the contract: the same type cannot be queried into, and says why.
            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.QueryAsync<ImmutableAccount>($"SELECT id, user_name FROM {table}", cancellationToken: None).ToListAsync());

            Assert.That(error.Message, Does.Contain("no public parameterless constructor"));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_ColumnList_SaysToUseInsertAsync()
    {
        // A column list should direct the caller to InsertAsync.
        await using var client = TcpServerFixture.CreateClient();
        IReadOnlyList<IColumn> columns = new List<IColumn> { new ArrayColumn<int>("value", "Int32", new[] { 1 }) };

        ArgumentException error = Assert.ThrowsAsync<ArgumentException>(
            async () => await client.InsertRowsAsync("INSERT INTO nowhere (value) VALUES", columns, cancellationToken: None));

        Assert.That(error.Message, Does.Contain("IReadOnlyList<IColumn>"));
    }

    [Test]
    public async Task InsertRowsAsync_UntypedRows_RoundTripPositionally()
    {
        // Use a caller-facing DateTime to verify positional conversion.
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String, stamp DateTime('UTC'), score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var stamp = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
            var rows = new[]
            {
                new object[] { 1UL, "ada", stamp, 99.5 },
                new object[] { 2UL, "grace", stamp.AddHours(1), null },
            };

            await client.InsertRowsAsync($"INSERT INTO {table} (id, name, stamp, score) VALUES", rows, cancellationToken: None);

            List<object[]> read = await client
                .QueryAsync($"SELECT id, name, stamp, score FROM {table} ORDER BY id", cancellationToken: None)
                .ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(read[0][0], Is.EqualTo(1UL));
                Assert.That(read[0][1], Is.EqualTo("ada"));
                Assert.That(read[0][2], Is.EqualTo((uint)new DateTimeOffset(stamp).ToUnixTimeSeconds()));
                Assert.That(read[0][3], Is.EqualTo(99.5));
                Assert.That(read[1][3], Is.Null);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_UntypedRowsFromAnUntypedRead_ReinsertWithoutConversion()
    {
        // Verify that untyped query rows can be inserted without reshaping their values.
        await using var client = TcpServerFixture.CreateClient();
        string source = CreateTableName();
        string copy = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {source} (id UInt64, stamp DateTime('UTC')) ENGINE = Memory", cancellationToken: None);
            await client.ExecuteAsync($"CREATE TABLE {copy} (id UInt64, stamp DateTime('UTC')) ENGINE = Memory", cancellationToken: None);
            await client.ExecuteAsync($"INSERT INTO {source} SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number FROM numbers(3)", cancellationToken: None);

            List<object[]> read = await client.QueryAsync($"SELECT id, stamp FROM {source} ORDER BY id", cancellationToken: None).ToListAsync();
            await client.InsertRowsAsync($"INSERT INTO {copy} (id, stamp) VALUES", read, cancellationToken: None);

            List<object[]> reread = await client.QueryAsync($"SELECT id, stamp FROM {copy} ORDER BY id", cancellationToken: None).ToListAsync();

            Assert.That(reread, Has.Count.EqualTo(3));
            for (int row = 0; row < reread.Count; row++)
            {
                Assert.That(reread[row], Is.EqualTo(read[row]), $"row {row}");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {source}", cancellationToken: None);
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {copy}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_UntypedRowOfTheWrongLength_ThrowsAndLeavesTheClientUsable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new object[] { 1UL, "ada" }, new object[] { 2UL } };

            ArgumentException error = Assert.ThrowsAsync<ArgumentException>(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} (id, name) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("Row 1"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_UntypedValueOfAWrongType_ThrowsNamingTheColumnAndLeavesTheClientUsable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new object[] { "not a number" } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} (id) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("id").And.Contain("System.String"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertRowsAsync_BothRowShapes_UsableThroughTheInterface()
    {
        IClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            await client.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", new[] { new Row<int> { Value = 1 } }, cancellationToken: None);
            await client.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", new[] { new object[] { 2 } }, cancellationToken: None);

            Assert.That(await CountAsync(client, table), Is.EqualTo(2));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
            await client.DisposeAsync();
        }
    }

    /// <summary>
    /// Invokes the generic row insert for a column type known only at runtime.
    /// </summary>
    private static Task InsertColumnAsRowsAsync(IClickHouseTcpClient client, string sql, ClickHouseTcpInsertOptions options, IColumn column, Type valueType)
    {
        MethodInfo writer = typeof(PocoWriteIntegrationTests)
            .GetMethod(nameof(InsertRowsAsync), BindingFlags.NonPublic | BindingFlags.Static)
            .MakeGenericMethod(valueType);

        return (Task)writer.Invoke(null, new object[] { client, sql, options, column });
    }

    private static async Task InsertRowsAsync<TValue>(IClickHouseTcpClient client, string sql, ClickHouseTcpInsertOptions options, IColumn column)
    {
        var typed = (IColumn<TValue>)column;
        var rows = new Row<TValue>[column.RowCount];
        for (int row = 0; row < rows.Length; row++)
        {
            rows[row] = new Row<TValue> { Value = typed[row] };
        }

        await client.InsertRowsAsync(sql, rows, options, None);
    }

    private static Task<object[]> ReadColumnAsRowsAsync(IClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options, Type valueType)
    {
        MethodInfo reader = typeof(PocoWriteIntegrationTests)
            .GetMethod(nameof(ReadRowsAsync), BindingFlags.NonPublic | BindingFlags.Static)
            .MakeGenericMethod(valueType);

        return (Task<object[]>)reader.Invoke(null, new object[] { client, sql, options });
    }

    private static async Task<object[]> ReadRowsAsync<TValue>(IClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options)
    {
        var values = new List<object>();
        await foreach (Row<TValue> row in client.QueryAsync<Row<TValue>>(sql, options, None))
        {
            values.Add(row.Value);
        }

        return values.ToArray();
    }

    /// <summary>A source with no count, so the row buffer has to grow rather than rent once.</summary>
    /// <param name="count">How many rows to yield.</param>
    /// <returns>The rows, one at a time.</returns>
    private static IEnumerable<Row<int>> Counting(int count)
    {
        for (int value = 0; value < count; value++)
        {
            yield return new Row<int> { Value = value };
        }
    }

    private static async Task<int> CountAsync(IClickHouseTcpClient client, string table)
    {
        List<object[]> rows = await client.QueryAsync($"SELECT count() FROM {table}", cancellationToken: None).ToListAsync();
        return (int)(ulong)rows[0][0];
    }

    private static async Task AssertSessionTimezoneInsertAsync(
        string clickHouseType,
        Func<IClickHouseTcpClient, string, ClickHouseTcpInsertOptions, DateTime, Task> insert)
    {
        await using IClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = CreateTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value {clickHouseType}) ENGINE = Memory", cancellationToken: None);

            var wallClock = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified);
            var options = new ClickHouseTcpInsertOptions { Settings = AmsterdamSessionSettings };
            await insert(client, $"INSERT INTO {table} (value) VALUES", options, wallClock);

            List<object[]> read = await client.QueryAsync($"SELECT value FROM {table}", cancellationToken: None).ToListAsync();
            var expectedInstant = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(1));
            object expected = clickHouseType == "DateTime"
                ? (uint)expectedInstant.ToUnixTimeSeconds()
                : expectedInstant.ToUnixTimeMilliseconds();

            Assert.That(read[0][0], Is.EqualTo(expected));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>The <c>T</c> of the <see cref="IColumn{T}"/> a column surfaces.</summary>
    /// <param name="column">The column.</param>
    /// <returns>Its CLR element type.</returns>
    private static Type ElementTypeOf(IColumn column)
    {
        foreach (Type implemented in column.GetType().GetInterfaces())
        {
            if (implemented.IsGenericType && implemented.GetGenericTypeDefinition() == typeof(IColumn<>))
            {
                return implemented.GetGenericArguments()[0];
            }
        }

        throw new InvalidOperationException($"Column '{column.Name}' ({column.TypeName}) surfaces no IColumn<T>.");
    }

    private static string CreateTableName() => $"tcp_poco_write_test_{Guid.NewGuid():N}";

    private enum Level : sbyte
    {
        Low = -1,
        High = 127,
    }

    private sealed class Account
    {
        public string UserName { get; set; }

        public ulong Id { get; set; }

        public double? Score { get; set; }
    }

    private sealed class CalendarRow
    {
        public DateTime Stamp { get; set; }

        public DateTimeOffset Precise { get; set; }

        public DateOnly Day { get; set; }

        public TimeSpan Clock { get; set; }

        public Level Level { get; set; }

        public DateTime? MaybeStamp { get; set; }

        public DateTimeOffset? MaybePrecise { get; set; }
    }

    private sealed class NestedCompositeRow
    {
        public ValueTuple<DateTime, string>[][] Pairs { get; set; }

        public KeyValuePair<string, DateTime[]>[] Buckets { get; set; }

        public ValueTuple<DateTime[], int?> Span { get; set; }

        public DateTime?[] Maybe { get; set; }
    }

    /// <summary>A getter-only type that can be inserted but not materialized.</summary>
    private sealed class ImmutableAccount
    {
        public ImmutableAccount(ulong id, string userName)
        {
            Id = id;
            UserName = userName;
        }

        public ulong Id { get; }

        public string UserName { get; }
    }

    private sealed class AttributedRow
    {
        [ClickHouseTcpColumn(Name = "event_time")]
        public DateTime Timestamp { get; set; }

        [ClickHouseTcpNotMapped]
        public string Ignored { get; set; }
    }

    /// <summary>
    /// A public-only column used to exercise overload resolution from a caller's perspective.
    /// </summary>
    private sealed class ExternalColumn<T> : IColumn<T>
    {
        private readonly T[] values;

        public ExternalColumn(string name, string typeName, params T[] values)
        {
            Name = name;
            TypeName = typeName;
            this.values = values;
        }

        public string Name { get; }

        public string TypeName { get; }

        public int RowCount => values.Length;

        public ReadOnlySpan<T> Values => values;

        public T this[int row] => values[row];

        public object GetValue(int row) => values[row];

        public void Dispose()
        {
        }
    }
}
