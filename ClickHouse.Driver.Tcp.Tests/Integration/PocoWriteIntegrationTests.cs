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
/// <c>InsertAsync&lt;T&gt;</c> and the untyped <c>object[]</c> insert against a real server. Per-type coverage rides
/// the round-trip corpus, as the read side's does: each case's insert column names the CLR type a property would
/// hold, so it becomes a <see cref="Row{TValue}"/> insert and a read-back with no corpus of its own. The rest are
/// the shapes the corpus cannot state — a POCO over several columns, the calendar types a property declares instead
/// of the raw wire value, and what a mapping failure does to the connection it happens on.
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

    [TestCaseSource(typeof(InsertRoundTripCase), nameof(InsertRoundTripCase.Cases))]
    public async Task InsertAsync_EveryCorpusType_WritesThePropertyIntoTheColumn(InsertRoundTripCase testCase)
    {
        await using var client = TcpServerFixture.CreateClient();
        var queryOptions = new ClickHouseTcpQueryOptions { Settings = testCase.Settings };
        var insertOptions = new ClickHouseTcpInsertOptions { Settings = testCase.Settings };
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value {testCase.ClickHouseType}) ENGINE = Memory", queryOptions, None);

            IColumn insert = testCase.BuildInsertColumn("value");
            string sql = $"INSERT INTO {table} (value) VALUES";

            // Geometry rows cannot be gathered either, but it fails a step later than Nested and for a different
            // reason, so it needs its own branch. Its codec accepts a column of object like any Variant — an
            // IColumn<object> says nothing about the runtime types it holds — and only the individual value is
            // refused, because four of its six alternatives are structurally identical in pairs. Still before any
            // byte reaches the wire.
            if (testCase.ClickHouseType == "Geometry")
            {
                ArgumentException ambiguous = Assert.ThrowsAsync<ArgumentException>(
                    async () => await InsertColumnAsRowsAsync(client, sql, insertOptions, insert, ElementTypeOf(insert)));

                Assert.That(ambiguous.Message, Does.Contain("more than one alternative"));
                return;
            }

            // A Nested target is the one corpus shape rows cannot be gathered into: its codec writes from its own
            // column type (flat field columns behind shared offsets), which no property can hold. It has to say so
            // rather than fail once the values are on the wire.
            //
            // Contains, not StartsWith: a Nested inside a composite — Array(Nested(...)), Tuple(Nested(...), ...),
            // Map(..., Nested(...)) — cannot be gathered either, for the same reason. Matching only the top level
            // let those corpus cases fall through to the happy path and fail on the refusal this branch expects.
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
    public async Task InsertAsync_PocoOverSeveralColumns_RoundTripsThroughQueryAsync()
    {
        // The POCO-to-POCO round trip: one type inserted and read back through both compiled paths. The property
        // order deliberately differs from the column order, since both directions match by name.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String, score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var written = new[]
            {
                new Account { UserName = "ada", Id = 1, Score = 99.5 },
                new Account { UserName = "grace", Id = 2, Score = null },
            };

            await client.InsertAsync($"INSERT INTO {table} (id, user_name, score) VALUES", written, cancellationToken: None);

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
    public async Task InsertAsync_CalendarAndEnumProperties_WriteThroughTheCodecsConversions()
    {
        // The corpus inserts these columns as their raw wire values; a POCO declares the type a caller would, and
        // the conversion is the codec's own — the same one the columnar path uses for a DateTime column.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = TimeSettings };
        string table = UniqueTableName();
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

                    // The nullable calendar spellings: a Nullable(DateTime) column accepts DateTime? only because
                    // the codec lifts its inner's write types, so a present row and a null row both have to survive.
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

            await client.InsertAsync(
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

    [Test]
    public async Task InsertAsync_ArrayOfDateTimeFromCalendarRows_RoundTripsThroughTheLiftedElements()
    {
        // The lifted write path: the column decodes as uint[], the property is DateTime[], and the inner codec does
        // the conversion as it writes. Only a round-trip shows the write and the read agree on the value.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
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

            await client.InsertAsync($"INSERT INTO {table} (value) VALUES", written, cancellationToken: None);

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

                // The bytes really carry the epoch seconds, not some other reading that happens to round-trip.
                Assert.That(Convert.ToUInt32(raw[1][0]), Is.EqualTo(1_705_314_600u));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_DeeplyNestedCompositeProperties_RoundTripThroughTheLiftedElements()
    {
        // Lifting composes, so the calendar conversion happens at whatever depth the child sits. A round-trip is the
        // only thing that shows the read and write sides agree at depth: either alone would look fine.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
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

            await client.InsertAsync($"INSERT INTO {table} ({columns}) VALUES", written, cancellationToken: None);

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
    public async Task InsertAsync_LowCardinalityOverACalendarInner_RoundTripsFromTheCalendarProperty()
    {
        // The asymmetry this epic closes: the column read into a DateTime property but could be written only from raw
        // epoch seconds. A non-nullable LowCardinality surfaces its inner's element type unchanged, so the dictionary
        // is now built from DateTime values and the inner codec converts them as it writes.
        await using var client = TcpServerFixture.CreateClient();

        // LowCardinality over a fixed-width type is refused by default as a performance foot-gun. It is named here
        // because it is the exact type whose asymmetry this closes, so the setting is enabled rather than the case
        // swapped for one the server likes better.
        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["allow_suspicious_low_cardinality_types"] = "1",
        };
        var options = new ClickHouseTcpQueryOptions { Settings = settings };
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (value LowCardinality(DateTime('UTC'))) ENGINE = Memory",
                options,
                None);

            var repeated = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            var other = new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);

            // The repeat matters: it is what makes the dictionary smaller than the row count, so a wrong dictionary
            // would show up as a wrong value rather than merely a different encoding.
            var written = new[]
            {
                new Row<DateTime> { Value = repeated },
                new Row<DateTime> { Value = other },
                new Row<DateTime> { Value = repeated },
            };

            await client.InsertAsync(
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
    public async Task InsertAsync_NamedColumnSubset_LeavesTheRestToTheirDefaults()
    {
        // A property no target column maps to is simply not inserted, which is what lets one POCO fill part of a
        // table: the statement names the columns, and the server defaults the others.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String DEFAULT 'unset', score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var written = new[] { new Account { Id = 7, UserName = "ada", Score = 1.5 } };
            await client.InsertAsync($"INSERT INTO {table} (id) VALUES", written, cancellationToken: None);

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
    public async Task InsertAsync_TargetColumnWithNoProperty_ThrowsAndLeavesTheClientUsable()
    {
        // The mapping is compiled from the sample block, so this failure lands with the INSERT already open. The
        // insert has to close its row stream with no rows and hand the connection back, or every mapping mistake
        // would cost a redial.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String, score Nullable(Float64), extra String) ENGINE = Memory", cancellationToken: None);

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertAsync($"INSERT INTO {table} VALUES", new[] { new Account { Id = 1 } }, cancellationToken: None));

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
    public async Task InsertAsync_NullPropertyIntoNonNullableColumn_ThrowsNamingTheRowAndInsertsNothing()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new Row<int?> { Value = 1 }, new Row<int?> { Value = null } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertAsync($"INSERT INTO {table} (value) VALUES", rows, cancellationToken: None));

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
    public async Task InsertAsync_NullStringPropertyIntoANonNullableColumn_ThrowsAndLeavesTheClientUsable()
    {
        // The commonest way a row arrives incomplete, and the one that used to be worst: a null reference reaching
        // the codec faults part-way through writing the block, which terminates the connection. It has to fail like
        // any other unwritable value — before anything is sent, naming the row, connection intact.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String, score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new Account { Id = 1, UserName = "ada" }, new Account { Id = 2, UserName = null } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertAsync($"INSERT INTO {table} (id, user_name, score) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("row 1").And.Contain("user_name"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_UntypedNullIntoANonNullableStringColumn_ThrowsAndLeavesTheClientUsable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (name String) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new object[] { "ada" }, new object[] { null } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertAsync($"INSERT INTO {table} (name) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("row 1").And.Contain("name"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_LazyRowSourceAcrossSeveralBlocks_WritesEveryRow()
    {
        // Two things at once: a source with no count, which the row buffer has to grow into, and more rows than one
        // wire block holds, which is the slice path the columns are read through.
        const int rowCount = 5_000;
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            await client.InsertAsync(
                $"INSERT INTO {table} (value) VALUES",
                Counting(rowCount),
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
    public async Task InsertAsync_ZeroRows_IsNoOp()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            await client.InsertAsync($"INSERT INTO {table} (value) VALUES", Array.Empty<Row<int>>(), cancellationToken: None);

            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_AttributedProperties_RenameAndExclude()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (event_time DateTime('UTC')) ENGINE = Memory", cancellationToken: None);

            var written = new[]
            {
                new AttributedRow { Timestamp = new DateTime(2024, 3, 1, 12, 0, 0, DateTimeKind.Utc), Ignored = "not a column" },
            };

            await client.InsertAsync($"INSERT INTO {table} (event_time) VALUES", written, cancellationToken: None);

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
    public async Task InsertAsync_TypeThatCannotBeMaterialized_StillInserts()
    {
        // An insert needs getters and no constructor of ours, so an immutable POCO — the shape a query refuses,
        // since there is nothing to construct — is a perfectly good insert source. The read plan asks the
        // descriptor for its activator first and this one has none; the write plan must never ask.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, user_name String) ENGINE = Memory", cancellationToken: None);

            await client.InsertAsync(
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
    public async Task InsertAsync_ColumnSequenceThatIsNotAList_SaysToUseTheColumnarOverload()
    {
        // The plausible slip: a LINQ operator over the columns yields IEnumerable<IColumn>, which binds to the POCO
        // overload and would otherwise map IColumn's own properties to columns.
        await using var client = TcpServerFixture.CreateClient();
        IEnumerable<IColumn> columns = new List<IColumn> { new ArrayColumn<int>("value", "Int32", new[] { 1 }) };

        ArgumentException error = Assert.ThrowsAsync<ArgumentException>(
            async () => await client.InsertAsync("INSERT INTO nowhere (value) VALUES", columns, cancellationToken: None));

        Assert.That(error.Message, Does.Contain("IReadOnlyList<IColumn>"));
    }

    [Test]
    public async Task InsertAsync_UntypedRows_RoundTripPositionally()
    {
        // The dynamic tier: no type, values matched to the target columns by position. The DateTime column is given
        // a DateTime rather than the raw epoch seconds, which is the spelling a caller writing rows by hand has.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String, stamp DateTime('UTC'), score Nullable(Float64)) ENGINE = Memory", cancellationToken: None);

            var stamp = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
            var rows = new[]
            {
                new object[] { 1UL, "ada", stamp, 99.5 },
                new object[] { 2UL, "grace", stamp.AddHours(1), null },
            };

            await client.InsertAsync($"INSERT INTO {table} (id, name, stamp, score) VALUES", rows, cancellationToken: None);

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
    public async Task InsertAsync_UntypedRowsFromAnUntypedRead_ReinsertWithoutConversion()
    {
        // The property that makes the untyped tier usable as a pipe: what QueryAsync hands out is what InsertAsync
        // takes, raw wire values (a DateTime column's epoch seconds) included.
        await using var client = TcpServerFixture.CreateClient();
        string source = UniqueTableName();
        string copy = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {source} (id UInt64, stamp DateTime('UTC')) ENGINE = Memory", cancellationToken: None);
            await client.ExecuteAsync($"CREATE TABLE {copy} (id UInt64, stamp DateTime('UTC')) ENGINE = Memory", cancellationToken: None);
            await client.ExecuteAsync($"INSERT INTO {source} SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number FROM numbers(3)", cancellationToken: None);

            List<object[]> read = await client.QueryAsync($"SELECT id, stamp FROM {source} ORDER BY id", cancellationToken: None).ToListAsync();
            await client.InsertAsync($"INSERT INTO {copy} (id, stamp) VALUES", read, cancellationToken: None);

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
    public async Task InsertAsync_UntypedRowOfTheWrongLength_ThrowsAndLeavesTheClientUsable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new object[] { 1UL, "ada" }, new object[] { 2UL } };

            ArgumentException error = Assert.ThrowsAsync<ArgumentException>(
                async () => await client.InsertAsync($"INSERT INTO {table} (id, name) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("Row 1"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_UntypedValueOfAWrongType_ThrowsNamingTheColumnAndLeavesTheClientUsable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = Memory", cancellationToken: None);

            var rows = new[] { new object[] { "not a number" } };

            InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
                async () => await client.InsertAsync($"INSERT INTO {table} (id) VALUES", rows, cancellationToken: None));

            Assert.That(error.Message, Does.Contain("id").And.Contain("System.String"));
            Assert.That(await CountAsync(client, table), Is.EqualTo(0));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_BothRowShapes_UsableThroughTheInterface()
    {
        IClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory", cancellationToken: None);

            await client.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { new Row<int> { Value = 1 } }, cancellationToken: None);
            await client.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { new object[] { 2 } }, cancellationToken: None);

            Assert.That(await CountAsync(client, table), Is.EqualTo(2));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
            await client.DisposeAsync();
        }
    }

    /// <summary>
    /// Inserts a one-column corpus case as <c>Row&lt;TValue&gt;</c> rows for a CLR type only known at runtime — the
    /// write mirror of the read fixture's reader, and what lets the corpus drive the POCO write path.
    /// </summary>
    /// <param name="client">The client to insert with.</param>
    /// <param name="sql">The INSERT statement.</param>
    /// <param name="options">The per-insert options the case needs.</param>
    /// <param name="column">The case's insert column, read row by row into the property.</param>
    /// <param name="valueType">The CLR type of the column's values.</param>
    /// <returns>A task that completes when the insert is acknowledged.</returns>
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

        await client.InsertAsync(sql, rows, options, None);
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

    private static string UniqueTableName() => $"tcp_poco_write_test_{Guid.NewGuid():N}";

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

    /// <summary>
    /// Nested composite columns declared in the CLR types a caller would write, rather than the raw wire values their
    /// children decode into.
    /// </summary>
    private sealed class NestedCompositeRow
    {
        public ValueTuple<DateTime, string>[][] Pairs { get; set; }

        public KeyValuePair<string, DateTime[]>[] Buckets { get; set; }

        public ValueTuple<DateTime[], int?> Span { get; set; }

        public DateTime?[] Maybe { get; set; }
    }

    /// <summary>
    /// Getter-only, with no parameterless constructor: insertable, but nothing a query could materialize into.
    /// </summary>
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
}
