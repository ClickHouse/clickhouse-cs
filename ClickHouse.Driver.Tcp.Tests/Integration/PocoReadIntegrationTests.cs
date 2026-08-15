using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// <c>QueryAsync&lt;T&gt;</c> against a real server. Per-type coverage rides the round-trip corpus: each case
/// already knows its ClickHouse type and the CLR type it reads back as, so it becomes a
/// <see cref="Row{TValue}"/> assertion with no corpus of its own. The rest are the shapes the corpus cannot state —
/// the calendar and enum readings a property asks for instead of the raw wire value, the session timezone, the
/// attributes, and rows outliving the blocks they came from.
/// </summary>
[TestFixture]
[Category("Integration")]
public class PocoReadIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly IReadOnlyDictionary<string, string> TimeSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["enable_time_time64_type"] = "1",
        ["allow_experimental_time_time64_type"] = "1",
    };

    [TestCaseSource(typeof(InsertRoundTripCase), nameof(InsertRoundTripCase.Cases))]
    public async Task QueryAsync_EveryCorpusType_MaterializesTheColumnIntoTheProperty(InsertRoundTripCase testCase)
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = testCase.Settings };
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value {testCase.ClickHouseType}) ENGINE = Memory", options, None);

            IColumn insert = testCase.BuildInsertColumn("value");
            await client.InsertAsync(
                $"INSERT INTO {table} (value) VALUES",
                new[] { insert },
                new ClickHouseTcpInsertOptions { Settings = testCase.Settings },
                None);

            IColumn expected = testCase.BuildExpectedColumn("value");
            object[] read = await ReadColumnAsRowsAsync(client, $"SELECT value FROM {table}", options, ElementTypeOf(expected));

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
    public async Task QueryAsync_CalendarAndEnumProperties_ReadTheProjectionsNotTheWireValues()
    {
        // The corpus reads each of these columns as its raw wire value (epoch seconds, a scaled count, an ordinal),
        // which is a different assertion from the one a POCO makes: here every property asks for the reading a
        // caller would actually declare.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = TimeSettings };
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (Stamp DateTime('UTC'), Precise DateTime64(3, 'UTC'), Day Date, Clock Time, Fine Time64(3), Level Enum8('low' = -1, 'high' = 127)) ENGINE = Memory",
                options,
                None);

            var stamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            DateTimeOffset precise = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);
            var day = new DateOnly(2024, 1, 15);
            var clock = new TimeSpan(12, 34, 56);
            var fine = new TimeSpan(0, 1, 2, 3, 456);

            IColumn[] columns =
            {
                new ArrayColumn<DateTime>("Stamp", "DateTime('UTC')", new[] { stamp }),
                new ArrayColumn<DateTimeOffset>("Precise", "DateTime64(3, 'UTC')", new[] { precise }),
                new ArrayColumn<DateOnly>("Day", "Date", new[] { day }),
                new ArrayColumn<TimeSpan>("Clock", "Time", new[] { clock }),
                new ArrayColumn<TimeSpan>("Fine", "Time64(3)", new[] { fine }),
                PrimitiveColumn<sbyte>.FromValues("Level", "Enum8('low' = -1, 'high' = 127)", new sbyte[] { 127 }),
            };

            await client.InsertAsync(
                $"INSERT INTO {table} (Stamp, Precise, Day, Clock, Fine, Level) VALUES",
                columns,
                new ClickHouseTcpInsertOptions { Settings = TimeSettings },
                None);

            List<CalendarRow> rows = await client.QueryAsync<CalendarRow>($"SELECT * FROM {table}", options, None).ToListAsync();

            Assert.That(rows, Has.Count.EqualTo(1));
            Assert.Multiple(() =>
            {
                Assert.That(rows[0].Stamp, Is.EqualTo(stamp));
                Assert.That(rows[0].Stamp.Kind, Is.EqualTo(DateTimeKind.Utc), "a zero offset presents as UTC");
                Assert.That(rows[0].Precise, Is.EqualTo(precise));
                Assert.That(rows[0].Day, Is.EqualTo(day));
                Assert.That(rows[0].Clock, Is.EqualTo(clock));
                Assert.That(rows[0].Fine, Is.EqualTo(fine));
                Assert.That(rows[0].Level, Is.EqualTo(Level.High));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task QueryAsync_TimezoneLessDateTimeColumn_PresentsTheSessionTimezoneWallClock()
    {
        // D7: a bare DateTime column is presented in the session timezone, so a DateTime property carries that wall
        // clock (as Unspecified, there being no offset to attach) while a DateTimeOffset property carries the offset.
        // This is a deliberate difference from the HTTP client, which has no session timezone to resolve.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["session_timezone"] = "Asia/Kolkata" },
        };

        List<InstantRow> rows = await client
            .QueryAsync<InstantRow>("SELECT toDateTime(1700000000) AS Stamp, toDateTime(1700000000) AS Offset", options, None)
            .ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Offset.ToUnixTimeSeconds(), Is.EqualTo(1_700_000_000));
            Assert.That(rows[0].Offset.Offset, Is.EqualTo(new TimeSpan(5, 30, 0)));
            Assert.That(rows[0].Stamp, Is.EqualTo(rows[0].Offset.DateTime));
            Assert.That(rows[0].Stamp.Kind, Is.EqualTo(DateTimeKind.Unspecified));
        });
    }

    [Test]
    public async Task QueryAsync_ResultSpanningSeveralBlocks_KeepsEveryRowValidAfterItsBlock()
    {
        // The rows are accumulated and asserted after the enumeration ends, which is the ownership contract: every
        // value a column surfaces is a copy, so a row outlives the borrowed block it came from. The row count is
        // well past one block, so the plan is also reused across blocks rather than rebuilt.
        const int rowCount = 200_000;
        await using var client = TcpServerFixture.CreateClient();

        List<Numbered> rows = await client
            .QueryAsync<Numbered>($"SELECT number AS Id, toString(number) AS Name FROM numbers({rowCount})", cancellationToken: None)
            .ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(rowCount));
        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Id, Is.EqualTo(0ul));
            Assert.That(rows[0].Name, Is.EqualTo("0"));
            Assert.That(rows[rowCount - 1].Id, Is.EqualTo((ulong)(rowCount - 1)));
            Assert.That(rows[rowCount - 1].Name, Is.EqualTo((rowCount - 1).ToString()));
        });
    }

    [Test]
    public async Task QueryAsync_ColumnWithNoPropertyAndPropertyWithNoColumn_SkipsOneAndDefaultsTheOther()
    {
        await using var client = TcpServerFixture.CreateClient();

        List<Numbered> rows = await client
            .QueryAsync<Numbered>("SELECT toUInt64(7) AS Id, 'ignored' AS Untouched", cancellationToken: None)
            .ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Id, Is.EqualTo(7ul));
            Assert.That(rows[0].Name, Is.Null, "no column maps to Name");
        });
    }

    [Test]
    public async Task QueryAsync_RenamedAndNotMappedProperties_HonorTheAttributes()
    {
        await using var client = TcpServerFixture.CreateClient();

        List<AttributedRow> rows = await client
            .QueryAsync<AttributedRow>("SELECT toDateTime(1700000000, 'UTC') AS event_time, 'x' AS Ignored", cancellationToken: None)
            .ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Timestamp, Is.EqualTo(DateTime.UnixEpoch.AddSeconds(1_700_000_000)));
            Assert.That(rows[0].Ignored, Is.Null, "[ClickHouseTcpNotMapped] keeps the column from reaching the property");
        });
    }

    [Test]
    public async Task QueryAsync_ResultWithNoRows_YieldsNothing()
    {
        // A result with no rows carries no block at all (the connection drops zero-row blocks), so there is no header
        // to compile a plan from: the sequence is simply empty, and nothing about T is validated.
        await using var client = TcpServerFixture.CreateClient();

        List<Numbered> rows = await client
            .QueryAsync<Numbered>("SELECT toUInt64(1) AS Id FROM numbers(1) WHERE 0", cancellationToken: None)
            .ToListAsync();

        Assert.That(rows, Is.Empty);
    }

    [Test]
    public async Task QueryAsync_EnumerationAbandonedEarly_LeavesTheClientUsable()
    {
        // Stopping mid-result has to release both the pooled row array and the connection; the second query is what
        // proves the release happened, since a leaked connection would leave the client unable to run another query.
        await using var client = TcpServerFixture.CreateClient();

        var seen = new List<ulong>();
        await foreach (Numbered row in client.QueryAsync<Numbered>("SELECT number AS Id FROM numbers(200000)", cancellationToken: None))
        {
            seen.Add(row.Id);
            if (seen.Count == 5)
            {
                break;
            }
        }

        List<Numbered> after = await client.QueryAsync<Numbered>("SELECT toUInt64(7) AS Id", cancellationToken: None).ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(new ulong[] { 0, 1, 2, 3, 4 }));
            Assert.That(after[0].Id, Is.EqualTo(7ul));
        });
    }

    [Test]
    public async Task QueryAsync_ColumnNameHoldingThePlanCacheSeparators_MapsToItsProperty()
    {
        // The premise of the plan cache's key: a column name is arbitrary text, tabs and newlines included, so a
        // quoted alias really can spell what a naively joined key uses as its separators.
        await using var client = TcpServerFixture.CreateClient();

        List<SeparatorRow> rows = await client
            .QueryAsync<SeparatorRow>("SELECT toInt32(42) AS `a\tb\nc`", cancellationToken: None)
            .ToListAsync();

        Assert.That(rows[0].Value, Is.EqualTo(42));
    }

    [Test]
    public async Task QueryAsync_PropertyTheColumnCannotBeReadAs_ThrowsBeforeTheFirstRow()
    {
        // The plan is compiled from the result's first block, so the failure arrives on the first MoveNext rather
        // than part-way through the rows.
        await using var client = TcpServerFixture.CreateClient();

        InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await client.QueryAsync<Row<Guid>>("SELECT toInt32(1) AS value", cancellationToken: None).ToListAsync());

        Assert.That(error.Message, Does.Contain("Int32").And.Contain("System.Guid"));
    }

    [Test]
    public async Task QueryAsync_EveryScatterTier_ReadsTheSameRows()
    {
        // The tiers are compared here as well as in the unit tests because these values come off a real server: the
        // span tier's zero-copy read and the two per-row tiers have to agree on decoded storage, not just on columns
        // a test built.
        const string sql = "SELECT toUInt64(number) AS Id, toString(number) AS Name FROM numbers(3)";
        var byTier = new Dictionary<PocoScatterTier, List<Numbered>>();
        foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
        {
            await using var client = new ClickHouseTcpClient(TcpServerFixture.Options()) { ForcedPocoScatterTier = tier };
            byTier[tier] = await client.QueryAsync<Numbered>(sql, cancellationToken: None).ToListAsync();
        }

        Assert.Multiple(() =>
        {
            foreach ((PocoScatterTier tier, List<Numbered> rows) in byTier)
            {
                Assert.That(Array.ConvertAll(rows.ToArray(), row => row.Id), Is.EqualTo(new ulong[] { 0, 1, 2 }), $"{tier}: Id");
                Assert.That(Array.ConvertAll(rows.ToArray(), row => row.Name), Is.EqualTo(new[] { "0", "1", "2" }), $"{tier}: Name");
            }
        });
    }

    /// <summary>
    /// Reads a one-column result into <c>Row&lt;TValue&gt;</c> for a CLR type only known at runtime, which is what
    /// lets the corpus drive the POCO path: each case's expected column names the type its rows come back as.
    /// </summary>
    /// <param name="client">The client to query with.</param>
    /// <param name="sql">The one-column query.</param>
    /// <param name="options">The per-query options the case needs.</param>
    /// <param name="valueType">The CLR type of the column's values.</param>
    /// <returns>Each row's value, in row order.</returns>
    private static Task<object[]> ReadColumnAsRowsAsync(IClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options, Type valueType)
    {
        MethodInfo reader = typeof(PocoReadIntegrationTests)
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

    private static string UniqueTableName() => $"tcp_poco_test_{Guid.NewGuid():N}";

    private enum Level : sbyte
    {
        Low = -1,
        High = 127,
    }

    private sealed class Numbered
    {
        public ulong Id { get; set; }

        public string Name { get; set; }
    }

    private sealed class CalendarRow
    {
        public DateTime Stamp { get; set; }

        public DateTimeOffset Precise { get; set; }

        public DateOnly Day { get; set; }

        public TimeSpan Clock { get; set; }

        public TimeSpan Fine { get; set; }

        public Level Level { get; set; }
    }

    private sealed class InstantRow
    {
        public DateTime Stamp { get; set; }

        public DateTimeOffset Offset { get; set; }
    }

    private sealed class SeparatorRow
    {
        [ClickHouseTcpColumn(Name = "a\tb\nc")]
        public int Value { get; set; }
    }

    private sealed class AttributedRow
    {
        [ClickHouseTcpColumn(Name = "event_time")]
        public DateTime Timestamp { get; set; }

        [ClickHouseTcpNotMapped]
        public string Ignored { get; set; }
    }
}
