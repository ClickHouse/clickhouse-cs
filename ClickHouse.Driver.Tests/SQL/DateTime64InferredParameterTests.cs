using System;
using System.Globalization;
using System.Threading.Tasks;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// Regression tests for the .NET equivalent of clickhouse-go #1483: a parameterized equality filter on
/// a DateTime64 column must match when the user passes a <see cref="DateTime"/>/<see cref="DateTimeOffset"/>
/// parameter WITHOUT an explicit type hint. Before the fix, default inference mapped these to whole-second
/// DateTime, <c>HttpParameterFormatter</c> dropped the sub-second component, and the server compared a
/// truncated value against the stored fractional value — returning zero rows. Default inference now
/// resolves DateTime64(7), which preserves .NET's 100ns (7-digit) sub-second precision losslessly.
/// </summary>
[TestFixture]
public class DateTime64InferredParameterTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ExecuteScalar_DateTime64EqualityFilterWithInferredUtcDateTime_MatchesRow()
    {
        var table = "test." + SanitizeTableName($"dt64_inferred_utc_datetime_{Guid.NewGuid():N}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (id UInt32, dt DateTime64(8, 'UTC')) ENGINE = MergeTree ORDER BY id");
        await connection.ExecuteStatementAsync($"INSERT INTO {table} VALUES (1, '1988-11-20 12:55:28.12345600')");

        // Sub-second .NET DateTime (12:55:28 + 1234560 ticks = .1234560) passed WITHOUT a type hint:
        // the @p placeholder resolves via value-based inference, which must keep the fractional seconds.
        var value = new DateTime(1988, 11, 20, 12, 55, 28, DateTimeKind.Utc).AddTicks(1234560);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id FROM {table} WHERE dt = @p";
        command.AddParameter("p", value);

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1u));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
    }

    [Test]
    public async Task ExecuteScalar_DateTime64EqualityFilterWithInferredDateTimeOffset_MatchesRow()
    {
        var table = "test." + SanitizeTableName($"dt64_inferred_datetimeoffset_{Guid.NewGuid():N}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (id UInt32, dt DateTime64(8, 'UTC')) ENGINE = MergeTree ORDER BY id");
        await connection.ExecuteStatementAsync($"INSERT INTO {table} VALUES (1, '1988-11-20 12:55:28.12345600')");

        // Same sub-second instant as a DateTimeOffset (UTC) — the other value-axis that infers DateTime64(7, 'UTC').
        var value = new DateTimeOffset(1988, 11, 20, 12, 55, 28, TimeSpan.Zero).AddTicks(1234560);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id FROM {table} WHERE dt = @p";
        command.AddParameter("p", value);

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1u));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
    }

    [Test]
    public async Task ExecuteScalar_DateTime64EqualityFilterWithInferredUnspecifiedDateTime_MatchesRow()
    {
        // The third value-axis: an Unspecified DateTime infers tz-less DateTime64(7). Against a tz-less
        // DateTime64 column both the stored literal and the parameter are parsed in the same (session)
        // timezone, so the wall-clock — including the sub-second digits — matches deterministically.
        var table = "test." + SanitizeTableName($"dt64_inferred_unspecified_datetime_{Guid.NewGuid():N}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (id UInt32, dt DateTime64(8)) ENGINE = MergeTree ORDER BY id");
        await connection.ExecuteStatementAsync($"INSERT INTO {table} VALUES (1, '1988-11-20 12:55:28.12345600')");

        var value = new DateTime(1988, 11, 20, 12, 55, 28, DateTimeKind.Unspecified).AddTicks(1234560);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id FROM {table} WHERE dt = @p";
        command.AddParameter("p", value);

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1u));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
    }

    [Test]
    public async Task ExecuteScalar_DateTimeColumnEqualityFilterWithInferredDateTime_StillMatches()
    {
        // Contrast case: a whole-second value filtered against a plain DateTime column must KEEP matching.
        // The inferred DateTime64(7, 'UTC') parameter compares equal to a DateTime column after the server
        // promotes both to a common type — so widening inference to DateTime64 does not regress this path.
        var table = "test." + SanitizeTableName($"dt_inferred_wholesecond_{Guid.NewGuid():N}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (id UInt32, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id");
        await connection.ExecuteStatementAsync($"INSERT INTO {table} VALUES (1, '2024-01-15 12:00:00')");

        var value = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id FROM {table} WHERE dt = @p";
        command.AddParameter("p", value);

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1u));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
    }

    [Test]
    public async Task ExecuteScalar_DateTime64EqualityFilterWithInferredLocalDateTime_MatchesRow()
    {
        // The Local value-axis: a Kind=Local DateTime is instant-bearing, so — like Utc and
        // DateTimeOffset — it infers DateTime64(7, 'UTC') and the formatter renders its UTC instant.
        // Unlike the Utc case (offset 0), Local exercises the TimeZoneInfo.Local conversion in the
        // formatter's new DateTimeOffset(value) step. Deriving the stored literal from
        // value.ToUniversalTime() (scale 7 = .NET's 100ns tick precision) keeps the test correct on
        // any machine's local timezone: the parameter and the stored value denote the same instant
        // regardless of TimeZoneInfo.Local, and the literal precision matches the column scale exactly.
        var table = "test." + SanitizeTableName($"dt64_inferred_local_datetime_{Guid.NewGuid():N}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (id UInt32, dt DateTime64(7, 'UTC')) ENGINE = MergeTree ORDER BY id");

        var value = new DateTime(1988, 11, 20, 12, 55, 28, DateTimeKind.Local).AddTicks(1234560);
        var utcLiteral = value.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        await connection.ExecuteStatementAsync($"INSERT INTO {table} VALUES (1, '{utcLiteral}')");

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id FROM {table} WHERE dt = @p";
        command.AddParameter("p", value);

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1u));

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
    }
}
