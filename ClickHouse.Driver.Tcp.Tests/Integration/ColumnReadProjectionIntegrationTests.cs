using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Checks <see cref="IColumnCodec.TryProjectRead"/> against a real server: a projection must agree with the instant
/// the server itself means by that value. The unit tests pin the arithmetic against hand-computed constants; these
/// pin it against the server's own timezone and scale handling, which a constant could match only by luck.
/// <para>
/// Every case names its timezone in the type, so no assertion depends on the container's session timezone.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ColumnReadProjectionIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <summary>
    /// Projects one row of a decoded column to <typeparamref name="T"/> through the column's own codec, the way a
    /// compiled POCO scatter will. The value is taken boxed and unboxed inside the expression, so the projection
    /// under test is the same expression tree a plan would inline.
    /// </summary>
    private static T ProjectRow<T>(IColumn column, int row)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(
            column.TypeName,
            new ResolveContext { ServerTimezone = "UTC" });

        ParameterExpression boxed = Expression.Parameter(typeof(object), "boxed");
        Assert.That(
            codec.TryProjectRead(Expression.Convert(boxed, codec.ElementType), typeof(T), out Expression projected),
            Is.True,
            $"the '{column.TypeName}' codec does not project to {typeof(T)}");

        var project = Expression.Lambda<Func<object, T>>(projected, boxed).Compile();

        return project(column.GetValue(row));
    }

    [Test]
    public async Task StreamAsync_DateTimeWithTimezone_ProjectsTheInstantTheServerMeans()
    {
        await using var client = TcpServerFixture.CreateClient();

        uint canonical = 0;
        DateTimeOffset asOffset = default;
        DateTime asDateTime = default;

        await foreach (Block block in client.StreamAsync(
            "SELECT toDateTime(1700000000, 'Europe/Berlin')",
            cancellationToken: None))
        {
            IColumn column = block[0];
            canonical = ((IColumn<uint>)column).Values[0];
            asOffset = ProjectRow<DateTimeOffset>(column, 0);
            asDateTime = ProjectRow<DateTime>(column, 0);
        }

        Assert.Multiple(() =>
        {
            // The canonical read is the raw epoch second count, unchanged by the timezone.
            Assert.That(canonical, Is.EqualTo(1_700_000_000u));

            // 2023-11-14T22:13:20Z is 23:13:20 +01:00 in Berlin (winter, so standard time).
            Assert.That(asOffset.UtcDateTime, Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
            Assert.That(asOffset.Offset, Is.EqualTo(TimeSpan.FromHours(1)));

            // A non-zero offset presents the wall clock in the column's zone, as Unspecified (HTTP-driver parity).
            Assert.That(asDateTime, Is.EqualTo(new DateTime(2023, 11, 14, 23, 13, 20)));
            Assert.That(asDateTime.Kind, Is.EqualTo(DateTimeKind.Unspecified));
        });
    }

    [Test]
    public async Task StreamAsync_DateTimeInUtc_ProjectsToUtcKind()
    {
        await using var client = TcpServerFixture.CreateClient();

        DateTime asDateTime = default;

        await foreach (Block block in client.StreamAsync(
            "SELECT toDateTime(1700000000, 'UTC')",
            cancellationToken: None))
        {
            asDateTime = ProjectRow<DateTime>(block[0], 0);
        }

        Assert.Multiple(() =>
        {
            Assert.That(asDateTime, Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20)));
            Assert.That(asDateTime.Kind, Is.EqualTo(DateTimeKind.Utc));
        });
    }

    /// <summary>
    /// The server accepts and applies fixed offsets .NET cannot represent — on 26.6 both of these are past
    /// <see cref="TimeZoneInfo"/>'s ±14 hours — and it is the server that decides what a header carries. The
    /// seconds are the wire value and need no zone, so the read has to arrive; only a calendar value reports it.
    /// </summary>
    [TestCase("Fixed/UTC+19:00:00", "+19:00:00")]
    [TestCase("Fixed/UTC-18:00:00", "-18:00:00")]
    public async Task StreamAsync_DateTimeOffsetTimeZoneInfoCannotHold_ReadsTheSecondsAndReportsOnlyTheZone(string zone, string offset)
    {
        await using var client = TcpServerFixture.CreateClient();

        uint canonical = 0;
        FormatException fromTimeZone = null;
        FormatException fromProjection = null;

        await foreach (Block block in client.StreamAsync(
            $"SELECT toDateTime(1700000000, '{zone}')",
            cancellationToken: None))
        {
            IColumn column = block[0];
            canonical = ((IColumn<uint>)column).Values[0];
            fromTimeZone = Assert.Throws<FormatException>(() => _ = ((IDateTimeColumn)column).TimeZone);
            fromProjection = Assert.Throws<FormatException>(() => ProjectRow<DateTime>(column, 0));
        }

        Assert.Multiple(() =>
        {
            Assert.That(canonical, Is.EqualTo(1_700_000_000u));
            Assert.That(fromTimeZone?.Message, Does.Contain(zone).And.Contain(offset));
            Assert.That(fromProjection?.Message, Does.Contain(offset));
        });
    }

    /// <summary>
    /// A daylight-saving date, where the offset differs from the zone's standard offset. A projection that used a
    /// fixed base offset instead of resolving it per instant would pass the winter case above and fail here.
    /// </summary>
    [Test]
    public async Task StreamAsync_DateTimeInDaylightSaving_ResolvesTheOffsetForThatInstant()
    {
        await using var client = TcpServerFixture.CreateClient();

        DateTimeOffset asOffset = default;

        await foreach (Block block in client.StreamAsync(
            "SELECT toDateTime(1689000000, 'Europe/Berlin')",
            cancellationToken: None))
        {
            asOffset = ProjectRow<DateTimeOffset>(block[0], 0);
        }

        Assert.Multiple(() =>
        {
            // 2023-07-10T14:40:00Z falls in CEST, so Berlin is +02:00 rather than its standard +01:00.
            Assert.That(asOffset.UtcDateTime, Is.EqualTo(new DateTime(2023, 7, 10, 14, 40, 0, DateTimeKind.Utc)));
            Assert.That(asOffset.Offset, Is.EqualTo(TimeSpan.FromHours(2)));
        });
    }

    [Test]
    [TestCase("fromUnixTimestamp64Milli(1700000000123, 'UTC')", 1_700_000_000_123L, "2023-11-14T22:13:20.1230000Z")]
    [TestCase("fromUnixTimestamp64Nano(1700000000123456789, 'UTC')", 1_700_000_000_123_456_789L, "2023-11-14T22:13:20.1234567Z")]
    public async Task StreamAsync_DateTime64_ProjectsAtTheColumnScale(string expression, long expectedCount, string expectedInstant)
    {
        await using var client = TcpServerFixture.CreateClient();

        long canonical = 0;
        DateTimeOffset asOffset = default;

        await foreach (Block block in client.StreamAsync($"SELECT {expression}", cancellationToken: None))
        {
            IColumn column = block[0];
            canonical = ((IColumn<long>)column).Values[0];
            asOffset = ProjectRow<DateTimeOffset>(column, 0);
        }

        Assert.Multiple(() =>
        {
            // The canonical read keeps the exact wire count, including the nanosecond digits a DateTimeOffset cannot
            // hold; the projection is the lossy calendar view of it (sub-100 ns digits truncate toward zero).
            Assert.That(canonical, Is.EqualTo(expectedCount));
            Assert.That(asOffset.UtcDateTime, Is.EqualTo(DateTimeOffset.Parse(expectedInstant).UtcDateTime));
        });
    }

    [Test]
    public async Task StreamAsync_NullableDateTime_ProjectsValueRowsAndKeepsNulls()
    {
        await using var client = TcpServerFixture.CreateClient();

        var projected = new DateTime?[2];

        await foreach (Block block in client.StreamAsync(
            "SELECT CAST(number = 0 ? NULL : 1700000000, 'Nullable(DateTime(\\'UTC\\'))') FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                projected[row] = ProjectRow<DateTime?>(block[0], row);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(projected[0], Is.Null);
            Assert.That(projected[1], Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
        });
    }

    [Test]
    public async Task StreamAsync_LowCardinalityNullableDateTime_ProjectsThroughBothWrappers()
    {
        await using var client = TcpServerFixture.CreateClient();

        var projected = new DateTimeOffset?[2];

        // The server refuses a LowCardinality over a small fixed-width type unless this is set; it says nothing
        // about how the column decodes, which is what is under test here.
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["allow_suspicious_low_cardinality_types"] = "1" },
        };

        await foreach (Block block in client.StreamAsync(
            "SELECT CAST(number = 0 ? NULL : 1700000000, 'LowCardinality(Nullable(DateTime(\\'UTC\\')))') FROM system.numbers LIMIT 2",
            options,
            None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                projected[row] = ProjectRow<DateTimeOffset?>(block[0], row);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(projected[0], Is.Null);
            Assert.That(projected[1]?.UtcDateTime, Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
        });
    }
}
