using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Verifies non-UTC wall-clock conversion against the server's own local and UTC rendering.
/// </summary>
[TestFixture]
[Category("Integration")]
public class TimezoneColumnIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <param name="columnType">The column to create, carrying its own timezone.</param>
    /// <param name="wallClock">The value to write, and the rendering the server must give it in that zone.</param>
    /// <param name="inUtc">The same instant in UTC, which is the offset the client had to apply.</param>
    [TestCase("DateTime('Europe/Amsterdam')", "2024-07-15 14:30:00", "2024-07-15 12:30:00")]
    [TestCase("DateTime('Europe/Amsterdam')", "2024-01-15 14:30:00", "2024-01-15 13:30:00")]
    [TestCase("DateTime('Europe/Amsterdam')", "2024-10-27 02:30:00", "2024-10-27 00:30:00")]
    [TestCase("DateTime64(3, 'America/New_York')", "2024-07-15 14:30:00.000", "2024-07-15 18:30:00.000")]
    [TestCase("DateTime64(3, 'America/New_York')", "2024-01-15 14:30:00.000", "2024-01-15 19:30:00.000")]
    public async Task InsertAsync_UnspecifiedWallClockIntoANonUtcColumn_StoresTheInstantThatZoneNames(
        string columnType,
        string wallClock,
        string inUtc)
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = $"tcp_timezone_test_{Guid.NewGuid():N}";

        await client.ExecuteAsync($"CREATE TABLE {table} (c {columnType}) ENGINE = MergeTree ORDER BY tuple()", cancellationToken: None);
        try
        {
            DateTime value = DateTime.Parse(wallClock, CultureInfo.InvariantCulture, DateTimeStyles.None);
            Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Unspecified), "the whole case is a value that names no offset of its own");

            IColumn[] columns = [new ArrayColumn<DateTime>("c", columnType, [value])];
            await client.InsertAsync($"INSERT INTO {table} (c) VALUES", columns, cancellationToken: None);

            string inZone = null;
            string inUniversal = null;
            await foreach (object[] row in client.QueryAsync(
                $"SELECT toString(c), toString(c, 'UTC') FROM {table}", cancellationToken: None))
            {
                inZone = (string)row[0];
                inUniversal = (string)row[1];
            }

            Assert.Multiple(() =>
            {
                Assert.That(inZone, Is.EqualTo(wallClock), "the server must render back the wall clock that was written");
                Assert.That(inUniversal, Is.EqualTo(inUtc), "and the offset applied has to be the zone's for that date");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// Verifies that UTC values can be written when the column timezone cannot be represented by
    /// <see cref="TimeZoneInfo"/>.
    /// </summary>
    [TestCase("DateTime('Fixed/UTC+19:00:00')", "2024-01-15 10:30:00")]
    [TestCase("DateTime64(3, 'Fixed/UTC+19:00:00')", "2024-01-15 10:30:00.000")]
    public async Task InsertAsync_UtcDateTimeIntoAZoneTimeZoneInfoCannotHold_StoresTheInstant(
        string columnType,
        string inUtc)
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string table = $"tcp_timezone_test_{Guid.NewGuid():N}";

        await client.ExecuteAsync($"CREATE TABLE {table} (c {columnType}) ENGINE = MergeTree ORDER BY tuple()", cancellationToken: None);
        try
        {
            var value = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            IColumn[] columns = [new ArrayColumn<DateTime>("c", columnType, [value])];
            await client.InsertAsync($"INSERT INTO {table} (c) VALUES", columns, cancellationToken: None);

            object stored = null;
            await foreach (object[] row in client.QueryAsync($"SELECT toString(c, 'UTC') FROM {table}", cancellationToken: None))
            {
                stored = row[0];
            }

            Assert.That(stored, Is.EqualTo(inUtc));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }
}
