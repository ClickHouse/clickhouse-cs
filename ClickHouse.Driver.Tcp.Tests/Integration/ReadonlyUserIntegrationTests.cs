using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Verifies queries under a readonly profile with optional serialization-setting injection.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ReadonlyUserIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private string user;

    [OneTimeSetUp]
    public async Task CreateReadonlyUser()
    {
        // Unique, because the framework suites run at once against one server and a user is server-wide.
        user = $"tcp_ro_{Guid.NewGuid():N}";
        await using var admin = TcpServerFixture.CreateClient();
        await admin.ExecuteAsync($"CREATE USER {user} IDENTIFIED WITH no_password SETTINGS readonly = 1", cancellationToken: None);
        await admin.ExecuteAsync($"GRANT SELECT ON *.* TO {user}", cancellationToken: None);
    }

    [OneTimeTearDown]
    public async Task DropReadonlyUser()
    {
        await using var admin = TcpServerFixture.CreateClient();
        await admin.ExecuteAsync($"DROP USER IF EXISTS {user}", cancellationToken: None);
    }

    [Test]
    public async Task QueryAsync_ReadonlyUserWithTheSerializationSettingsOff_ReturnsRows()
    {
        ClickHouseTcpClientOptions options = TcpServerFixture.Options(user, string.Empty)
            with
            { SendJsonAndDynamicSerializationSettings = false };

        await using var client = new ClickHouseTcpClient(options);

        long value = 0;
        await foreach (Block block in client.StreamAsync("SELECT 1", cancellationToken: None))
        {
            value = Convert.ToInt64(block[0].GetValue(0));
        }

        Assert.That(value, Is.EqualTo(1));
    }

    /// <summary>
    /// Verifies that the server rejects injected serialization settings for a readonly user.
    /// </summary>
    [Test]
    public async Task QueryAsync_ReadonlyUserWithTheSerializationSettingsOn_IsRefusedByTheServer()
    {
        await using var client = TcpServerFixture.CreateClient(user, string.Empty);

        var refusal = Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
        {
            await foreach (Block block in client.StreamAsync("SELECT 1", cancellationToken: None))
            {
                _ = block;
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(refusal.Code, Is.EqualTo(ClickHouseErrorCode.ReadOnly));
            Assert.That(refusal.Message, Does.Contain("readonly mode"));
        });
    }

    /// <summary>
    /// Verifies that disabling JSON and Dynamic serialization settings does not affect other typed columns.
    /// </summary>
    [Test]
    public async Task QueryAsync_SerializationSettingsOff_LeavesEveryTypeButJsonAndDynamicWorking()
    {
        ClickHouseTcpClientOptions options = TcpServerFixture.Options(user, string.Empty)
            with
            { SendJsonAndDynamicSerializationSettings = false };

        await using var client = new ClickHouseTcpClient(options);

        DateTimeOffset instant = default;
        int[] numbers = null;
        string label = null;
        decimal amount = 0;
        await foreach (Block block in client.StreamAsync(
            @"SELECT toDateTime('2024-06-15 12:00:00', 'UTC') AS instant,
                     [toInt32(1), toInt32(2)] AS numbers,
                     toLowCardinality('lc') AS label,
                     toDecimal64(1.25, 2) AS amount",
            cancellationToken: None))
        {
            instant = ((IDateTimeColumn)block["instant"]).GetDateTimeOffset(0);
            numbers = (int[])block["numbers"].GetValue(0);
            label = (string)block["label"].GetValue(0);
            amount = (decimal)block["amount"].GetValue(0);
        }

        Assert.Multiple(() =>
        {
            Assert.That(instant, Is.EqualTo(new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero)));
            Assert.That(numbers, Is.EqualTo(new[] { 1, 2 }));
            Assert.That(label, Is.EqualTo("lc"));
            Assert.That(amount, Is.EqualTo(1.25m));
        });
    }
}
