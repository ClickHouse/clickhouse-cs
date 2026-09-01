using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// A user under a readonly profile, which is the one caller the high-level client used to refuse outright. Only a
/// real server can show this: the refusal is the server's (<c>Code: 164 … Cannot modify … in readonly mode</c>),
/// raised because the client sends two <c>output_format_native_*</c> settings whose server defaults are <c>0</c>,
/// which makes every operation a setting modification.
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
    /// The same user with the injection left on, which is what the switch exists to avoid. Pinned so that the
    /// reason the switch exists stays visible, and so a server that one day allows the modification is noticed
    /// here rather than by a caller.
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
    /// Turning the injection off costs exactly one thing, and this is it: a JSON column may arrive in a
    /// serialization this client does not read. The test asserts the cost is paid by JSON alone — ordinary
    /// columns in the same session are unaffected — so anyone weighing the switch can see its price.
    /// <para>
    /// The columns arrive as themselves rather than as one <c>toString</c> expression, and are read through the
    /// typed accessors. A server-computed string would come back through the String decoder alone, so the test
    /// would pass with the DateTime, Array and LowCardinality codecs broken and could not support its name.
    /// </para>
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
