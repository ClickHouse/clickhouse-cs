using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// A user with <c>readonly = 1</c> may send a setting only when its value equals the one already in
/// effect for that user; any other value fails the query with READONLY (code 164). The driver must
/// therefore not put a setting on the URL just to restate a value it assumes.
/// </summary>
[TestFixture]
public class ReadonlyUserTests
{
    private const string StockProfile = "stock profile";
    private const string CompressionDisabledProfile = "enable_http_compression = 0";

    private readonly Dictionary<string, string> users = new();
    private ClickHouseClient admin;

    [OneTimeSetUp]
    public async Task Setup()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Requires local_single_node environment with access storage");
        }

        admin = TestUtilities.GetTestClickHouseClient();

        var guid = Guid.NewGuid().ToString("N");
        users[StockProfile] = $"clickhousecs__readonly_{guid}";
        users[CompressionDisabledProfile] = $"clickhousecs__readonly_nocomp_{guid}";

        await admin.ExecuteNonQueryAsync(
            $"CREATE USER {users[StockProfile]} IDENTIFIED WITH no_password SETTINGS readonly = 1");
        await admin.ExecuteNonQueryAsync(
            $"CREATE USER {users[CompressionDisabledProfile]} IDENTIFIED WITH no_password SETTINGS readonly = 1, enable_http_compression = 0");
    }

    [OneTimeTearDown]
    public async Task Cleanup()
    {
        if (admin == null)
            return;

        try
        {
            foreach (var user in users.Values)
            {
                try
                {
                    await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}");
                }
                catch (Exception e)
                {
                    TestContext.Progress.WriteLine($"Cleanup: failed to drop user {user}: {e.Message}");
                }
            }
        }
        finally
        {
            admin.Dispose();
        }
    }

    // The JSON modes are set to None: that is the documented way to keep the driver from sending the
    // JSON format settings, which a readonly user cannot change either.
    [TestCase(StockProfile, false)]
    [TestCase(StockProfile, true)]
    [TestCase(CompressionDisabledProfile, false)]
    public async Task ExecuteScalarAsync_AsReadonlyUser_Succeeds(string profile, bool compression)
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.Username = users[profile];
        builder.Password = string.Empty;
        builder.Compression = compression;
        builder.JsonReadMode = JsonReadMode.None;
        builder.JsonWriteMode = JsonWriteMode.None;
        using var client = new ClickHouseClient(builder.ConnectionString);

        Assert.That(await client.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }
}
