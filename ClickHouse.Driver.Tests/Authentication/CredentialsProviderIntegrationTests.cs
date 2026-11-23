using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Authentication;

/// <summary>
/// <see cref="ClickHouseClientSettings.CredentialsProvider"/> against a real server, with a password that
/// rotates. The JWT flow needs ClickHouse Cloud: see <c>BearerAuthenticationTests</c>.
/// </summary>
public class CredentialsProviderIntegrationTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ExecuteScalarAsync_PasswordRotatedOnServer_SendsOnceMoreWithPasswordFromProvider()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Requires local_single_node environment with access storage");
        }

        var userName = $"test_rotation_{Guid.NewGuid():N}"[..30];
        var oldPassword = $"Old_{Guid.NewGuid():N}";
        var newPassword = $"New_{Guid.NewGuid():N}";
        await client.ExecuteNonQueryAsync($"CREATE USER {userName} IDENTIFIED BY '{oldPassword}'");
        try
        {
            // A provider that keeps its own cache, as one in front of a secret store does: it reads the
            // store again only when the client says the server rejected the password.
            var store = oldPassword;
            var known = oldPassword;
            var reasons = new ConcurrentQueue<ClickHouseCredentialsRefreshReason>();
            var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
            {
                CredentialsProvider = (request, _) =>
                {
                    reasons.Enqueue(request.Reason);
                    if (request.Reason == ClickHouseCredentialsRefreshReason.Rejected)
                    {
                        known = store;
                    }

                    return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBasic(userName, known));
                },
            };
            using var rotatingClient = new ClickHouseClient(settings);

            var before = await rotatingClient.ExecuteScalarAsync("SELECT currentUser()");
            await client.ExecuteNonQueryAsync($"ALTER USER {userName} IDENTIFIED BY '{newPassword}'");
            store = newPassword;
            var after = await rotatingClient.ExecuteScalarAsync("SELECT currentUser()");

            Assert.Multiple(() =>
            {
                Assert.That(before, Is.EqualTo(userName));
                Assert.That(after, Is.EqualTo(userName));
                Assert.That(reasons.ToArray(), Is.EqualTo(new[]
                {
                    ClickHouseCredentialsRefreshReason.Initial,
                    ClickHouseCredentialsRefreshReason.Expiring,
                    ClickHouseCredentialsRefreshReason.Rejected,
                }));
            });
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WrongPasswordFromProvider_ThrowsAuthenticationFailedAfterOneRetry()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Requires local_single_node environment with access storage");
        }

        var userName = $"test_rotation_{Guid.NewGuid():N}"[..30];
        await client.ExecuteNonQueryAsync($"CREATE USER {userName} IDENTIFIED BY 'Right_{Guid.NewGuid():N}'");
        try
        {
            var calls = 0;
            var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
            {
                CredentialsProvider = (_, _) =>
                {
                    calls++;
                    return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBasic(userName, "wrong"));
                },
            };
            using var rotatingClient = new ClickHouseClient(settings);

            var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => rotatingClient.ExecuteScalarAsync("SELECT 1"));

            Assert.Multiple(() =>
            {
                Assert.That(ex.ErrorCode, Is.EqualTo(516));
                Assert.That(calls, Is.EqualTo(2));
            });
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}");
        }
    }
}
