using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Authentication;

/// <summary>
/// <see cref="ClickHouseClientSettings.CredentialsProvider"/> against a real server: a password that rotates,
/// the rejection codes of the server, and no resend after a rejection. The JWT flow needs ClickHouse Cloud: see
/// <c>BearerAuthenticationTests</c>.
/// </summary>
public class CredentialsProviderIntegrationTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ExecuteScalarAsync_PasswordRotatedOnServer_ThrowsOnceThenNextCallGetsPasswordFromProvider()
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
            var rejection = Assert.ThrowsAsync<ClickHouseServerException>(() => rotatingClient.ExecuteScalarAsync("SELECT currentUser()"));
            var after = await rotatingClient.ExecuteScalarAsync("SELECT currentUser()");

            Assert.Multiple(() =>
            {
                Assert.That(before, Is.EqualTo(userName));
                Assert.That(rejection.ErrorCode, Is.EqualTo(516));
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
    public async Task ExecuteScalarAsync_WrongPasswordFromProvider_ThrowsAuthenticationFailedWithoutResend()
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

            // Credentials without an expiry are not cached: one provider call is one request.
            Assert.Multiple(() =>
            {
                Assert.That(ex.ErrorCode, Is.EqualTo(516));
                Assert.That(calls, Is.EqualTo(1));
            });
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_StalePasswordOfDefaultUser_Throws194AndNextCallAsksWithReasonRejected()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Requires local_single_node environment");
        }

        // The server answers wrong HTTP Basic credentials of the user default with 194 (REQUIRED_PASSWORD), not 516.
        var builder = TestUtilities.GetConnectionStringBuilder();
        var reasons = new ConcurrentQueue<ClickHouseCredentialsRefreshReason>();
        var settings = new ClickHouseClientSettings(builder)
        {
            CredentialsProvider = (request, _) =>
            {
                reasons.Enqueue(request.Reason);
                return new ValueTask<ClickHouseCredentials>(request.Reason == ClickHouseCredentialsRefreshReason.Rejected
                    ? ClickHouseCredentials.CreateBasic(builder.Username, builder.Password)
                    : ClickHouseCredentials.CreateBasic("default", $"Stale_{Guid.NewGuid():N}"));
            },
        };
        using var rotatingClient = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => rotatingClient.ExecuteScalarAsync("SELECT 1"));
        var after = await rotatingClient.ExecuteScalarAsync("SELECT currentUser()");

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(194));
            Assert.That(after, Is.EqualTo(string.IsNullOrEmpty(builder.Username) ? "default" : builder.Username));
            Assert.That(reasons.ToArray(), Is.EqualTo(new[]
            {
                ClickHouseCredentialsRefreshReason.Initial,
                ClickHouseCredentialsRefreshReason.Rejected,
            }));
        });
    }

    [Test]
    public async Task ExecuteNonQueryAsync_InsertRejectedAfterTheRowIsStored_SendsTheInsertOnce()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Requires local_single_node environment: remote() connects to the native port of the server itself");
        }

        // A materialized view that reads from remote() with wrong credentials fails the INSERT with 516 after the
        // source table has stored the row. prefer_localhost_replica = 0 makes remote() open a connection.
        var source = CreateTableName("source");
        var target = CreateTableName("target");
        var view = CreateTableName("view");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {source} (x UInt32) ENGINE = MergeTree ORDER BY x");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {target} (x UInt32) ENGINE = MergeTree ORDER BY x");
        await client.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {view} TO {target} AS SELECT x FROM {source} " +
            "WHERE x IN (SELECT toUInt32(dummy) FROM remote('localhost', system.one, 'no_such_user', 'wrong'))");
        try
        {
            var builder = TestUtilities.GetConnectionStringBuilder();
            var calls = 0;
            var settings = new ClickHouseClientSettings(builder)
            {
                CredentialsProvider = (_, _) =>
                {
                    Interlocked.Increment(ref calls);
                    return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBasic(builder.Username, builder.Password));
                },
            };
            using var providerClient = new ClickHouseClient(settings);

            var ex = Assert.ThrowsAsync<ClickHouseServerException>(
                () => providerClient.ExecuteNonQueryAsync($"INSERT INTO {source} SETTINGS prefer_localhost_replica = 0 VALUES (1)"));
            var rows = await client.ExecuteScalarAsync($"SELECT count() FROM {source}");

            // Credentials without an expiry are not cached: one provider call is one request.
            Assert.Multiple(() =>
            {
                Assert.That(ex.ErrorCode, Is.EqualTo(516));
                Assert.That(calls, Is.EqualTo(1));
                Assert.That(rows, Is.EqualTo(1UL));
            });
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP VIEW IF EXISTS {view}");
        }
    }
}
