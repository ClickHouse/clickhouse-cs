using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// <see cref="ClickHouseTcpClientOptions.CredentialsProvider"/> against a real server, with a password that rotates.
/// The JWT flow needs ClickHouse Cloud.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CredentialsProviderIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task ExecuteScalarAsync_PasswordRotatedOnServer_ConnectsAgainWithThePasswordFromTheProvider()
    {
        // Unique, because the framework suites run at once against one server and a user is server-wide.
        string user = $"tcp_rotation_{Guid.NewGuid():N}";
        string oldPassword = $"Old_{Guid.NewGuid():N}";
        string newPassword = $"New_{Guid.NewGuid():N}";
        await using var admin = TcpServerFixture.CreateClient();
        await admin.ExecuteAsync($"CREATE USER {user} IDENTIFIED BY '{oldPassword}'", cancellationToken: None);
        try
        {
            // A provider that keeps its own cache, as one in front of a secret store does: it reads the store again
            // only when the client says the server rejected the password.
            string store = oldPassword;
            string known = oldPassword;
            var reasons = new ConcurrentQueue<ClickHouseCredentialsRefreshReason>();
            ClickHouseTcpClientOptions options = TcpServerFixture.Options() with
            {
                // Each operation gets a new connection, so the second one authenticates after the rotation.
                MaxConnectionLifetime = TimeSpan.FromTicks(1),
                CredentialsProvider = new FuncCredentialsProvider((request, _) =>
                {
                    reasons.Enqueue(request.Reason);
                    if (request.Reason == ClickHouseCredentialsRefreshReason.Rejected)
                    {
                        known = store;
                    }

                    return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBasic(user, known));
                }),
            };
            await using var client = new ClickHouseTcpClient(options);

            object before = await client.ExecuteScalarAsync("SELECT currentUser()", cancellationToken: None);
            await admin.ExecuteAsync($"ALTER USER {user} IDENTIFIED BY '{newPassword}'", cancellationToken: None);
            store = newPassword;
            object after = await client.ExecuteScalarAsync("SELECT currentUser()", cancellationToken: None);

            // A username and a password have no expiry, so the client does not keep them: it calls the provider for each
            // new connection, with the reason Expiring after the first call.
            Assert.Multiple(() =>
            {
                Assert.That(before, Is.EqualTo(user));
                Assert.That(after, Is.EqualTo(user));
                Assert.That(reasons, Is.EqualTo(new[]
                {
                    ClickHouseCredentialsRefreshReason.Initial,
                    ClickHouseCredentialsRefreshReason.Expiring,
                    ClickHouseCredentialsRefreshReason.Rejected,
                }));
            });
        }
        finally
        {
            await admin.ExecuteAsync($"DROP USER IF EXISTS {user}", cancellationToken: None);
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WrongPasswordFromProvider_ThrowsAuthenticationFailedAfterOneMoreConnect()
    {
        string user = $"tcp_rotation_{Guid.NewGuid():N}";
        await using var admin = TcpServerFixture.CreateClient();
        await admin.ExecuteAsync($"CREATE USER {user} IDENTIFIED BY 'Right_{Guid.NewGuid():N}'", cancellationToken: None);
        try
        {
            int calls = 0;
            ClickHouseTcpClientOptions options = TcpServerFixture.Options() with
            {
                CredentialsProvider = new FuncCredentialsProvider((_, _) =>
                {
                    Interlocked.Increment(ref calls);
                    return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBasic(user, "wrong"));
                }),
            };
            await using var client = new ClickHouseTcpClient(options);

            var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(
                async () => await client.ExecuteScalarAsync("SELECT 1", cancellationToken: None));

            Assert.Multiple(() =>
            {
                Assert.That(thrown.Code, Is.EqualTo(ClickHouseErrorCode.AuthenticationFailed));
                Assert.That(calls, Is.EqualTo(2));
            });
        }
        finally
        {
            await admin.ExecuteAsync($"DROP USER IF EXISTS {user}", cancellationToken: None);
        }
    }
}
