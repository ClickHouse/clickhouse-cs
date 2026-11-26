using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// JWT authentication over the native protocol. JWT authentication needs ClickHouse Cloud: set
/// <c>CLICKHOUSE_CLOUD_JWT</c> to a valid token to run these tests, and <c>CLICKHOUSE_CLOUD_JWT_EXPIRED</c> to a token
/// that has expired for the token refresh test. In CI, the Cloud workflow signs new tokens for each run with
/// <c>.github/scripts/jwt.cs</c>.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class BearerAuthenticationIntegrationTests
{
    // ClickHouse Cloud runs a JWT-authenticated query as an ephemeral user named JWT::<subject>::<claims_hash>.
    private const string JwtUserNamePattern = "^JWT::.+::.+$";

    private static readonly CancellationToken None = CancellationToken.None;

    private string bearerToken;

    [SetUp]
    public void SetUp()
    {
        bearerToken = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_JWT");
        if (string.IsNullOrEmpty(bearerToken))
        {
            Assert.Ignore("Skipping JWT tests: CLICKHOUSE_CLOUD_JWT environment variable must be set");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WithBearerTokenOption_RunsAsJwtUser()
    {
        // The fixture options also carry a username and a password: the token takes precedence over them.
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { BearerToken = bearerToken });

        object user = await client.ExecuteScalarAsync("SELECT currentUser()", cancellationToken: None);

        Assert.That(user, Does.Match(JwtUserNamePattern));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithBearerTokenInConnectionString_RunsAsJwtUser()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder(TcpServerFixture.ConnectionString)
        {
            BearerToken = bearerToken,
        };
        await using var client = new ClickHouseTcpClient(builder.ConnectionString);

        object user = await client.ExecuteScalarAsync("SELECT currentUser()", cancellationToken: None);

        Assert.That(user, Does.Match(JwtUserNamePattern));
    }

    [Test]
    public async Task ExecuteScalarAsync_ProviderReturnsExpiredTokenFirst_ConnectsAgainWithTheNextToken()
    {
        string expiredToken = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_JWT_EXPIRED");
        if (string.IsNullOrEmpty(expiredToken))
        {
            Assert.Ignore("Skipping the JWT refresh test: CLICKHOUSE_CLOUD_JWT_EXPIRED environment variable must be set");
        }

        // The server rejects the expired token in the handshake. The client then asks the provider again, with the
        // reason Rejected, and connects once more.
        var reasons = new ConcurrentQueue<ClickHouseCredentialsRefreshReason>();
        ClickHouseTcpClientOptions options = TcpServerFixture.Options() with
        {
            CredentialsProvider = new FuncCredentialsProvider((request, _) =>
            {
                reasons.Enqueue(request.Reason);
                string token = request.Reason == ClickHouseCredentialsRefreshReason.Initial ? expiredToken : bearerToken;
                return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBearer(token));
            }),
        };
        await using var client = new ClickHouseTcpClient(options);

        object user = await client.ExecuteScalarAsync("SELECT currentUser()", cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(user, Does.Match(JwtUserNamePattern));
            Assert.That(reasons, Is.EqualTo(new[]
            {
                ClickHouseCredentialsRefreshReason.Initial,
                ClickHouseCredentialsRefreshReason.Rejected,
            }));
        });
    }
}
