using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// What the registrations are is unit-tested without a server. What needs one: that the client the container hands
// out really runs on the configured endpoint, and that disposing the provider tears down a pool with a live
// connection in it — the container disposes both the data source and the client it owns, so the teardown runs twice.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpServiceCollectionExtensionsIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static ServiceProvider BuildProvider()
        => new ServiceCollection()
            .AddClickHouseTcpDataSource(TcpServerFixture.ConnectionString)
            .BuildServiceProvider();

    [Test]
    public async Task AddClickHouseTcpDataSource_WithAConnectionString_ResolvesAClientThatReachesTheServer()
    {
        await using ServiceProvider provider = BuildProvider();

        IClickHouseTcpClient client = provider.GetRequiredService<IClickHouseTcpClient>();

        object value = await client.ExecuteScalarAsync("SELECT 42", cancellationToken: None);
        Assert.That(value, Is.EqualTo((byte)42));
    }

    [Test]
    public async Task DisposeAsync_OnTheProvider_ClosesThePoolTheResolvedClientWasUsing()
    {
        ServiceProvider provider = BuildProvider();
        IClickHouseTcpClient client = provider.GetRequiredService<IClickHouseTcpClient>();
        await client.PingAsync(None);

        await provider.DisposeAsync();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.PingAsync(None));
    }
}
