using System;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// The point of the data source is ownership, and only a live pool can show it: a view that disposes itself
// must leave the pool usable, and disposing the data source must not.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpDataSourceIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static ClickHouseTcpDataSource CreateDataSource()
        => new(TcpServerFixture.Options());

    [Test]
    public async Task GetClient_ReturnsTheSameInstanceEveryCall()
    {
        await using ClickHouseTcpDataSource source = CreateDataSource();

        Assert.That(source.GetClient(), Is.SameAs(source.GetClient()));
    }

    [Test]
    public async Task GetClient_DisposedByAConsumer_LeavesThePoolWorking()
    {
        // A scoped service that disposes what it was injected must not close a pool it does not own.
        await using ClickHouseTcpDataSource source = CreateDataSource();

        IClickHouseTcpClient injected = source.GetClient();
        await injected.PingAsync(None);
        await injected.DisposeAsync();

        object value = await source.GetClient().ExecuteScalarAsync("SELECT 1", cancellationToken: None);
        Assert.That(value, Is.EqualTo((byte)1));
    }

    [Test]
    public async Task DisposeAsync_ClosesThePool_SoTheViewStopsWorking()
    {
        ClickHouseTcpDataSource source = CreateDataSource();
        IClickHouseTcpClient client = source.GetClient();
        await client.PingAsync(None);

        await source.DisposeAsync();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.PingAsync(None));
    }

    [Test]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        ClickHouseTcpDataSource source = CreateDataSource();
        await source.DisposeAsync();

        Assert.DoesNotThrowAsync(async () => await source.DisposeAsync());
    }

    [Test]
    public async Task OpenSessionAsync_RunsOnThePoolAndIsTheCallersToDispose()
    {
        await using ClickHouseTcpDataSource source = CreateDataSource();

        await using (IClickHouseTcpSession session = await source.OpenSessionAsync(None))
        {
            await session.ExecuteAsync("CREATE TEMPORARY TABLE ds_session_marker (id UInt8)", cancellationToken: None);
            object count = await session.ExecuteScalarAsync("SELECT count() FROM ds_session_marker", cancellationToken: None);
            Assert.That(count, Is.EqualTo(0UL));
        }

        // The session closed its connection, but the data source's pool is untouched.
        object value = await source.GetClient().ExecuteScalarAsync("SELECT 2", cancellationToken: None);
        Assert.That(value, Is.EqualTo((byte)2));
    }

    [Test]
    public async Task Options_ReportsTheConfigurationOperationsRunUnder()
    {
        await using ClickHouseTcpDataSource source = CreateDataSource();

        Assert.That(source.Options.Host, Is.EqualTo(TcpServerFixture.Host));
    }
}
