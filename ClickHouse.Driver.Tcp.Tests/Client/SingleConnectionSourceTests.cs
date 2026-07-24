using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// These exercise the source's lifecycle without a server: with no rent, no connection is ever dialed, so
// disposal and post-disposal rejection can be checked in isolation. The rent/redial/reuse behavior over a live
// connection is covered by the client integration tests.
[TestFixture]
public class SingleConnectionSourceTests
{
    private static ClickHouseTcpClientOptions Options() => new() { Host = "localhost", Port = 9000 };

    [Test]
    public async Task DisposeAsync_CalledTwice_IsNoOp()
    {
        var source = new SingleConnectionSource(Options());

        await source.DisposeAsync();

        Assert.DoesNotThrowAsync(async () => await source.DisposeAsync());
    }

    [Test]
    public async Task RentAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var source = new SingleConnectionSource(Options());
        await source.DisposeAsync();

        Assert.ThrowsAsync<ObjectDisposedException>(async () => await source.RentAsync(CancellationToken.None));
    }

    [Test]
    public void RentAsync_PreCancelledToken_ThrowsOperationCanceledAndDoesNotDeadlock()
    {
        var source = new SingleConnectionSource(Options());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await source.RentAsync(cts.Token));

        // The gate was released on the cancellation path, so a subsequent (also-cancelled) rent still throws
        // its own cancellation rather than hanging on a leaked gate.
        Assert.CatchAsync<OperationCanceledException>(async () => await source.RentAsync(cts.Token));
    }
}
