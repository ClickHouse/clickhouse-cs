using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// What a session looks like from underneath, in the cases a live session cannot show. A real server proves the
// effects — state carried, state kept private, connection not pooled afterwards — but not the order the disposal
// does them in, nor what happens when the session is disposed with an operation still running, which needs an
// operation held open at an exact moment.
[TestFixture]
public class PinnedConnectionSourceTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <summary>
    /// Stands in for the pool: hands over one connection and records what came back, so a test can ask whether the
    /// lease was returned, how often, and in what state the connection was by then.
    /// </summary>
    private sealed class RecordingLease : IConnectionLease
    {
        private int returns;
        private TcpConnectionState? stateOnReturn;

        private RecordingLease(ClickHouseTcpConnection connection) => Connection = connection;

        public ClickHouseTcpConnection Connection { get; }

        /// <summary>
        /// How many times the lease was returned. More than once would over-release the pool's permit. Counted
        /// with an interlocked increment, so two concurrent returns register as two rather than racing to one and
        /// leaving the very failure this exists to catch invisible.
        /// </summary>
        public int Returns => Volatile.Read(ref returns);

        /// <summary>The connection's state when the lease came back, or null while it has not.</summary>
        public TcpConnectionState? StateOnReturn => stateOnReturn;

        internal static async ValueTask<RecordingLease> CreateAsync()
            => new(await FakeConnectionFactory.CreateReadyAsync().ConfigureAwait(false));

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref returns);
            stateOnReturn = Connection.State;
            return default;
        }
    }

    [Test]
    public async Task RentAsync_TwoOperationsInARow_BothGetTheSamePinnedConnection()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        IConnectionLease first = await source.RentAsync(None);
        ClickHouseTcpConnection connection = first.Connection;
        await first.DisposeAsync();

        IConnectionLease second = await source.RentAsync(None);
        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(second.Connection, Is.SameAs(connection));
                Assert.That(lease.Returns, Is.Zero, "the underlying lease stays held between operations");
            });
        }
        finally
        {
            await second.DisposeAsync();
            await source.DisposeAsync();
        }
    }

    [Test]
    public async Task DisposeAsync_WithNoOperationRunning_TerminatesTheConnectionBeforeReturningTheLease()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        await source.DisposeAsync();

        // The order is what keeps a session's state out of the pool: the pool decides a returned connection's fate
        // from its state, so a connection still Ready when the lease comes back is one the pool keeps.
        Assert.Multiple(() =>
        {
            Assert.That(lease.Returns, Is.EqualTo(1));
            Assert.That(lease.StateOnReturn, Is.EqualTo(TcpConnectionState.Terminated));
        });
    }

    [Test]
    public async Task DisposeAsync_WhileAnOperationIsRunning_AbortsAtOnceAndReturnsWhenTheOperationEnds()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);
        IConnectionLease operation = await source.RentAsync(None);

        await source.DisposeAsync();

        // The connection is unusable immediately, so nothing can be started over it, but the lease is held back:
        // returning it would let the pool close a connection whose buffers the operation is still reading into.
        Assert.Multiple(() =>
        {
            Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Terminated));
            Assert.That(lease.Returns, Is.Zero);
        });

        await operation.DisposeAsync();

        Assert.That(lease.Returns, Is.EqualTo(1), "the operation completes the return as it unwinds");
    }

    [Test]
    public async Task DisposeAsync_CalledTwice_ReturnsTheLeaseOnce()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        await source.DisposeAsync();
        await source.DisposeAsync();

        // A second return releases a permit the source does not hold, which lets the pool run one operation past
        // MaxPoolSize for the rest of its life.
        Assert.That(lease.Returns, Is.EqualTo(1));
    }

    [Test]
    public async Task DisposeAsync_AfterAnOperationEndedFollowingDisposal_DoesNotReturnTheLeaseAgain()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);
        IConnectionLease operation = await source.RentAsync(None);

        await source.DisposeAsync();
        await operation.DisposeAsync();
        await source.DisposeAsync();

        Assert.That(lease.Returns, Is.EqualTo(1));
    }

    [Test]
    public async Task DisposeAsync_OperationLeaseDisposedTwice_ReturnsTheLeaseOnce()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);
        IConnectionLease operation = await source.RentAsync(None);

        await source.DisposeAsync();
        await operation.DisposeAsync();
        await operation.DisposeAsync();

        Assert.That(lease.Returns, Is.EqualTo(1));
    }

    [Test]
    public async Task RentAsync_WhileAnotherOperationHoldsTheConnection_Throws()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);
        IConnectionLease operation = await source.RentAsync(None);

        try
        {
            Assert.That(async () => await source.RentAsync(None), Throws.InvalidOperationException);
        }
        finally
        {
            await operation.DisposeAsync();
            await source.DisposeAsync();
        }
    }

    [Test]
    public async Task RentAsync_AfterDisposal_ThrowsObjectDisposed()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);
        await source.DisposeAsync();

        Assert.That(async () => await source.RentAsync(None), Throws.TypeOf<ObjectDisposedException>());
    }

    [Test]
    public async Task RentAsync_AfterAnOperationLostTheConnection_ThrowsAndSaysTheSessionIsOver()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        IConnectionLease operation = await source.RentAsync(None);
        operation.Connection.Terminate();
        await operation.DisposeAsync();

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(source.IsOpen, Is.False);
                Assert.That(
                    async () => await source.RentAsync(None),
                    Throws.InvalidOperationException.With.Message.Contains("Open a new session"));
            });
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Test]
    public async Task IsOpen_AfterTheConnectionDiesBetweenOperations_TurnsFalseWithoutAnOperationToNoticeIt()
    {
        // What testing the connection before each operation buys, and the one case release cannot cover: at
        // release nothing has had time to go wrong yet. Here the connection is lost while the session sits idle —
        // a proxy or the server dropping one nobody is using — so only a later look finds it.
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        IConnectionLease operation = await source.RentAsync(None);
        await operation.DisposeAsync();
        Assert.That(source.IsOpen, Is.True, "the operation left the connection fine");

        // Stands in for the peer going away between operations: the session did nothing, and the connection is
        // gone all the same.
        lease.Connection.AbortTransport();

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(source.IsOpen, Is.False);
                Assert.That(
                    async () => await source.RentAsync(None),
                    Throws.InvalidOperationException.With.Message.Contains("Open a new session"));
            });
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Test]
    public async Task RentAsync_WhenTheConnectionIsOutOfStepWithTheServer_RefusesItThoughItLooksReady()
    {
        // Bytes nobody read leave the reader out of step, which is what an abandoned result leaves behind. The
        // state is still Ready, so a check on the state alone would run the next operation over a stream whose
        // position is wrong and read another response's bytes as its own; only the reusability test sees it.
        var lease = await TrailingBytesLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(lease.Connection.State, Is.EqualTo(TcpConnectionState.Ready));
                Assert.That(source.IsOpen, Is.False);
                Assert.That(
                    async () => await source.RentAsync(None),
                    Throws.InvalidOperationException.With.Message.Contains("Open a new session"));
            });
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Test]
    public async Task IsOpen_BeforeAndAfterDisposal_ReportsWhetherOperationsCanStillRun()
    {
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        Assert.That(source.IsOpen, Is.True);

        IConnectionLease operation = await source.RentAsync(None);
        Assert.That(source.IsOpen, Is.True, "a session running an operation is still open");

        await operation.DisposeAsync();
        await source.DisposeAsync();

        Assert.That(source.IsOpen, Is.False);
    }

    [Test]
    public async Task DisposeAsync_RacedAgainstAnEndingOperationAndAnotherDisposal_ReturnsTheLeaseExactlyOnce()
    {
        // Over-releasing is the failure that does not announce itself: a second return hands the pool a permit the
        // source never held, and the pool then runs one operation past MaxPoolSize for good. A session has one
        // owner by contract, so this is not the supported use — it is the proof that misuse cannot corrupt the
        // pool the session shares with everyone else.
        for (int attempt = 0; attempt < 200; attempt++)
        {
            RecordingLease lease = await RecordingLease.CreateAsync();
            var source = new PinnedConnectionSource(lease);
            IConnectionLease operation = await source.RentAsync(None);

            await Task.WhenAll(
                Task.Run(async () => await source.DisposeAsync()),
                Task.Run(async () => await operation.DisposeAsync()),
                Task.Run(async () => await source.DisposeAsync()));

            Assert.Multiple(() =>
            {
                Assert.That(lease.Returns, Is.EqualTo(1), $"attempt {attempt}");
                Assert.That(lease.StateOnReturn, Is.EqualTo(TcpConnectionState.Terminated), $"attempt {attempt}");
            });
        }
    }

    /// <summary>
    /// A lease over a connection whose "server" said more than the handshake called for. Nothing read those bytes,
    /// so the connection is Ready but out of step — the state an abandoned result leaves, reproduced without one.
    /// </summary>
    private sealed class TrailingBytesLease : IConnectionLease
    {
        private TrailingBytesLease(ClickHouseTcpConnection connection) => Connection = connection;

        public ClickHouseTcpConnection Connection { get; }

        internal static async ValueTask<TrailingBytesLease> CreateAsync()
            => new(await FakeConnectionFactory.CreateReadyAsync(trailing: [0x09]).ConfigureAwait(false));

        public ValueTask DisposeAsync() => default;
    }
}
