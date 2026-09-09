using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// Tests lease-return ordering and disposal races with explicitly controlled operation lifetimes.
[TestFixture]
public class PinnedConnectionSourceTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <summary>
    /// Records lease returns and the connection state at return without a pool.
    /// </summary>
    private sealed class RecordingLease : IConnectionLease
    {
        private int returns;
        private TcpConnectionState? stateOnReturn;

        private RecordingLease(ClickHouseTcpConnection connection) => Connection = connection;

        public ClickHouseTcpConnection Connection { get; }

        /// <summary>
        /// Return count, incremented atomically so concurrent duplicate returns cannot go undetected.
        /// </summary>
        public int Returns => Volatile.Read(ref returns);

        /// <summary>Connection state at lease return, or null before return.</summary>
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

        // Terminate before returning the lease so the pool cannot reuse session state.
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

        // Abort immediately, but defer lease return until the operation stops using the connection's buffers.
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

        // Duplicate returns over-release the pool permit, violating MaxPoolSize.
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
        // Idle connection loss must be detected on access, not only when an operation releases its lease.
        RecordingLease lease = await RecordingLease.CreateAsync();
        var source = new PinnedConnectionSource(lease);

        IConnectionLease operation = await source.RentAsync(None);
        await operation.DisposeAsync();
        Assert.That(source.IsOpen, Is.True, "the operation left the connection fine");

        // Simulate transport loss between operations.
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
        // Unread bytes make a Ready connection non-reusable; checking its state alone is insufficient.
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
        // Concurrent disposal violates single-owner usage but must not over-release the shared pool's permit.
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
    /// Provides a Ready but non-reusable connection with unread bytes after the fake handshake.
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
