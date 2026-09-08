using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Uses connection-scoped temporary tables to verify session persistence, isolation, and disposal on a real server.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpSessionIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static string UniqueTableName() => $"tcp_session_test_{Guid.NewGuid():N}";

    private sealed class ValueRow
    {
        public ulong Value { get; set; }
    }

    /// <summary>
    /// Limits the pool to one slot so subsequent operations expose connection reuse or a leaked lease.
    /// </summary>
    private static ClickHouseTcpClient SingleConnectionClient()
        => new(TcpServerFixture.Options() with { MaxPoolSize = 1, PoolTimeout = TimeSpan.FromSeconds(10) });

    private static async Task<ulong> ScalarAsync(IClickHouseTcpOperations operations, string sql)
    {
        List<ulong> values = [];
        await foreach (ValueRow row in operations.QueryAsync<ValueRow>(sql, cancellationToken: None))
        {
            values.Add(row.Value);
        }

        return values.Single();
    }

    [Test]
    public async Task ExecuteAsync_TemporaryTableCreatedInASession_IsStillThereForTheNextOperation()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        string table = UniqueTableName();

        await session.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);
        await session.ExecuteAsync($"INSERT INTO {table} SELECT number FROM system.numbers LIMIT 5", cancellationToken: None);

        Assert.That(await ScalarAsync(session, $"SELECT sum(value) AS value FROM {table}"), Is.EqualTo(10UL));
    }

    [Test]
    public async Task ExecuteAsync_SetInASession_AppliesToTheNextOperation()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);

        // Read system.settings to verify SET persists across operations.
        await session.ExecuteAsync("SET max_threads = 3", cancellationToken: None);

        ulong threads = await ScalarAsync(
            session,
            "SELECT toUInt64(value) AS value FROM system.settings WHERE name = 'max_threads'");
        Assert.That(threads, Is.EqualTo(3UL));
    }

    [Test]
    public async Task OpenSessionAsync_TemporaryTableInASession_IsInvisibleToTheClientItCameFrom()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        string table = UniqueTableName();

        await session.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);

        // Client operations must not reuse the session's pinned connection.
        Assert.That(
            async () => await client.ExecuteAsync($"SELECT * FROM {table}", cancellationToken: None),
            Throws.TypeOf<ClickHouseServerException>());
    }

    [Test]
    public async Task OpenSessionAsync_TwoSessionsAtOnce_EachHasItsOwnTemporaryTable()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession first = await client.OpenSessionAsync(None);
        await using IClickHouseTcpSession second = await client.OpenSessionAsync(None);

        // Use the same table name in both sessions to verify independent connection-local state.
        string table = UniqueTableName();
        await first.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);
        await second.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);
        await first.ExecuteAsync($"INSERT INTO {table} VALUES (1)", cancellationToken: None);
        await second.ExecuteAsync($"INSERT INTO {table} VALUES (2)", cancellationToken: None);

        Assert.Multiple(async () =>
        {
            Assert.That(await ScalarAsync(first, $"SELECT sum(value) AS value FROM {table}"), Is.EqualTo(1UL));
            Assert.That(await ScalarAsync(second, $"SELECT sum(value) AS value FROM {table}"), Is.EqualTo(2UL));
        });
    }

    [Test]
    public async Task DisposeAsync_AfterASession_TheConnectionIsClosedRatherThanPooled()
    {
        await using ClickHouseTcpClient client = SingleConnectionClient();
        string table = UniqueTableName();

        IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        await session.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);
        await session.DisposeAsync();

        // With one pool slot, reusing the session's connection would expose its temporary table.
        Assert.That(
            async () => await client.ExecuteAsync($"SELECT * FROM {table}", cancellationToken: None),
            Throws.TypeOf<ClickHouseServerException>());
    }

    [Test]
    public async Task DisposeAsync_AfterASession_TheSlotGoesBackToThePool()
    {
        await using ClickHouseTcpClient client = SingleConnectionClient();

        IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        await session.PingAsync(None);
        await session.DisposeAsync();

        // A leaked lease would exhaust the single-slot pool and cause a timeout.
        Assert.That(await ScalarAsync(client, "SELECT toUInt64(1) AS value"), Is.EqualTo(1UL));
    }

    [Test]
    public async Task DisposeAsync_WithAStreamNobodyAdvancesOrDisposes_DoesNotGetTheSlotBack()
    {
        // Transport abort cannot resume an enumerator suspended at yield; its lease remains held until disposal.
        // Session disposal cannot return that lease while the operation may still access connection buffers.
        await using ClickHouseTcpClient client = new(TcpServerFixture.Options() with
        {
            MaxPoolSize = 1,
            PoolTimeout = TimeSpan.FromSeconds(1),
        });

        IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        IAsyncEnumerator<Block> abandoned = session
            .StreamAsync("SELECT number FROM system.numbers LIMIT 100000", cancellationToken: None)
            .GetAsyncEnumerator(None);
        Assert.That(await abandoned.MoveNextAsync(), Is.True);

        await session.DisposeAsync();

        Assert.That(
            async () => await client.ExecuteAsync("SELECT 1", cancellationToken: None),
            Throws.TypeOf<TimeoutException>(),
            "the pool's only slot is still held by the enumerator nobody disposed");
    }

    [Test]
    public async Task OpenSessionAsync_WhenThePoolIsFull_TimesOutRatherThanWaitingForever()
    {
        await using ClickHouseTcpClient client = new(TcpServerFixture.Options() with
        {
            MaxPoolSize = 1,
            PoolTimeout = TimeSpan.FromSeconds(1),
        });

        await using IClickHouseTcpSession holder = await client.OpenSessionAsync(None);

        // The open session holds the only pool slot.
        Assert.That(async () => await client.OpenSessionAsync(None), Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public async Task ExecuteAsync_AfterTheClientIsDisposed_ReportsTheSessionAsOverRatherThanTheConnection()
    {
        // Client disposal aborts the pinned connection. Subsequent access must report a closed session,
        // not an internal connection-reuse error.
        ClickHouseTcpClient client = new(TcpServerFixture.Options() with
        {
            MaxPoolSize = 1,
            PoolTimeout = TimeSpan.FromSeconds(1),
        });

        IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        await session.PingAsync(None);
        await client.DisposeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(session.IsOpen, Is.False);
            Assert.That(
                async () => await session.ExecuteAsync("SELECT 1", cancellationToken: None),
                Throws.InvalidOperationException.With.Message.Contains("Open a new session"));
        });
    }

    [Test]
    public async Task ExecuteAsync_AfterTheSessionIsDisposed_ThrowsObjectDisposed()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();

        IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        await session.DisposeAsync();

        Assert.That(
            async () => await session.ExecuteAsync("SELECT 1", cancellationToken: None),
            Throws.TypeOf<ObjectDisposedException>());
    }

    [Test]
    public async Task ExecuteAsync_WhileAStreamIsStillOpen_IsRefusedRatherThanInterleaved()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);

        // Keep the stream open between blocks to test rejection of interleaved operations.
        IAsyncEnumerator<Block> stream = session
            .StreamAsync("SELECT number FROM system.numbers LIMIT 100000", cancellationToken: None)
            .GetAsyncEnumerator(None);
        try
        {
            Assert.That(await stream.MoveNextAsync(), Is.True);

            // A mid-response connection is not reusable, but its active session remains open.
            Assert.That(session.IsOpen, Is.True);

            // Match the message to verify rejection by the session rather than the connection.
            Assert.That(
                async () => await session.ExecuteAsync("SELECT 1", cancellationToken: None),
                Throws.InvalidOperationException.With.Message.Contains("The session is already running an operation"));
        }
        finally
        {
            await stream.DisposeAsync();
        }
    }

    [Test]
    public async Task IsOpen_AfterAStreamIsAbandonedPartWay_IsFalseAndFurtherOperationsSayWhy()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);

        // Early stream disposal closes the connection; the session cannot reconnect without losing its state.
        await foreach (Block block in session.StreamAsync("SELECT number FROM system.numbers LIMIT 100000", cancellationToken: None))
        {
            _ = block.RowCount;
            break;
        }

        Assert.Multiple(() =>
        {
            Assert.That(session.IsOpen, Is.False);
            Assert.That(
                async () => await session.ExecuteAsync("SELECT 1", cancellationToken: None),
                Throws.TypeOf<InvalidOperationException>().With.Message.Contains("Open a new session"));
        });
    }

    [Test]
    public async Task IsOpen_AfterAQueryTheServerRejects_IsFalseAndFurtherOperationsSayWhy()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        string table = UniqueTableName();

        await session.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);
        Assert.That(
            async () => await session.ExecuteAsync("SELECT * FROM no_such_table_here", cancellationToken: None),
            Throws.TypeOf<ClickHouseServerException>());

        // A server exception retires the connection; the session must fail rather than reconnect without its state.
        Assert.Multiple(() =>
        {
            Assert.That(session.IsOpen, Is.False);
            Assert.That(
                async () => await session.ExecuteAsync($"SELECT count() FROM {table}", cancellationToken: None),
                Throws.TypeOf<InvalidOperationException>().With.Message.Contains("Open a new session"));
        });
    }

    [Test]
    public async Task InsertAsync_IntoASessionTemporaryTable_RoundTripsThroughTheSameConnection()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        string table = UniqueTableName();

        await session.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);
        IColumn[] columns = [PrimitiveColumn<ulong>.FromValues("value", "UInt64", [7UL, 11UL])];
        await session.InsertAsync($"INSERT INTO {table} (value) VALUES", columns, cancellationToken: None);

        Assert.That(await ScalarAsync(session, $"SELECT sum(value) AS value FROM {table}"), Is.EqualTo(18UL));
    }

    [Test]
    public async Task OpenSessionAsync_WhileASessionIsOpen_TheClientKeepsWorking()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);

        // Client operations use other pooled connections while the session runs a query.
        Task<ulong> onTheSession = ScalarAsync(session, "SELECT toUInt64(sleep(0.5) + 1) AS value");
        ulong[] onTheClient = await Task.WhenAll(Enumerable.Range(0, 4)
            .Select(i => ScalarAsync(client, $"SELECT toUInt64({i}) AS value")));

        Assert.Multiple(async () =>
        {
            Assert.That(await onTheSession, Is.EqualTo(1UL));
            Assert.That(onTheClient, Is.EqualTo(new ulong[] { 0, 1, 2, 3 }));
        });
    }

    [Test]
    public async Task IClickHouseTcpSession_EveryOperationRunsOnThePinnedConnection()
    {
        // Temporary-table access detects any insert or query overload routed off the pinned connection.
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);
        string table = UniqueTableName();

        await session.PingAsync(None);
        await session.ExecuteAsync($"CREATE TEMPORARY TABLE {table} (value UInt64)", cancellationToken: None);

        IColumn[] columns = [PrimitiveColumn<ulong>.FromValues("value", "UInt64", [1UL])];
        await session.InsertAsync($"INSERT INTO {table} (value) VALUES", columns, cancellationToken: None);
        await session.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", [new ValueRow { Value = 2 }], cancellationToken: None);
        await session.InsertRowsAsync($"INSERT INTO {table} (value) VALUES", [new object[] { 3UL }], cancellationToken: None);

        var untyped = new List<ulong>();
        await foreach (object[] row in session.QueryAsync($"SELECT value FROM {table} ORDER BY value", cancellationToken: None))
        {
            untyped.Add((ulong)row[0]);
        }

        var typed = new List<ulong>();
        await foreach (ValueRow row in session.QueryAsync<ValueRow>($"SELECT value FROM {table} ORDER BY value", cancellationToken: None))
        {
            typed.Add(row.Value);
        }

        var streamed = new List<ulong>();
        await foreach (Block block in session.StreamAsync($"SELECT value FROM {table} ORDER BY value", cancellationToken: None))
        {
            streamed.AddRange(((IColumn<ulong>)block[0]).Values.ToArray());
        }

        Assert.Multiple(() =>
        {
            Assert.That(untyped, Is.EqualTo(new ulong[] { 1, 2, 3 }));
            Assert.That(typed, Is.EqualTo(new ulong[] { 1, 2, 3 }));
            Assert.That(streamed, Is.EqualTo(new ulong[] { 1, 2, 3 }));
        });
    }

    [Test]
    public async Task Options_OnASession_AreTheVeryOptionsOfTheClientItCameFrom()
    {
        // Custom settings exercise dictionary copying; the session must expose the parent's options instance.
        await using ClickHouseTcpClient client = new(TcpServerFixture.Options() with
        {
            CustomSettings = new Dictionary<string, string> { ["max_threads"] = "3" },
        });
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);

        Assert.That(session.Options, Is.SameAs(client.Options));
    }

    [Test]
    public async Task OpenSessionAsync_AfterTheClientIsDisposed_ThrowsObjectDisposed()
    {
        ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        await client.DisposeAsync();

        Assert.That(async () => await client.OpenSessionAsync(None), Throws.TypeOf<ObjectDisposedException>());
    }
}
