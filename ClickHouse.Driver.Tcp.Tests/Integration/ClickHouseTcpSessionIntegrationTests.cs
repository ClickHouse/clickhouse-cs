using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// A session is only worth having if the server agrees that it is one connection, so everything that defines one —
// state carried between operations, state kept out of everyone else's, and the connection closed rather than pooled
// afterwards — is asserted against a real server. A temporary table is the marker throughout: the server scopes it
// to the connection that made it, so its visibility says which connection ran a query without the client having to
// claim anything about its own pool.
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
    /// A client whose pool is one connection wide, so a session leaves nothing behind it: whatever the client does
    /// next must use the very connection the session gave back, which is what makes "was it pooled?" observable.
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

        // Read back from system.settings rather than the value we sent: that view reports what the server holds for
        // this connection, so it answers whether the SET survived rather than whether it parsed.
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

        // The session holds its connection, so the client is served from the rest of the pool and sees no such
        // table. This is the pinning itself: without it the client could land on the session's connection.
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

        // One name, two sessions: the creates can only both succeed if each landed on its own connection, and the
        // reads then say which rows each session's table holds.
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

        // The pool is one wide, so this query runs on whatever the session gave back. A pooled connection would
        // still have the temporary table on it; a closed one is replaced by a dial to a server that never saw it.
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

        // The only slot the pool has. Had disposal closed the connection without returning the lease, this would
        // wait out PoolTimeout and fail instead.
        Assert.That(await ScalarAsync(client, "SELECT toUInt64(1) AS value"), Is.EqualTo(1UL));
    }

    [Test]
    public async Task DisposeAsync_WithAStreamNobodyAdvancesOrDisposes_DoesNotGetTheSlotBack()
    {
        // The caveat on the abort path, pinned rather than left to be discovered. Aborting the transport frees an
        // operation parked on the socket; an enumerator the caller stopped advancing is parked at its yield
        // instead, so nothing resumes it, nothing disposes its lease, and the slot stays out. Disposal still
        // returns promptly and reports nothing — it cannot return the lease itself without letting the pool
        // terminate a connection a live operation may still be reading into.
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

        // A session competes for a connection like any other operation, and holds one for its whole life, so the
        // pool being one wide is the pool being full.
        Assert.That(async () => await client.OpenSessionAsync(None), Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public async Task ExecuteAsync_AfterTheClientIsDisposed_ReportsTheSessionAsOverRatherThanTheConnection()
    {
        // Disposing the client first is the wrong order — its drain aborts the session's connection — and the
        // session learns of it only when asked. Because it is asked before each operation, and not merely after
        // the last one, this reports a finished session rather than letting the connection's own internal
        // "terminated and cannot be reused" reach the caller.
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

        // The enumerator holds the connection between blocks, which is exactly when a second operation would write
        // its Query packet into the middle of this result.
        IAsyncEnumerator<Block> stream = session
            .StreamAsync("SELECT number FROM system.numbers LIMIT 100000", cancellationToken: None)
            .GetAsyncEnumerator(None);
        try
        {
            Assert.That(await stream.MoveNextAsync(), Is.True);

            // Parked between blocks the connection is mid-response and so not reusable, which is the trap in
            // testing it for liveness before each operation: a test that ran now would read "not reusable" and
            // condemn a session that is working perfectly.
            Assert.That(session.IsOpen, Is.True);

            // The connection refuses a second operation too, so the message is what says the session caught this
            // first — and the session's message is the one that mentions the enumerator still holding it.
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

        // Stopping mid-result leaves the rest of it coming, so the connection cannot carry anything else and is
        // closed. On the client that costs a redial; on a session it is the end of the session, because the state
        // the session existed for went with the connection.
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

        // An Exception packet does not say whether the server will accept another request, so the connection is
        // retired. Unlike a pooled client, a session cannot redial without losing the state it exists to preserve.
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

        // The session owns one connection; the pool serves the client from the others, including concurrently with
        // the session's own operation.
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
        // The session delegates each operation to an inner client, so the risk is a member wired to the wrong
        // place. A temporary table catches that: every tier below has to reach the one connection that owns it, so
        // a misrouted member fails outright rather than quietly using another connection.
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
        // With custom settings present, the options carry a dictionary that is copied whenever a client is built
        // over them — so this passes only because the session is handed the copy its parent already owns. An
        // equal-looking copy would satisfy a property-by-property check and still break record equality.
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
