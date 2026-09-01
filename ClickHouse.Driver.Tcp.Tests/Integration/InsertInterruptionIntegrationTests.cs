using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// An INSERT whose data phase does not finish, either because the caller cancelled it or because the server
/// rejected an early block while later ones were still going out. An insert writes every block before reading
/// anything, so both cases leave blocks the socket has already taken, and the question a caller needs answered
/// is whether any of those rows land.
///
/// <para>
/// The row phase is also the one stretch where the client cannot send a Cancel packet — appended to a truncated
/// Data packet it would be read as more block bytes — so an interrupted insert is expressed by dropping the
/// connection. That makes "the pool slot comes back" a separate claim from "the connection survives", and both
/// are asserted here on a pool one connection wide.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class InsertInterruptionIntegrationTests
{
    private const int RowsPerBlock = 100;

    private static readonly CancellationToken None = CancellationToken.None;

    private static string UniqueTableName() => $"tcp_insert_interrupt_test_{Guid.NewGuid():N}";

    private static object[][] Ids(int count) => Enumerable.Range(0, count).Select(i => new object[] { (ulong)i }).ToArray();

    [Test]
    public async Task InsertRowsAsync_CancelledPartWayThroughTheDataPhase_CommitsNothingAndGivesThePoolSlotBack()
    {
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { MaxPoolSize = 1 });
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64) ENGINE = MergeTree ORDER BY id", cancellationToken: None);

            // OnBlockWritten runs after the block's flush, so cancelling from it puts the cancellation squarely
            // inside the data phase with one block already on the socket. Waiting on a timer instead would land
            // anywhere, including before the phase starts.
            using var cancellation = new CancellationTokenSource();
            var blocksWritten = new List<int>();
            var options = new ClickHouseTcpInsertOptions
            {
                MaxRowsPerBlock = RowsPerBlock,
                Callbacks = new ClickHouseTcpQueryCallbacks
                {
                    OnBlockWritten = block =>
                    {
                        blocksWritten.Add(block.RowCount);
                        if (block.BlockIndex == 0)
                        {
                            cancellation.Cancel();
                        }
                    },
                },
            };

            Assert.That(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} (id) VALUES", Ids(5 * RowsPerBlock), options, cancellation.Token),
                Throws.InstanceOf<OperationCanceledException>());

            object committed = await client.ExecuteScalarAsync($"SELECT count() FROM {table}", cancellationToken: None);
            object next = await client.ExecuteScalarAsync("SELECT toUInt64(7)", cancellationToken: None);

            Assert.Multiple(() =>
            {
                Assert.That(blocksWritten, Is.EqualTo(new[] { RowsPerBlock }), "one block out, then the cancel");
                Assert.That(committed, Is.EqualTo(0UL), "an insert the server never saw the end of commits nothing");
                Assert.That(next, Is.EqualTo(7UL), "the only pool slot came back");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    /// <summary>
    /// The server rejects the second block while the client is still writing the rest. Nothing reads the error
    /// until every block has gone out, so this pins that the write phase finishes and reports the server's error
    /// rather than stalling against a peer that stopped reading.
    /// </summary>
    [TestCase(5, TestName = "{m}(five blocks)")]
    [TestCase(200, TestName = "{m}(two hundred blocks)")]
    public async Task InsertRowsAsync_ServerRejectsAnEarlyBlockWhileLaterOnesAreStillGoingOut_ReportsTheError(int blocks)
    {
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { MaxPoolSize = 1 });
        string table = UniqueTableName();
        try
        {
            // The constraint passes for the first block's ids and fails for every one after it.
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (id UInt64, CONSTRAINT first_block_only CHECK id < {RowsPerBlock}) ENGINE = MergeTree ORDER BY id",
                cancellationToken: None);

            var options = new ClickHouseTcpInsertOptions { MaxRowsPerBlock = RowsPerBlock };

            var refusal = Assert.ThrowsAsync<ClickHouseTcpServerException>(
                async () => await client.InsertRowsAsync($"INSERT INTO {table} (id) VALUES", Ids(blocks * RowsPerBlock), options, None));

            object committed = await client.ExecuteScalarAsync($"SELECT count() FROM {table}", cancellationToken: None);
            object next = await client.ExecuteScalarAsync("SELECT toUInt64(7)", cancellationToken: None);

            Assert.Multiple(() =>
            {
                Assert.That(refusal.Message, Does.Contain("first_block_only"), "the server names the constraint");
                Assert.That(committed, Is.EqualTo(0UL), "not even the block that satisfied the constraint");
                Assert.That(next, Is.EqualTo(7UL), "the only pool slot came back");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }
}
