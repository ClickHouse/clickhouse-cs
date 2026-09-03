using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Giving up on a result has to reach the server, not just the client: without the Cancel packet the server keeps
// running the query and writing into a socket nobody reads. Error code 735, QUERY_WAS_CANCELLED_BY_CLIENT, is
// raised only where the server reads that packet, so a query logged with it ended because the client asked rather
// than because the connection went away.
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class ClickHouseTcpCancellationIntegrationTests
{
    private const int QueryWasCancelledByClient = 735;

    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task StreamAsync_CancelledMidResult_StopsTheQueryOnTheServer()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string queryId = Guid.NewGuid().ToString();

        using var cts = new CancellationTokenSource();
        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (Block block in Unbounded(client, queryId, cts.Token))
            {
                _ = block;
                await cts.CancelAsync();
            }
        });

        Assert.That(await CancelledByClientAsync(client, queryId), Is.True);
    }

    [Test]
    public async Task StreamAsync_EnumerationAbandonedEarly_StopsTheQueryOnTheServer()
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        string queryId = Guid.NewGuid().ToString();

        await foreach (Block block in Unbounded(client, queryId, None))
        {
            _ = block;
            break;
        }

        Assert.That(await CancelledByClientAsync(client, queryId), Is.True);
    }

    [Test]
    public async Task StreamAsync_CancelledMidResult_ReturnsThePoolSlotForTheNextOperation()
    {
        // MaxPoolSize 1, so a lost permit or a connection put back broken leaves nothing for the next query to
        // run on: the assertion below could not pass by chance on a fresh connection.
        ClickHouseTcpClientOptions options = TcpServerFixture.Options() with { MaxPoolSize = 1 };
        await using var client = new ClickHouseTcpClient(options);

        using var cts = new CancellationTokenSource();
        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (Block block in Unbounded(client, Guid.NewGuid().ToString(), cts.Token))
            {
                _ = block;
                await cts.CancelAsync();
            }
        });

        var answer = 0;
        await foreach (Block block in client.StreamAsync("SELECT 42", cancellationToken: None))
        {
            answer = ((IColumn<byte>)block[0]).Values[0];
        }

        Assert.That(answer, Is.EqualTo(42));
    }

    [Test]
    public async Task StreamAsync_QueryLongerThanReadTimeout_SurvivesBecauseTheDeadlineMeasuresSilence()
    {
        // Roughly two seconds of work delivered in 200ms blocks, under a one-second deadline. The deadline bounds
        // the gap between packets, not the response, so only the blocks have to fit inside it. The rows are
        // selected rather than counted: an aggregate the planner can answer without them prunes the sleep away
        // and the query returns instantly, proving nothing.
        ClickHouseTcpClientOptions options = TcpServerFixture.Options() with { ReadTimeout = TimeSpan.FromSeconds(1) };
        await using var client = new ClickHouseTcpClient(options);

        var rows = 0;
        await foreach (Block block in client.StreamAsync(
            "SELECT number, sleepEachRow(0.02) FROM system.numbers LIMIT 100 SETTINGS max_block_size = 10",
            cancellationToken: None))
        {
            rows += block.RowCount;
        }

        Assert.That(rows, Is.EqualTo(100));
    }

    /// <summary>
    /// The same one-second deadline against a query that sends nothing at all, which is what it takes to make it
    /// fire. A <c>Progress</c> packet counts as the server speaking, and the server sends one every
    /// <c>interactive_delay</c> microseconds (100 ms by default), so <c>sleep(3)</c> on its own finishes under a
    /// one-second deadline — see the case below. Raising <c>interactive_delay</c> past the sleep buys the silence.
    /// </summary>
    [Test]
    public async Task StreamAsync_ServerSilentForLongerThanReadTimeout_FailsWithTimeoutAndGivesThePoolSlotBack()
    {
        ClickHouseTcpClientOptions options = TcpServerFixture.Options() with
        {
            ReadTimeout = TimeSpan.FromSeconds(1),
            MaxPoolSize = 1,
        };
        await using var client = new ClickHouseTcpClient(options);

        var queryOptions = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["interactive_delay"] = "10000000" },
        };

        // TimeoutException comes from nowhere else, so the type alone proves the deadline fired. Asserted instead
        // of the elapsed time, which would make this a race on a loaded machine.
        var timeout = Assert.ThrowsAsync<TimeoutException>(
            async () => await client.ExecuteScalarAsync("SELECT sleep(3)", queryOptions, None));

        // The deadline unwinds a socket read that was genuinely blocked, and the cancel attempt on the way out
        // must not block behind the same silence. A slot that never came back would fail here instead.
        object next = await client.ExecuteScalarAsync("SELECT toUInt64(7)", cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(timeout.Message, Does.Contain("ReadTimeout"), "the message has to name the option to change");
            Assert.That(next, Is.EqualTo(7UL));
        });
    }

    /// <summary>
    /// The complement, and the reason the case above has to silence the server: three seconds of work that
    /// produces no row until the end still survives a one-second deadline, because the periodic
    /// <c>Progress</c> packets keep resetting it.
    /// </summary>
    [Test]
    public async Task StreamAsync_QuerySilentApartFromProgressPackets_SurvivesAShorterReadTimeout()
    {
        ClickHouseTcpClientOptions options = TcpServerFixture.Options() with { ReadTimeout = TimeSpan.FromSeconds(1) };
        await using var client = new ClickHouseTcpClient(options);

        Assert.That(await client.ExecuteScalarAsync("SELECT sleep(3)", cancellationToken: None), Is.EqualTo((byte)0));
    }

    // A result with no end, so the server is certainly still producing it when the client stops reading. Slow and
    // small on purpose: a result that saturates the socket blocks the server in a write, where it reads nothing.
    private static IAsyncEnumerable<Block> Unbounded(ClickHouseTcpClient client, string queryId, CancellationToken cancellationToken)
        => client.StreamAsync(
            "SELECT number, sleepEachRow(0.02) FROM system.numbers SETTINGS max_block_size = 10",
            new ClickHouseTcpQueryOptions { QueryId = queryId },
            cancellationToken);

    // Waits for the query's own log record, which the server writes once the query has actually stopped, and
    // reports whether it ended because the client cancelled it.
    private static async Task<bool> CancelledByClientAsync(ClickHouseTcpClient client, string queryId)
    {
        object code = await QueryLog.ScalarAsync(
            client,
            $"SELECT exception_code FROM system.query_log WHERE query_id = '{queryId}' AND type != 'QueryStart' ORDER BY event_time_microseconds DESC LIMIT 1");

        return Convert.ToInt32(code, CultureInfo.InvariantCulture) == QueryWasCancelledByClient;
    }
}
