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

// Verify cancellation through query_log error code 735 (QUERY_WAS_CANCELLED_BY_CLIENT),
// which confirms that the server processed the Cancel packet.
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
        // A single pool slot verifies that cancellation leaves capacity for the next query.
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
    public async Task StreamAsync_QueryLongerThanReadTimeout_SurvivesBecauseTheTimeoutMeasuresSilence()
    {
        // The query takes roughly two seconds, producing ten-row blocks every 200 ms with a one-second read timeout.
        // Select the sleepEachRow result so the planner must evaluate it.
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
    /// Verifies that the read timeout fires when no packet arrives before the timeout expires.
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

        // The exception types prove the timeout fired without a timing-sensitive elapsed-time assertion.
        var timeout = Assert.ThrowsAsync<ClickHouseTcpConnectionException>(
            async () => await client.ExecuteScalarAsync("SELECT sleep(3)", queryOptions, None));

        // The next query verifies that timeout cleanup returned the only pool slot.
        object next = await client.ExecuteScalarAsync("SELECT toUInt64(7)", cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(timeout.Message, Does.Contain("ReadTimeout"), "the message has to name the option to change");
            Assert.That(timeout.InnerException, Is.TypeOf<TimeoutException>());
            Assert.That(next, Is.EqualTo(7UL));
        });
    }

    /// <summary>
    /// Verifies that progress packets reset the read timeout.
    /// </summary>
    [Test]
    public async Task StreamAsync_QuerySilentApartFromProgressPackets_SurvivesAShorterReadTimeout()
    {
        ClickHouseTcpClientOptions options = TcpServerFixture.Options() with { ReadTimeout = TimeSpan.FromSeconds(1) };
        await using var client = new ClickHouseTcpClient(options);

        Assert.That(await client.ExecuteScalarAsync("SELECT sleep(3)", cancellationToken: None), Is.EqualTo((byte)0));
    }

    // An unbounded query remains active when the client stops reading. Small, delayed blocks
    // allow the server to read Cancel between writes.
    private static IAsyncEnumerable<Block> Unbounded(ClickHouseTcpClient client, string queryId, CancellationToken cancellationToken)
        => client.StreamAsync(
            "SELECT number, sleepEachRow(0.02) FROM system.numbers SETTINGS max_block_size = 10",
            new ClickHouseTcpQueryOptions { QueryId = queryId },
            cancellationToken);

    // Wait for the query log record and check for the client-cancellation error code.
    private static async Task<bool> CancelledByClientAsync(ClickHouseTcpClient client, string queryId)
    {
        object code = await QueryLog.ScalarAsync(
            client,
            $"SELECT exception_code FROM system.query_log WHERE query_id = '{queryId}' AND type != 'QueryStart' ORDER BY event_time_microseconds DESC LIMIT 1");

        return Convert.ToInt32(code, CultureInfo.InvariantCulture) == QueryWasCancelledByClient;
    }
}
