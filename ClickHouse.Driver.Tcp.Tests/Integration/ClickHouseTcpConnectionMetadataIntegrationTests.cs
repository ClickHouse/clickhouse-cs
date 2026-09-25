using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Drives each interleaved metadata packet from a real server and asserts its handler fires, validating the
// decoders against real server output rather than only hand-authored bytes. A yielded Block is borrowed, and so
// is a block handed to a block handler — everything needed is copied out inside the callback.
//
// Not covered here: TableColumns (the server sends it for external-table/defaults scenarios that a plain query
// does not create, and the client discards it anyway) and PartUUIDs (needs part-level query deduplication on a
// replicated table). Both remain covered by the scripted-byte unit tests.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpConnectionMetadataIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static async Task DrainAsync(ClickHouseTcpConnection connection, string sql, MetadataHandlers handlers, IReadOnlyDictionary<string, string> settings = null)
    {
        await foreach (Block block in connection.QueryAsync(sql, settings: settings, handlers: handlers, cancellationToken: None))
        {
            _ = block.RowCount;
        }
    }

    [Test]
    public async Task QueryAsync_ScanningManyRows_InvokesProgressWithRowsRead()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int progressCount = 0;
        ulong maxRows = 0;
        await DrainAsync(connection, "SELECT sum(number) FROM numbers(2000000)", new MetadataHandlers
        {
            OnProgress = p =>
            {
                progressCount++;
                if (p.Rows > maxRows)
                {
                    maxRows = p.Rows;
                }
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(progressCount, Is.GreaterThan(0), "at least one Progress packet");
            Assert.That(maxRows, Is.GreaterThan(0UL), "Progress reports rows read");
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_AnyQuery_InvokesProfileInfo()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int count = 0;
        ulong rows = 0;
        await DrainAsync(connection, "SELECT number FROM numbers(10)", new MetadataHandlers
        {
            OnProfileInfo = info =>
            {
                count++;
                rows = info.Rows;
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.GreaterThan(0), "ProfileInfo summary is sent");
            Assert.That(rows, Is.EqualTo(10UL));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_AnyQuery_InvokesProfileEvents()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int blocks = 0;
        await DrainAsync(connection, "SELECT number FROM numbers(10)", new MetadataHandlers
        {
            OnProfileEvents = block =>
            {
                if (block.RowCount > 0)
                {
                    blocks++;
                }
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(blocks, Is.GreaterThan(0), "the server sends a ProfileEvents block");
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_GroupByWithTotals_InvokesTotalsOnce()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int totalsCount = 0;
        int totalsRows = 0;
        ulong totalCount = 0;
        await DrainAsync(
            connection,
            "SELECT number % 3 AS k, count() AS c FROM numbers(100) GROUP BY k WITH TOTALS",
            new MetadataHandlers
            {
                OnTotals = block =>
                {
                    totalsCount++;
                    totalsRows = block.RowCount;
                    totalCount = ((IColumn<ulong>)block[1]).Values[0];
                },
            });

        Assert.Multiple(() =>
        {
            Assert.That(totalsCount, Is.EqualTo(1), "exactly one Totals block");
            Assert.That(totalsRows, Is.EqualTo(1), "the totals row");
            Assert.That(totalCount, Is.EqualTo(100UL), "the grand total count");
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_ExtremesSetting_InvokesExtremesWithMinAndMax()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int extremesCount = 0;
        ulong[] rows = null;
        await DrainAsync(
            connection,
            "SELECT number FROM numbers(10)",
            new MetadataHandlers
            {
                OnExtremes = block =>
                {
                    extremesCount++;
                    rows = ((IColumn<ulong>)block[0]).Values.ToArray();
                },
            },
            settings: new Dictionary<string, string> { ["extremes"] = "1" });

        Assert.Multiple(() =>
        {
            Assert.That(extremesCount, Is.EqualTo(1), "exactly one Extremes block");
            Assert.That(rows, Is.EqualTo(new ulong[] { 0, 9 }), "min then max");
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_TraceLogsSetting_InvokesLog()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int logRows = 0;
        await DrainAsync(
            connection,
            "SELECT sum(number) FROM numbers(100000)",
            new MetadataHandlers
            {
                OnLog = block => logRows += block.RowCount,
            },
            settings: new Dictionary<string, string> { ["send_logs_level"] = "trace" });

        Assert.Multiple(() =>
        {
            Assert.That(logRows, Is.GreaterThan(0), "the server streams trace-level log rows");
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }
}
