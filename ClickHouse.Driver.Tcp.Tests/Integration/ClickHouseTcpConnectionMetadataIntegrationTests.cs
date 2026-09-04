using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Drives each interleaved metadata packet from a real server at the connection level, where the packet dispatcher
// sits, and asserts two things a fully-drained query does not: that the packet's callback fires, and that the
// connection is left reusable. A decoder consuming the wrong byte count can still let the query finish while
// leaving the connection unusable. What the packets carry, and the schema of the Log and ProfileEvents blocks,
// belongs to ClickHouseTcpCallbackIntegrationTests.
//
// Not covered here: TableColumns (the server sends it for external-table/defaults scenarios that a plain query
// does not create, and the client discards it anyway) and PartUUIDs (needs part-level query deduplication on a
// replicated table). Both remain covered by the scripted-byte unit tests.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpConnectionMetadataIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <summary>The metadata packets a plain query can be made to produce.</summary>
    public enum MetadataPacket
    {
        Progress,
        ProfileInfo,
        ProfileEvents,
        Totals,
        Extremes,
        Log,
    }

    [TestCase(MetadataPacket.Progress, "SELECT sum(number) FROM numbers(2000000)", null, null)]
    [TestCase(MetadataPacket.ProfileInfo, "SELECT number FROM numbers(10)", null, null)]
    [TestCase(MetadataPacket.ProfileEvents, "SELECT number FROM numbers(10)", null, null)]
    [TestCase(MetadataPacket.Totals, "SELECT number % 3 AS k, count() AS c FROM numbers(100) GROUP BY k WITH TOTALS", null, null)]
    [TestCase(MetadataPacket.Extremes, "SELECT number FROM numbers(10)", "extremes", "1")]
    [TestCase(MetadataPacket.Log, "SELECT sum(number) FROM numbers(100000)", "send_logs_level", "trace")]
    public async Task QueryAsync_MetadataPacket_InvokesItsCallbackAndLeavesTheConnectionReady(
        MetadataPacket packet,
        string sql,
        string settingKey,
        string settingValue)
    {
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);

        int invocations = 0;
        ClickHouseTcpQueryCallbacks callbacks = packet switch
        {
            MetadataPacket.Progress => new ClickHouseTcpQueryCallbacks { OnProgress = _ => invocations++ },
            MetadataPacket.ProfileInfo => new ClickHouseTcpQueryCallbacks { OnProfileInfo = _ => invocations++ },
            MetadataPacket.ProfileEvents => new ClickHouseTcpQueryCallbacks { OnProfileEvents = WhenRows },
            MetadataPacket.Totals => new ClickHouseTcpQueryCallbacks { OnTotals = WhenRows },
            MetadataPacket.Extremes => new ClickHouseTcpQueryCallbacks { OnExtremes = WhenRows },
            MetadataPacket.Log => new ClickHouseTcpQueryCallbacks { OnLog = WhenRows },
            _ => throw new ArgumentOutOfRangeException(nameof(packet), packet, "unhandled packet"),
        };

        IReadOnlyDictionary<string, string> settings = settingKey is null
            ? null
            : new Dictionary<string, string> { [settingKey] = settingValue };

        await foreach (Block block in connection.QueryAsync(sql, settings: settings, callbacks: callbacks, cancellationToken: None))
        {
            _ = block.RowCount;
        }

        Assert.Multiple(() =>
        {
            Assert.That(invocations, Is.GreaterThan(0), "the packet reached its callback");
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready), "and was consumed whole, leaving the connection reusable");
        });

        // A block-shaped packet counts only when it carries rows, an empty one proving nothing was decoded out of it.
        void WhenRows(Block block)
        {
            if (block.RowCount > 0)
            {
                invocations++;
            }
        }
    }
}
