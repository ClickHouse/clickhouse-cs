using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Client;

// What no server round-trip can reach: the null-when-nothing-is-listening contract, the ordering guarantee
// between the client's own observer and the caller's callback, and the schema error paths — a real server always
// sends the schema the projection expects, so only a hand-built block can drive them. The projection of a real
// server's blocks is covered by ClickHouseTcpCallbackIntegrationTests.
[TestFixture]
public class MetadataCallbackBridgeTests
{
    [Test]
    public void Build_NothingSet_ReturnsNull()
    {
        // The read path's null check is then the whole cost of the feature for a caller who asked for nothing.
        Assert.That(MetadataCallbackBridge.Build(null), Is.Null);
    }

    [Test]
    public void Build_EmptyCallbacksObject_ReturnsNull()
    {
        Assert.That(MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks()), Is.Null);
    }

    [Test]
    public void Build_OnlyAnInternalObserver_ReturnsHandlersForThatPacketAlone()
    {
        MetadataHandlers handlers = MetadataCallbackBridge.Build(null, onProgress: _ => { });

        Assert.That(handlers, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(handlers.OnProgress, Is.Not.Null);
            Assert.That(handlers.OnLog, Is.Null, "an unset callback leaves the packet discarded rather than decoded");
            Assert.That(handlers.OnProfileEvents, Is.Null);
            Assert.That(handlers.OnTotals, Is.Null);
        });
    }

    [Test]
    public void Build_BothObservers_RunsTheClientsBeforeTheCallers()
    {
        // The documented ordering: a caller's callback that throws must not cost the client the telemetry it
        // already had in hand.
        var order = new List<string>();
        MetadataHandlers handlers = MetadataCallbackBridge.Build(
            new ClickHouseTcpQueryCallbacks { OnProgress = _ => order.Add("caller") },
            onProgress: _ => order.Add("client"));

        handlers.OnProgress(default);

        Assert.That(order, Is.EqualTo(new[] { "client", "caller" }));
    }

    [Test]
    public void Build_CallerCallbackThrows_TheClientObserverHasAlreadyRun()
    {
        var seen = new List<ClickHouseTcpProgress>();
        MetadataHandlers handlers = MetadataCallbackBridge.Build(
            new ClickHouseTcpQueryCallbacks { OnProgress = _ => throw new InvalidOperationException("from the caller") },
            onProgress: seen.Add);

        Assert.Throws<InvalidOperationException>(() => handlers.OnProgress(new ClickHouseTcpProgress(3, 24, 3, 0, 0, 1)));

        Assert.That(seen, Has.Count.EqualTo(1), "the client's observer ran first, so its count survives the throw");
        Assert.That(seen[0].Rows, Is.EqualTo(3UL));
    }

    [Test]
    public void OnLog_WellFormedBlock_ProjectsEveryRow()
    {
        var rows = new List<ClickHouseTcpServerLogRow>();
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnServerLog = rows.Add });

        using Block block = LogBlock(priority: 7);
        handlers.OnLog(block);

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            // 1700000000 seconds plus 500000 microseconds, to the microsecond.
            Assert.That(rows[0].EventTime, Is.EqualTo(DateTimeOffset.FromUnixTimeSeconds(1700000000).AddTicks(5000000)));
            Assert.That(rows[0].HostName, Is.EqualTo("host-1"));
            Assert.That(rows[0].QueryId, Is.EqualTo("query-1"));
            Assert.That(rows[0].ThreadId, Is.EqualTo(4242UL));
            Assert.That(rows[0].Level, Is.EqualTo(ClickHouseTcpServerLogLevel.Debug));
            Assert.That(rows[0].Source, Is.EqualTo("executeQuery"));
            Assert.That(rows[0].Text, Is.EqualTo("a message"));
        });
    }

    [TestCase((sbyte)1, ClickHouseTcpServerLogLevel.Fatal)]
    [TestCase((sbyte)8, ClickHouseTcpServerLogLevel.Trace)]
    [TestCase((sbyte)9, ClickHouseTcpServerLogLevel.Test)]
    [TestCase((sbyte)0, ClickHouseTcpServerLogLevel.Unknown)]
    [TestCase((sbyte)10, ClickHouseTcpServerLogLevel.Unknown)]
    [TestCase((sbyte)-1, ClickHouseTcpServerLogLevel.Unknown)]
    public void OnLog_PriorityOutsideTheKnownRange_DegradesToUnknown(sbyte priority, ClickHouseTcpServerLogLevel expected)
    {
        // A server that grows a level must not break a caller who only wanted the message text, so an unmapped
        // priority is reported rather than refused. No real server sends one, which is why this is a unit test.
        var rows = new List<ClickHouseTcpServerLogRow>();
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnServerLog = rows.Add });

        using Block block = LogBlock(priority);
        handlers.OnLog(block);

        Assert.That(rows[0].Level, Is.EqualTo(expected));
    }

    [Test]
    public void OnLog_BlockMissingAColumn_ThrowsNamingIt()
    {
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnServerLog = _ => { } });

        using var block = new Block(
            string.Empty,
            default,
            1,
            new IColumn[] { PrimitiveColumn<uint>.FromValues("event_time", "DateTime", [1]) },
            null,
            default);

        ClickHouseProtocolException thrown = Assert.Throws<ClickHouseProtocolException>(() => handlers.OnLog(block));
        Assert.That(thrown.Message, Does.Contain("event_time_microseconds"));
    }

    [Test]
    public void OnLog_ColumnOfTheWrongType_ThrowsNamingTheTypeItGot()
    {
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnServerLog = _ => { } });

        using Block block = LogBlock(priority: 7, threadIdColumn: new ArrayColumn<string>("thread_id", "String", ["not a number"]));

        ClickHouseProtocolException thrown = Assert.Throws<ClickHouseProtocolException>(() => handlers.OnLog(block));
        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("thread_id"));
            Assert.That(thrown.Message, Does.Contain("String"));
        });
    }

    [Test]
    public void OnProfileEvents_ValueColumnOfAnotherWidth_ThrowsNamingIt()
    {
        // Every supported server sends Int64, so a different width means the packet is not the one this projects
        // and reinterpreting it would report wrong numbers rather than an error.
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnProfileEvent = _ => { } });

        using Block block = ProfileEventsBlock(PrimitiveColumn<ulong>.FromValues("value", "UInt64", [17]));

        ClickHouseProtocolException thrown = Assert.Throws<ClickHouseProtocolException>(() => handlers.OnProfileEvents(block));
        Assert.Multiple(() =>
        {
            Assert.That(thrown.Message, Does.Contain("value"));
            Assert.That(thrown.Message, Does.Contain("UInt64"));
        });
    }

    [Test]
    public void OnProfileEvents_WellFormedBlock_ProjectsEveryField()
    {
        var events = new List<ClickHouseTcpProfileEvent>();
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnProfileEvent = events.Add });

        using Block block = ProfileEventsBlock(PrimitiveColumn<long>.FromValues("value", "Int64", [99]));
        handlers.OnProfileEvents(block);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].CurrentTime, Is.EqualTo(DateTimeOffset.FromUnixTimeSeconds(1700000000)));
            Assert.That(events[0].HostName, Is.EqualTo("host-1"));
            Assert.That(events[0].ThreadId, Is.EqualTo(4242UL));
            Assert.That(events[0].Type, Is.EqualTo(ClickHouseTcpProfileEventType.Increment));
            Assert.That(events[0].Name, Is.EqualTo("SelectedRows"));
            Assert.That(events[0].Value, Is.EqualTo(99L));
        });
    }

    [TestCase((sbyte)2, ClickHouseTcpProfileEventType.Gauge)]
    [TestCase((sbyte)0, ClickHouseTcpProfileEventType.Unknown)]
    [TestCase((sbyte)3, ClickHouseTcpProfileEventType.Unknown)]
    public void OnProfileEvents_TypeOutsideTheKnownRange_DegradesToUnknown(sbyte type, ClickHouseTcpProfileEventType expected)
    {
        var events = new List<ClickHouseTcpProfileEvent>();
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnProfileEvent = events.Add });

        using Block block = ProfileEventsBlock(PrimitiveColumn<long>.FromValues("value", "Int64", [1]), type);
        handlers.OnProfileEvents(block);

        Assert.That(events[0].Type, Is.EqualTo(expected));
    }

    [Test]
    public void OnLog_ZeroRowBlock_InvokesNothing()
    {
        int calls = 0;
        MetadataHandlers handlers = MetadataCallbackBridge.Build(new ClickHouseTcpQueryCallbacks { OnServerLog = _ => calls++ });

        using Block block = LogBlock(priority: 7, rowCount: 0);
        handlers.OnLog(block);

        Assert.That(calls, Is.Zero);
    }

    // The server's fixed Log schema, in its documented order.
    private static Block LogBlock(sbyte priority, IColumn threadIdColumn = null, int rowCount = 1)
        => new(
            string.Empty,
            default,
            rowCount,
            new[]
            {
                PrimitiveColumn<uint>.FromValues("event_time", "DateTime", [1700000000]),
                PrimitiveColumn<uint>.FromValues("event_time_microseconds", "UInt32", [500000]),
                new ArrayColumn<string>("host_name", "String", ["host-1"]),
                new ArrayColumn<string>("query_id", "String", ["query-1"]),
                threadIdColumn ?? PrimitiveColumn<ulong>.FromValues("thread_id", "UInt64", [4242]),
                PrimitiveColumn<sbyte>.FromValues("priority", "Int8", [priority]),
                new ArrayColumn<string>("source", "String", ["executeQuery"]),
                new ArrayColumn<string>("text", "String", ["a message"]),
            },
            null,
            default);

    // The server's fixed ProfileEvents schema, in its documented order.
    private static Block ProfileEventsBlock(IColumn valueColumn, sbyte type = 1)
        => new(
            string.Empty,
            default,
            1,
            new[]
            {
                new ArrayColumn<string>("host_name", "String", ["host-1"]),
                PrimitiveColumn<uint>.FromValues("current_time", "DateTime", [1700000000]),
                PrimitiveColumn<ulong>.FromValues("thread_id", "UInt64", [4242]),
                PrimitiveColumn<sbyte>.FromValues("type", "Enum8('increment' = 1, 'gauge' = 2)", [type]),
                new ArrayColumn<string>("name", "String", ["SelectedRows"]),
                valueColumn,
            },
            null,
            default);
}
