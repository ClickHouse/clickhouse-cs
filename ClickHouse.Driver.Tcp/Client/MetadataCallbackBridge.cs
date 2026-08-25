using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// Turns the public <see cref="ClickHouseTcpQueryCallbacks"/> into the wire-shaped
/// <see cref="MetadataHandlers"/> the connection drains into, and projects the two fixed-schema metadata blocks
/// into owned rows on the way.
/// </summary>
/// <remarks>
/// The projection is what keeps a borrowed block from reaching a caller who cannot know it is about to be
/// released. Log and ProfileEvents blocks have a schema the server fixes, so a row type is safe to publish;
/// Totals and Extremes carry the query's own result shape, so they stay blocks.
/// </remarks>
internal static class MetadataCallbackBridge
{
    /// <summary>
    /// Builds the handlers for one operation, or null when nothing at all is listening — which keeps the read
    /// path's null check the whole cost of the feature for a caller who set no callbacks.
    /// </summary>
    /// <param name="callbacks">The caller's callbacks, or null.</param>
    /// <param name="onProgress">A client-internal progress observer to run before the caller's, or null.</param>
    /// <param name="onProfileInfo">A client-internal summary observer to run before the caller's, or null.</param>
    /// <returns>The handlers to drain into, or null.</returns>
    public static MetadataHandlers Build(
        ClickHouseTcpQueryCallbacks callbacks,
        Action<ClickHouseTcpProgress> onProgress = null,
        Action<ClickHouseTcpProfileInfo> onProfileInfo = null)
    {
        Action<ClickHouseTcpServerLogRow> serverLog = callbacks?.OnServerLog;
        Action<ClickHouseTcpProfileEvent> profileEvent = callbacks?.OnProfileEvent;
        Action<ClickHouseTcpProgress> progress = Combine(onProgress, callbacks?.OnProgress);
        Action<ClickHouseTcpProfileInfo> profileInfo = Combine(onProfileInfo, callbacks?.OnProfileInfo);
        Action<Block> totals = callbacks?.OnTotals;
        Action<Block> extremes = callbacks?.OnExtremes;

        if (progress is null && profileInfo is null && serverLog is null && profileEvent is null && totals is null && extremes is null)
        {
            return null;
        }

        return new MetadataHandlers
        {
            OnProgress = progress,
            OnProfileInfo = profileInfo,
            OnTotals = totals,
            OnExtremes = extremes,
            OnLog = serverLog is null ? null : block => ProjectServerLog(block, serverLog),
            OnProfileEvents = profileEvent is null ? null : block => ProjectProfileEvents(block, profileEvent),
        };
    }

    // The client's own observer runs first, so a caller's callback that throws cannot rob the client of the
    // telemetry it already had in hand.
    private static Action<T> Combine<T>(Action<T> first, Action<T> second)
    {
        if (first is null)
        {
            return second;
        }

        return second is null ? first : first + second;
    }

    private static void ProjectServerLog(Block block, Action<ClickHouseTcpServerLogRow> callback)
    {
        const string Kind = "Log";

        ReadOnlySpan<uint> eventTime = Column<uint>(block, Kind, "event_time").Values;
        ReadOnlySpan<uint> microseconds = Column<uint>(block, Kind, "event_time_microseconds").Values;
        ReadOnlySpan<ulong> threadId = Column<ulong>(block, Kind, "thread_id").Values;
        ReadOnlySpan<sbyte> priority = Column<sbyte>(block, Kind, "priority").Values;
        IColumn<string> hostName = Column<string>(block, Kind, "host_name");
        IColumn<string> queryId = Column<string>(block, Kind, "query_id");
        IColumn<string> source = Column<string>(block, Kind, "source");
        IColumn<string> text = Column<string>(block, Kind, "text");

        for (int row = 0; row < block.RowCount; row++)
        {
            callback(new ClickHouseTcpServerLogRow(
                Instant(eventTime[row], microseconds[row]),
                hostName[row],
                queryId[row],
                threadId[row],
                LogLevel(priority[row]),
                source[row],
                text[row]));
        }
    }

    private static void ProjectProfileEvents(Block block, Action<ClickHouseTcpProfileEvent> callback)
    {
        const string Kind = "ProfileEvents";

        ReadOnlySpan<uint> currentTime = Column<uint>(block, Kind, "current_time").Values;
        ReadOnlySpan<ulong> threadId = Column<ulong>(block, Kind, "thread_id").Values;
        ReadOnlySpan<sbyte> type = Column<sbyte>(block, Kind, "type").Values;
        IColumn<string> hostName = Column<string>(block, Kind, "host_name");
        IColumn<string> name = Column<string>(block, Kind, "name");

        // A counter is a signed count: a gauge can fall as well as rise. Every supported server sends Int64
        // (checked against 25.8, the oldest), so any other width is a protocol change to fail on rather than
        // reinterpret.
        ReadOnlySpan<long> values = Column<long>(block, Kind, "value").Values;

        for (int row = 0; row < block.RowCount; row++)
        {
            callback(new ClickHouseTcpProfileEvent(
                Instant(currentTime[row], 0),
                hostName[row],
                threadId[row],
                EventType(type[row]),
                name[row],
                values[row]));
        }
    }

    // The seconds column is a bare DateTime, so its value is a Unix instant with no timezone of its own.
    private static DateTimeOffset Instant(uint unixSeconds, uint microseconds)
        => DateTimeOffset.FromUnixTimeSeconds(unixSeconds).AddTicks(microseconds * TimeSpan.TicksPerMicrosecond);

    // An out-of-range value becomes Unknown rather than throwing: a server that grows a level must not break a
    // caller who only wanted the message text.
    private static ClickHouseTcpServerLogLevel LogLevel(sbyte priority)
        => priority is >= (sbyte)ClickHouseTcpServerLogLevel.Fatal and <= (sbyte)ClickHouseTcpServerLogLevel.Test
            ? (ClickHouseTcpServerLogLevel)priority
            : ClickHouseTcpServerLogLevel.Unknown;

    private static ClickHouseTcpProfileEventType EventType(sbyte type)
        => type is >= (sbyte)ClickHouseTcpProfileEventType.Increment and <= (sbyte)ClickHouseTcpProfileEventType.Gauge
            ? (ClickHouseTcpProfileEventType)type
            : ClickHouseTcpProfileEventType.Unknown;

    private static IColumn<T> Column<T>(Block block, string kind, string name)
    {
        IColumn column = Column(block, kind, name);
        return column as IColumn<T>
            ?? throw new ClickHouseProtocolException(
                $"{kind} column '{name}' has type '{column.TypeName}', which does not read as {typeof(T).Name}.");
    }

    // Walks the columns rather than Block.ColumnNames, which would materialize and cache a string[] for every
    // metadata block, and these arrive repeatedly through a query.
    private static IColumn Column(Block block, string kind, string name)
    {
        IReadOnlyList<IColumn> columns = block.Columns;
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, name, StringComparison.Ordinal))
            {
                return columns[i];
            }
        }

        throw new ClickHouseProtocolException($"{kind} block has no column named '{name}'.");
    }
}
