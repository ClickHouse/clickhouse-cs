using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// One server performance counter, streamed to the client while a query runs. The same counter arrives
/// repeatedly as the query progresses.
/// </summary>
/// <remarks>
/// Every member is owned, so an event is safe to keep after the callback returns.
/// </remarks>
public readonly record struct ClickHouseTcpProfileEvent
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpProfileEvent"/> struct.</summary>
    /// <param name="currentTime">When the server sampled the counter.</param>
    /// <param name="hostName">The server host the counter came from.</param>
    /// <param name="threadId">The OS thread the counter belongs to, or 0 for a query-wide total.</param>
    /// <param name="type">Whether <paramref name="value"/> is an increment or a reading.</param>
    /// <param name="name">The counter name.</param>
    /// <param name="value">The increment or the reading.</param>
    public ClickHouseTcpProfileEvent(
        DateTimeOffset currentTime,
        string hostName,
        ulong threadId,
        ClickHouseTcpProfileEventType type,
        string name,
        long value)
    {
        CurrentTime = currentTime;
        HostName = hostName;
        ThreadId = threadId;
        Type = type;
        Name = name;
        Value = value;
    }

    /// <summary>When the server sampled the counter, as a UTC instant.</summary>
    public DateTimeOffset CurrentTime { get; }

    /// <summary>The server host the counter came from.</summary>
    public string HostName { get; }

    /// <summary>The OS thread the counter belongs to, or 0 for a query-wide total.</summary>
    public ulong ThreadId { get; }

    /// <summary>Whether <see cref="Value"/> is an increment to add up or a reading to take as it stands.</summary>
    public ClickHouseTcpProfileEventType Type { get; }

    /// <summary>The counter name, e.g. <c>Query</c> or <c>NetworkReceiveBytes</c>.</summary>
    public string Name { get; }

    /// <summary>The increment or the reading, per <see cref="Type"/>.</summary>
    public long Value { get; }
}

/// <summary>How to read the <see cref="ClickHouseTcpProfileEvent.Value"/> of a profile event.</summary>
public enum ClickHouseTcpProfileEventType
{
    /// <summary>The server sent a type outside the range it documents.</summary>
    Unknown = 0,

    /// <summary>An increment: add it to the running total for this counter.</summary>
    Increment = 1,

    /// <summary>A reading at a point in time: it replaces the previous one rather than adding to it.</summary>
    Gauge = 2,
}
