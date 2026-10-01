namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Packet type codes the server sends to the client, encoded as the leading VarUInt of each packet.
/// Codes overlap numerically with <see cref="ClientPacketType"/> but carry different meanings per direction
/// (e.g. code 2 is server <see cref="Exception"/> but client <see cref="ClientPacketType.Data"/>).
///
/// <para>
/// Backed by <c>ulong</c> to match the VarUInt wire type, so casting to and from the encoded value never
/// narrows. A read yields the raw code cast to this enum with no range validation — the packet dispatcher
/// owns unknown-code handling.
/// </para>
/// </summary>
internal enum ServerPacketType : ulong
{
    /// <summary>Handshake response (<c>ServerHello</c>).</summary>
    Hello = 0,

    /// <summary>Result data block.</summary>
    Data = 1,

    /// <summary>Error.</summary>
    Exception = 2,

    /// <summary>Query execution progress.</summary>
    Progress = 3,

    /// <summary>Liveness response.</summary>
    Pong = 4,

    /// <summary>Query complete (no body).</summary>
    EndOfStream = 5,

    /// <summary>Post-execution profiling data.</summary>
    ProfileInfo = 6,

    /// <summary>GROUP BY WITH TOTALS row.</summary>
    Totals = 7,

    /// <summary>Min/max values (a two-row block).</summary>
    Extremes = 8,

    /// <summary>Table status response.</summary>
    TablesStatusResponse = 9,

    /// <summary>Query execution log lines.</summary>
    Log = 10,

    /// <summary>Column descriptions for defaults.</summary>
    TableColumns = 11,

    /// <summary>Unique part IDs.</summary>
    PartUUIDs = 12,

    /// <summary>Cluster read task request.</summary>
    ReadTaskRequest = 13,

    /// <summary>Performance counters.</summary>
    ProfileEvents = 14,

    /// <summary>Parallel read initialization. Inter-server only.</summary>
    MergeTreeAllRangesAnnouncement = 15,

    /// <summary>Parallel read task assignment. Inter-server only.</summary>
    MergeTreeReadTaskRequest = 16,

    /// <summary>Server timezone update.</summary>
    TimezoneUpdate = 17,

    /// <summary>SSH auth challenge.</summary>
    SSHChallenge = 18,
}
