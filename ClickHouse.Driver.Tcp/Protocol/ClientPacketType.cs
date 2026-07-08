namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Packet type codes the client sends to the server, encoded as the leading VarUInt of each packet.
/// Codes overlap numerically with <see cref="ServerPacketType"/> but carry different meanings per direction
/// (e.g. code 2 is client <see cref="Data"/> but server <see cref="ServerPacketType.Exception"/>).
///
/// <para>
/// Backed by <c>ulong</c> to match the VarUInt wire type, so casting to and from the encoded value never
/// narrows.
/// </para>
/// </summary>
internal enum ClientPacketType : ulong
{
    /// <summary>Handshake initiation (<c>ClientHello</c>).</summary>
    Hello = 0,

    /// <summary>Query execution request.</summary>
    Query = 1,

    /// <summary>Data block: INSERT data, external tables, or the empty end-of-input marker.</summary>
    Data = 2,

    /// <summary>Cancel the running query (no body).</summary>
    Cancel = 3,

    /// <summary>Liveness check.</summary>
    Ping = 4,

    /// <summary>Table status check.</summary>
    TablesStatusRequest = 5,

    /// <summary>Connection keepalive.</summary>
    KeepAlive = 6,

    /// <summary>Scalar data block.</summary>
    Scalar = 7,

    /// <summary>Parts to exclude from the query.</summary>
    IgnoredPartUUIDs = 8,

    /// <summary>S3 cluster read response.</summary>
    ReadTaskResponse = 9,

    /// <summary>Parallel read task response.</summary>
    MergeTreeReadTaskResponse = 10,

    /// <summary>SSH auth challenge request.</summary>
    SSHChallengeRequest = 11,

    /// <summary>SSH auth challenge response.</summary>
    SSHChallengeResponse = 12,

    /// <summary>Query plan.</summary>
    QueryPlan = 13,

    /// <summary>Initiator's reply to a follower's parallel-read announcement. Inter-server only — external clients never send this.</summary>
    MergeTreeAllRangesAnnouncementResponse = 14,
}
