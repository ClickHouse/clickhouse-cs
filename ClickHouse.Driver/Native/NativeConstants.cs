namespace ClickHouse.Driver.Native;

/// <summary>
/// Client packet type codes (client -> server) for the ClickHouse Native TCP protocol.
/// </summary>
internal enum ClientPacketType
{
    Hello = 0,
    Query = 1,
    Data = 2,
    Cancel = 3,
    Ping = 4,
}

/// <summary>
/// Server packet type codes (server -> client) for the ClickHouse Native TCP protocol.
/// </summary>
internal enum ServerPacketType
{
    Hello = 0,
    Data = 1,
    Exception = 2,
    Progress = 3,
    Pong = 4,
    EndOfStream = 5,
    ProfileInfo = 6,
    Totals = 7,
    Extremes = 8,
    TablesStatusResponse = 9,
    Log = 10,
    TableColumns = 11,
    PartUUIDs = 12,
    ReadTaskRequest = 13,
    ProfileEvents = 14,
    MergeTreeAllRangesAnnouncement = 15,
    MergeTreeReadTaskRequest = 16,
    TimezoneUpdate = 17,
}

/// <summary>
/// Constants for the ClickHouse Native protocol. This MVP implementation advertises protocol
/// revision 54460, which is the highest revision where every field this client sends/parses is
/// active while chunked framing (54470), password-complexity rules (54461), the inter-server
/// nonce (54462) and server-settings broadcast (54474) are all still <em>above</em> the negotiated
/// version and therefore omitted by the server. The effective negotiated revision is
/// min(<see cref="ClientProtocolVersion"/>, server revision); all feature gates below are checked
/// against that negotiated value.
/// </summary>
internal static class NativeConstants
{
    public const int ClientProtocolVersion = 54460;
    public const string ClientName = "ClickHouse .NET Driver";
    public const int ClientVersionMajor = 1;
    public const int ClientVersionMinor = 0;

    // Feature gates (minimum negotiated revision at which a wire field becomes present).
    public const int MinRevisionWithBlockInfo = 51903;
    public const int MinRevisionWithServerTimezone = 54058;
    public const int MinRevisionWithQuotaKeyInClientInfo = 54060;
    public const int MinRevisionWithServerDisplayName = 54372;
    public const int MinRevisionWithVersionPatch = 54401;
    public const int MinRevisionWithClientInfo = 54420;
    public const int MinRevisionWithSettingsSerializedAsStrings = 54429;
    public const int MinRevisionWithInterserverSecret = 54441;
    public const int MinRevisionWithOpenTelemetry = 54442;
    public const int MinRevisionWithDistributedDepth = 54448;
    public const int MinRevisionWithInitialQueryStartTime = 54449;
    public const int MinRevisionWithParallelReplicas = 54453;
    public const int MinRevisionWithCustomSerialization = 54454;
    public const int MinRevisionWithAddendum = 54458;
    public const int MinRevisionWithParameters = 54459;
    public const int MinRevisionWithServerQueryTimeInProgress = 54460;
    public const int MinRevisionWithTotalBytesInProgress = 54463;
    public const int MinRevisionWithRowsBeforeAggregation = 54469;
    public const int MinRevisionWithExternallyGrantedRoles = 54472;

    // Query stages.
    public const int StageComplete = 2;
}
