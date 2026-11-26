namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Protocol features, each identified by the protocol version that introduced it. A feature is active when
/// the negotiated version is at least this value; every version-conditional read or write consults
/// <see cref="NegotiatedProtocol.Supports"/> rather than an inline magic number. Mirrors ClickHouse's
/// <c>DBMS_MIN_REVISION_WITH_*</c> constants.
/// </summary>
internal enum ProtocolFeature
{
    /// <summary>ServerHello carries a <c>timezone</c> string.</summary>
    Timezone = 54058,

    /// <summary>ClientInfo carries a <c>quota_key</c> string (distinct from the Addendum quota key).</summary>
    QuotaKeyInClientInfo = 54060,

    /// <summary>ServerHello carries a <c>display_name</c> string.</summary>
    DisplayName = 54372,

    /// <summary>ServerHello carries a <c>version_patch</c> VarUInt; ClientInfo carries <c>client_version_patch</c>.</summary>
    VersionPatch = 54401,

    /// <summary>Progress carries <c>wrote_rows</c> and <c>wrote_bytes</c> VarUInts.</summary>
    ProgressWriteInfo = 54420,

    /// <summary>Query settings are serialized as (key, flags, string-value) triples rather than the legacy binary form.</summary>
    SettingsAsStrings = 54429,

    /// <summary>The Query packet carries an <c>interserver_secret</c> string (empty for external clients).</summary>
    InterserverSecret = 54441,

    /// <summary>ClientInfo carries an OpenTelemetry trace block (a presence byte, plus trace context when present).</summary>
    OpenTelemetry = 54442,

    /// <summary>ClientInfo carries a <c>distributed_depth</c> VarUInt.</summary>
    DistributedDepth = 54448,

    /// <summary>ClientInfo carries an <c>initial_query_start_time_microseconds</c> Int64.</summary>
    InitialQueryStartTime = 54449,

    /// <summary>ClientInfo carries the parallel-replicas collaboration fields (collaborate flag, replica counts).</summary>
    ParallelReplicasClientInfo = 54453,

    /// <summary>Each block column carries a <c>has_custom_serialization</c> byte.</summary>
    CustomSerialization = 54454,

    /// <summary>The client sends an Addendum after the handshake exchange.</summary>
    Addendum = 54458,

    /// <summary>The Query packet carries a query-parameters list (name, flags, value) terminated by an empty key.</summary>
    Parameters = 54459,

    /// <summary>Progress carries an <c>elapsed_ns</c> VarUInt.</summary>
    ProgressElapsedNs = 54460,

    /// <summary>Per-packet chunk framing wraps every packet body; preferences are exchanged in ServerHello and Addendum.</summary>
    ChunkedProtocol = 54470,

    /// <summary>Both sides exchange a parallel-replicas coordination protocol version (ServerHello field 4a, Addendum tail).</summary>
    ParallelReplicas = 54471,
}
