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

    /// <summary>ServerHello carries a <c>display_name</c> string.</summary>
    DisplayName = 54372,

    /// <summary>ServerHello carries a <c>version_patch</c> VarUInt.</summary>
    VersionPatch = 54401,

    /// <summary>The client sends an Addendum after the handshake exchange.</summary>
    Addendum = 54458,

    /// <summary>Per-packet chunk framing wraps every packet body; preferences are exchanged in ServerHello and Addendum.</summary>
    ChunkedProtocol = 54470,

    /// <summary>Both sides exchange a parallel-replicas coordination protocol version (ServerHello field 4a, Addendum tail).</summary>
    ParallelReplicas = 54471,
}
