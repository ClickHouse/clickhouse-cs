namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// The server identity and protocol details decoded from ServerHello. The timezone and display name are
/// blank when the negotiated version predates the features that introduced them.
/// </summary>
internal sealed record ServerHandshake
{
    /// <summary>Server identifier (e.g. <c>"ClickHouse"</c>).</summary>
    required public string ServerName { get; init; }

    /// <summary>Server major version.</summary>
    required public int VersionMajor { get; init; }

    /// <summary>Server minor version.</summary>
    required public int VersionMinor { get; init; }

    /// <summary>The server's protocol revision, as reported in ServerHello.</summary>
    required public int Revision { get; init; }

    /// <summary>Server patch version, or 0 when not present at the negotiated version.</summary>
    public int VersionPatch { get; init; }

    /// <summary>Server timezone (e.g. <c>"UTC"</c>), or empty when not present. Used for DateTime offset resolution.</summary>
    public string Timezone { get; init; } = string.Empty;

    /// <summary>Human-readable server name, or empty when not present.</summary>
    public string DisplayName { get; init; } = string.Empty;

    /// <summary>The version negotiated with this server: <c>min(client, <see cref="Revision"/>)</c>.</summary>
    public NegotiatedProtocol Negotiated => new(Revision);
}
