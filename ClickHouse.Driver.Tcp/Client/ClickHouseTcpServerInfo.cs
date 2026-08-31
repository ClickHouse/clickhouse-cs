using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// What the server said about itself in its ServerHello: its version, the protocol revision in use, and the
/// session defaults the client resolves timestamps against. Read it with
/// <see cref="IClickHouseTcpOperations.GetServerInfoAsync"/>.
/// </summary>
/// <remarks>
/// Three protocol revisions are reported, and they are usually three different numbers: what the server
/// advertised, what this client implements, and the negotiated one those two settle on.
/// <see cref="ProtocolRevision"/> is the one in force, and the one to gate a feature on.
/// </remarks>
public sealed record ClickHouseTcpServerInfo
{
    /// <summary>The server identifier, normally <c>"ClickHouse"</c>.</summary>
    public string Name { get; init; }

    /// <summary>The server's major version.</summary>
    public int VersionMajor { get; init; }

    /// <summary>The server's minor version.</summary>
    public int VersionMinor { get; init; }

    /// <summary>The server's patch version.</summary>
    public int VersionPatch { get; init; }

    /// <summary>
    /// The protocol revision in use for this connection: the lower of <see cref="ServerProtocolRevision"/> and
    /// <see cref="ClientProtocolRevision"/>, so it can be below what either alone offers. <b>Gate features on
    /// this number</b>, not on either of the other two.
    /// </summary>
    public int ProtocolRevision { get; init; }

    /// <summary>
    /// The protocol revision the server advertised in its ServerHello — what it can do, not what is in force.
    /// Higher than <see cref="ProtocolRevision"/> whenever the server is newer than this client.
    /// </summary>
    public int ServerProtocolRevision { get; init; }

    /// <summary>
    /// The protocol revision this client implements — a constant of this driver version, the same for every
    /// server. Higher than <see cref="ProtocolRevision"/> whenever the server is older than this client.
    /// </summary>
    public int ClientProtocolRevision { get; init; }

    /// <summary>
    /// The server's timezone (e.g. <c>"UTC"</c>), which is what a bare <c>DateTime</c> column is interpreted in.
    /// Empty when the server did not send one.
    /// </summary>
    public string Timezone { get; init; } = string.Empty;

    /// <summary>The server's configured display name, or empty when it sent none.</summary>
    public string DisplayName { get; init; } = string.Empty;

    /// <summary>
    /// The server version as a <see cref="System.Version"/>, for comparing against a required version.
    /// </summary>
    public Version Version => new(VersionMajor, VersionMinor, VersionPatch);

    /// <summary>Renders the server name and version, e.g. <c>"ClickHouse 25.8.1"</c>.</summary>
    /// <returns>The name followed by the version.</returns>
    public override string ToString() => $"{Name} {Version.ToString(3)}";
}
