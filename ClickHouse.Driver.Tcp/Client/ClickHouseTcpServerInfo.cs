using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// What the server said about itself in its ServerHello: its version, the protocol revision in use, and the
/// session defaults the client resolves timestamps against. Read it with
/// <see cref="IClickHouseTcpOperations.GetServerInfoAsync"/>.
/// </summary>
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
    /// The protocol revision in use for this connection: the lower of what the client and the server support, so
    /// it can be below what either alone offers. Feature gates are decided against this number.
    /// </summary>
    public int ProtocolRevision { get; init; }

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
