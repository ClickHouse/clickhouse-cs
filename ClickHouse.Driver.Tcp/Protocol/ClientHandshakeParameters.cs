using System.Globalization;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Client-supplied values for the handshake: the ClientHello fields plus the Addendum's quota key. The
/// connection layer builds this from client options; the handshake code treats it as opaque input.
/// </summary>
internal sealed class ClientHandshakeParameters
{
    /// <summary>The product identifier this driver reports on the wire, for both the handshake and every query.</summary>
    public const string DefaultClientName = "clickhouse-cs-tcp";

    /// <summary>Informational client identifier (ClientHello field 1). Pinned to <see cref="DefaultClientName"/>
    /// and not caller-configurable, so the handshake and every query's ClientInfo report the same name. Does not
    /// affect negotiation.</summary>
    public string ClientName => DefaultClientName;

    /// <summary>Client major version (ClientHello field 2). Defaults to this driver's version.</summary>
    public int VersionMajor { get; init; } = ClientVersion.Current.Major;

    /// <summary>Client minor version (ClientHello field 3). Defaults to this driver's version.</summary>
    public int VersionMinor { get; init; } = ClientVersion.Current.Minor;

    /// <summary>
    /// Client patch version. ClientHello has no field for it; every query's ClientInfo carries it. Defaults to this
    /// driver's version.
    /// </summary>
    public int VersionPatch { get; init; } = ClientVersion.Current.Patch;

    /// <summary>Default database (ClientHello field 5).</summary>
    public string Database { get; init; } = "default";

    /// <summary>Username for authentication (ClientHello field 6).</summary>
    required public string Username { get; init; }

    /// <summary>Password, sent in plaintext and protected only by TLS (ClientHello field 7).</summary>
    public string Password { get; init; } = string.Empty;

    /// <summary>Keyed-quota resource key (Addendum field 1). Empty when the client uses no keyed quota.</summary>
    public string QuotaKey { get; init; } = string.Empty;

    /// <inheritdoc/>
    public override string ToString()
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{nameof(ClientHandshakeParameters)} {{ ClientName = {ClientName}, Version = {VersionMajor}.{VersionMinor}.{VersionPatch}, Database = {Database}, Username = {Username}, Password = <redacted>, QuotaKey = <redacted> }}");
}
