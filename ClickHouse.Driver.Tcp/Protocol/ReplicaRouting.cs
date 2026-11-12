using System;
using System.Net;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Replica-aware routing for ClickHouse Cloud. A connection asks for a replica by the TLS server name (SNI) it sends:
/// <c>&lt;tag&gt;.sticky.&lt;host&gt;</c>. Connections that send the same tag go to the same replica while the
/// number of replicas does not change. The client still dials the normal host.
/// </summary>
/// <remarks>
/// The tag is fixed by the TLS handshake, so it belongs to a connection, not to a query. Cloud certificates name
/// <c>&lt;host&gt;</c> and <c>*.&lt;host&gt;</c>, which do not cover the two extra labels, so the certificate is
/// checked against the normal host name (see <see cref="TlsParameters"/>).
/// </remarks>
internal static class ReplicaRouting
{
    /// <summary>The label between the tag and the host name.</summary>
    internal const string StickyLabel = "sticky";

    // RFC 1035 limits: one label, and a whole name in its text form.
    private const int MaxLabelLength = 63;
    private const int MaxNameLength = 253;

    /// <summary>A tag as the client uses it: null for none. An empty tag means none, as an empty query id does.</summary>
    /// <param name="tag">The tag as given.</param>
    /// <returns>The tag, or null.</returns>
    internal static string Normalize(string tag) => string.IsNullOrEmpty(tag) ? null : tag;

    /// <summary>The name a connection with no tag sends as SNI and checks the certificate against.</summary>
    /// <param name="options">The client options.</param>
    /// <returns><see cref="ClickHouseTcpClientOptions.TlsServerName"/> when set, otherwise the host.</returns>
    internal static string ServerName(ClickHouseTcpClientOptions options)
        => string.IsNullOrEmpty(options.TlsServerName) ? options.Host : options.TlsServerName;

    /// <summary>The SNI that asks for the replica of <paramref name="tag"/>.</summary>
    /// <param name="tag">A valid tag.</param>
    /// <param name="serverName">The normal server name.</param>
    /// <returns><c>&lt;tag&gt;.sticky.&lt;serverName&gt;</c>.</returns>
    internal static string StickyServerName(string tag, string serverName)
        => string.Concat(tag, ".", StickyLabel, ".", serverName);

    /// <summary>
    /// Refuses a tag that cannot be sent as SNI with these options. A null or empty tag is always accepted: it asks
    /// for no routing.
    /// </summary>
    /// <param name="tag">The tag to check.</param>
    /// <param name="options">The client options the connection is opened with.</param>
    /// <param name="parameterName">The name to report in the exception.</param>
    /// <exception cref="ArgumentException">The client does not use TLS, the tag is not one DNS label, or the server
    /// name is not a DNS name.</exception>
    internal static void Validate(string tag, ClickHouseTcpClientOptions options, string parameterName)
    {
        if (string.IsNullOrEmpty(tag))
        {
            return;
        }

        if (!options.UseTls)
        {
            throw new ArgumentException(
                $"A replica tag needs TLS: the tag is sent as the TLS server name, and without TLS ClickHouse Cloud uses normal load balancing. Set {nameof(ClickHouseTcpClientOptions.UseTls)} to true, or remove the tag.",
                parameterName);
        }

        if (!IsLabel(tag))
        {
            throw new ArgumentException(
                $"The replica tag '{tag}' is not valid. A tag is sent as one DNS label, so it must have 1 to {MaxLabelLength} characters, use only the letters A-Z and a-z, the digits 0-9 and '-', and not start or end with '-'. To route by a value such as an email address, use a hash of it as the tag.",
                parameterName);
        }

        string serverName = ServerName(options);
        if (IPAddress.TryParse(serverName, out _) || Uri.CheckHostName(serverName) != UriHostNameType.Dns)
        {
            throw new ArgumentException(
                $"A replica tag needs a server host name, but '{serverName}' is not a DNS name. Set {nameof(ClickHouseTcpClientOptions.TlsServerName)} to the service host name.",
                parameterName);
        }

        if (StickyServerName(tag, serverName).Length > MaxNameLength)
        {
            throw new ArgumentException(
                $"The replica tag '{tag}' makes the TLS server name longer than {MaxNameLength} characters. Use a shorter tag.",
                parameterName);
        }
    }

    private static bool IsLabel(string tag)
    {
        if (tag.Length > MaxLabelLength || tag[0] == '-' || tag[^1] == '-')
        {
            return false;
        }

        foreach (char c in tag)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '-')
            {
                return false;
            }
        }

        return true;
    }
}
