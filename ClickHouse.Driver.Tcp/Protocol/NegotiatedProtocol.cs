using System;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// The protocol version agreed for a connection — <c>min(client, server)</c> — and the single authority for
/// which version-gated wire fields are present. Every conditional read/write keys off <see cref="Supports"/>.
/// </summary>
internal readonly struct NegotiatedProtocol
{
    /// <summary>
    /// The protocol version this client advertises in ClientHello, and the ceiling for negotiation.
    /// </summary>
    public const int ClientTcpProtocolVersion = 54460;

    /// <summary>
    /// The lowest negotiated version this client can talk to. The Query packet always serializes settings as
    /// string triples (the form introduced at this version); below it the server expects the legacy binary
    /// encoding, which this client does not emit — so a lower negotiation would silently desync the stream.
    /// Negotiating below this floor is rejected at the handshake rather than left to corrupt the connection.
    /// </summary>
    public const int MinimumTcpProtocolVersion = 54429;

    /// <summary>Computes the negotiated version from the revision the server reported in ServerHello.</summary>
    /// <param name="serverRevision">The server's protocol revision (ServerHello field 4).</param>
    public NegotiatedProtocol(int serverRevision) => Version = Math.Min(ClientTcpProtocolVersion, serverRevision);

    /// <summary>The agreed protocol version: <c>min(client, server)</c>.</summary>
    public int Version { get; }

    /// <summary>Whether <paramref name="feature"/> is active at the negotiated version.</summary>
    /// <param name="feature">The feature to test.</param>
    /// <returns><c>true</c> when the negotiated version is at least the feature's introducing version.</returns>
    public bool Supports(ProtocolFeature feature) => Version >= (int)feature;
}
