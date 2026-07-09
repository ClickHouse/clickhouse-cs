using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Encodes and decodes the connection handshake — ClientHello, ServerHello, and the Addendum — and runs the
/// exchange end to end. At the current negotiation ceiling there is no chunk framing, so every message rides
/// straight on the buffered reader/writer.
/// </summary>
internal static class Handshake
{
    /// <summary>Writes the ClientHello packet (type code plus its seven fields). Does not flush.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="parameters">The client-supplied handshake values.</param>
    public static void WriteClientHello(ClickHouseBinaryWriter writer, ClientHandshakeParameters parameters)
    {
        writer.WriteClientPacketType(ClientPacketType.Hello);
        writer.WriteString(parameters.ClientName);
        writer.WriteVarUInt((ulong)parameters.VersionMajor);
        writer.WriteVarUInt((ulong)parameters.VersionMinor);
        writer.WriteVarUInt(NegotiatedProtocol.ClientTcpProtocolVersion);
        writer.WriteString(parameters.Database);
        writer.WriteString(parameters.Username);
        writer.WriteString(parameters.Password);
    }

    /// <summary>
    /// Decodes the ServerHello body (the bytes after a <see cref="ServerPacketType.Hello"/> type code).
    /// Version-gated fields are read only when the negotiated version — computed from the server's reported
    /// revision — activates them, so an unexpected revision reads exactly the fields the server wrote.
    /// </summary>
    /// <param name="reader">The reader positioned at the start of the ServerHello body.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded server handshake.</returns>
    public static async ValueTask<ServerHandshake> ReadServerHelloAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        string serverName = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
        int versionMajor = (int)await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        int versionMinor = (int)await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);

        // Guard the revision before narrowing: a corrupt value with bit 31 set would wrap negative, drive the
        // negotiated version negative, and silently skip every gated field below — desyncing the stream.
        ulong revisionRaw = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        if (revisionRaw > int.MaxValue)
        {
            throw new InvalidDataException($"Server reported an implausible protocol revision {revisionRaw} (corrupt stream).");
        }

        int revision = (int)revisionRaw;
        var negotiated = new NegotiatedProtocol(revision);

        // The negotiated version is capped at the client ceiling, below every field between the revision and
        // the timezone (the parallel-replicas version gates at 54471). Anyone raising the ceiling must decode
        // that field here, immediately after the revision, before the timezone.
        string timezone = negotiated.Supports(ProtocolFeature.Timezone)
            ? await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false)
            : string.Empty;
        string displayName = negotiated.Supports(ProtocolFeature.DisplayName)
            ? await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false)
            : string.Empty;
        int versionPatch = negotiated.Supports(ProtocolFeature.VersionPatch)
            ? (int)await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false)
            : 0;

        // Everything beyond version_patch (chunked prefs, password rules, nonce, server settings, ...) gates
        // above the ceiling and is therefore absent.
        return new ServerHandshake
        {
            ServerName = serverName,
            VersionMajor = versionMajor,
            VersionMinor = versionMinor,
            Revision = revision,
            Timezone = timezone,
            DisplayName = displayName,
            VersionPatch = versionPatch,
        };
    }

    /// <summary>
    /// Writes the Addendum. It has no packet type prefix — the fields go on the wire raw. At the current
    /// ceiling this reduces to the quota key; the chunked-framing and parallel-replicas fields gate higher.
    /// Does not flush.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="quotaKey">The keyed-quota resource key (empty when unused).</param>
    public static void WriteAddendum(ClickHouseBinaryWriter writer, string quotaKey)
        => writer.WriteString(quotaKey);

    /// <summary>
    /// Runs the full handshake: sends ClientHello, reads the reply, and — on success — sends the Addendum
    /// when the negotiated version calls for it. A server Exception reply is decoded and thrown; any other
    /// reply is a protocol violation.
    /// </summary>
    /// <param name="reader">The reader over the server stream.</param>
    /// <param name="writer">The writer over the client stream.</param>
    /// <param name="parameters">The client-supplied handshake values.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded server handshake, including the negotiated version.</returns>
    /// <exception cref="ClickHouseServerException">The server rejected the handshake (e.g. authentication failure).</exception>
    /// <exception cref="ClickHouseProtocolException">The server sent neither Hello nor Exception.</exception>
    /// <exception cref="NotSupportedException">The server negotiated below the minimum protocol version this client supports.</exception>
    public static async ValueTask<ServerHandshake> PerformAsync(
        ClickHouseBinaryReader reader,
        ClickHouseBinaryWriter writer,
        ClientHandshakeParameters parameters,
        CancellationToken cancellationToken)
    {
        WriteClientHello(writer, parameters);
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

        ServerPacketType reply = await reader.ReadServerPacketTypeAsync(cancellationToken).ConfigureAwait(false);
        switch (reply)
        {
            case ServerPacketType.Hello:
                break;
            case ServerPacketType.Exception:
                throw await ClickHouseServerException.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
            default:
                throw new ClickHouseProtocolException($"Unexpected packet type {reply} ({(ulong)reply}) during handshake; expected Hello or Exception.");
        }

        ServerHandshake server = await ReadServerHelloAsync(reader, cancellationToken).ConfigureAwait(false);

        // The write path assumes string-serialized settings (the form introduced at the floor). A server that
        // negotiates below it would desync rather than merely lose features, so refuse it with a clear error
        // instead of corrupting the connection.
        if (server.Negotiated.Version < NegotiatedProtocol.MinimumTcpProtocolVersion)
        {
            throw new NotSupportedException(
                $"The ClickHouse server's protocol revision {server.Revision} is older than the minimum " +
                $"{NegotiatedProtocol.MinimumTcpProtocolVersion} this client supports.");
        }

        if (server.Negotiated.Supports(ProtocolFeature.Addendum))
        {
            WriteAddendum(writer, parameters.QuotaKey);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        return server;
    }
}
