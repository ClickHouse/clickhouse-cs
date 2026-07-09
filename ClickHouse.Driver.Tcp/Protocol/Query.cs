using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Writes the Query packet: query id, the ClientInfo block, per-query settings, the SQL text, and query
/// parameters. Settings and parameters are (key, flags, value) string triples terminated by an empty key.
/// This string-serialized form is unconditional; the handshake rejects any server that negotiates below the
/// version where it became the wire format, so it is always the form the peer expects.
/// </summary>
internal static class Query
{
    private const byte StageComplete = 2;      // process the query to completion (not just parse/plan)
    private const ulong ParameterFlagCustom = 0x02;

    // The block reader decodes textual column type headers. Enabling binary type headers would change the
    // Native block layout and desync the reader, so if the caller sets this we force it back to the textual
    // form. We only override a value the caller supplied: servers that predate the setting reject unknown
    // settings, and its default is already the textual form we rely on, so injecting it unprompted would be
    // both unnecessary and unsafe against older servers.
    private const string NativeBinaryTypesSetting = "output_format_native_encode_types_in_binary_format";
    private const string NativeBinaryTypesDisabled = "0";

    /// <summary>Writes the Query packet body. Does not flush and does not write the following data marker.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="negotiated">The negotiated protocol, gating version-conditional fields.</param>
    /// <param name="client">The client identity for the embedded ClientInfo.</param>
    /// <param name="queryId">The query id (empty to let the server assign one).</param>
    /// <param name="sql">The SQL text.</param>
    /// <param name="settings">Per-query settings as textual values, or null for none.</param>
    /// <param name="queryParameters">Query parameter values (already in SQL representation), or null for none.</param>
    public static void Write(
        ClickHouseBinaryWriter writer,
        NegotiatedProtocol negotiated,
        ClientMetadata client,
        string queryId,
        string sql,
        IReadOnlyDictionary<string, string> settings,
        IReadOnlyDictionary<string, string> queryParameters)
    {
        writer.WriteClientPacketType(ClientPacketType.Query);
        writer.WriteString(queryId ?? string.Empty);

        ClientInfo.Write(writer, negotiated, client);

        WriteSettings(writer, settings);

        if (negotiated.Supports(ProtocolFeature.InterserverSecret))
        {
            writer.WriteString(string.Empty);      // interserver_secret: empty for external clients
        }

        writer.WriteByte(StageComplete);
        writer.WriteBool(false);                   // compression disabled

        writer.WriteString(sql);

        if (negotiated.Supports(ProtocolFeature.Parameters))
        {
            WriteParameters(writer, queryParameters);
        }
        else if (queryParameters is { Count: > 0 })
        {
            // The parameters list is not part of the wire format below this version, so it cannot be sent.
            // Fail loudly rather than dropping the parameters and running a query that silently means something
            // different (or errors on the server with an opaque "unknown parameter" message).
            throw new NotSupportedException(
                $"The server's negotiated protocol revision {negotiated.Version} does not support query parameters " +
                $"(introduced in revision {(int)ProtocolFeature.Parameters}); pass none or use a newer server.");
        }
    }

    private static void WriteSettings(ClickHouseBinaryWriter writer, IReadOnlyDictionary<string, string> settings)
    {
        if (settings is not null)
        {
            foreach (KeyValuePair<string, string> setting in settings)
            {
                writer.WriteString(setting.Key);
                writer.WriteVarUInt(0);            // flags: neither important nor custom

                // Force the reader-desyncing binary-type-header setting back off if the caller enabled it.
                string value = string.Equals(setting.Key, NativeBinaryTypesSetting, StringComparison.Ordinal)
                    ? NativeBinaryTypesDisabled
                    : setting.Value;
                writer.WriteString(value);
            }
        }

        writer.WriteString(string.Empty);          // empty key terminates the list
    }

    private static void WriteParameters(ClickHouseBinaryWriter writer, IReadOnlyDictionary<string, string> queryParameters)
    {
        if (queryParameters is not null)
        {
            foreach (KeyValuePair<string, string> parameter in queryParameters)
            {
                writer.WriteString(parameter.Key);
                writer.WriteVarUInt(ParameterFlagCustom);
                writer.WriteString(parameter.Value);
            }
        }

        writer.WriteString(string.Empty);          // empty key terminates the list
    }
}
