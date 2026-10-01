using System;
using System.Diagnostics;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Writes the ClientInfo block embedded in a Query packet (the TCP-interface branch). Identifies the query's
/// origin and the client. Fields gate on the negotiated version; at the current ceiling every gate is active.
/// The client sends a plain initial query with no distributed context and no parallel-replica coordination, so
/// those fields are their zero/absent forms. The trace-context field carries the ambient
/// <see cref="Activity"/> when there is one.
/// </summary>
internal static class ClientInfo
{
    // A free-form address string used only for server-side logging of the initiating client.
    private const string InitialAddress = "0.0.0.0:0";

    private static readonly string OsUser = TryGet(() => Environment.UserName);
    private static readonly string Hostname = TryGet(() => Environment.MachineName);

    /// <summary>Writes the ClientInfo fields in wire order. Does not flush.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="negotiated">The negotiated protocol, gating version-conditional fields.</param>
    /// <param name="client">The client identity retained after authentication.</param>
    public static void Write(ClickHouseBinaryWriter writer, NegotiatedProtocol negotiated, ClientMetadata client)
    {
        writer.WriteByte(1);                        // query_kind = InitialQuery
        writer.WriteString(client.Username);        // initial_user
        writer.WriteString(string.Empty);           // initial_query_id (always empty for an initial query)
        writer.WriteString(InitialAddress);         // initial_address

        if (negotiated.Supports(ProtocolFeature.InitialQueryStartTime))
        {
            writer.WriteInt64(0);                   // initial_query_start_time_microseconds
        }

        writer.WriteByte(1);                        // interface = TCP
        writer.WriteString(OsUser);                 // os_user
        writer.WriteString(Hostname);               // client_hostname
        writer.WriteString(client.ClientName);      // client_name
        writer.WriteVarUInt((ulong)client.VersionMajor);
        writer.WriteVarUInt((ulong)client.VersionMinor);
        writer.WriteVarUInt(NegotiatedProtocol.ClientTcpProtocolVersion);

        if (negotiated.Supports(ProtocolFeature.QuotaKeyInClientInfo))
        {
            writer.WriteString(client.QuotaKey);
        }

        if (negotiated.Supports(ProtocolFeature.DistributedDepth))
        {
            writer.WriteVarUInt(0);                 // distributed_depth
        }

        if (negotiated.Supports(ProtocolFeature.VersionPatch))
        {
            writer.WriteVarUInt(0);                 // client_version_patch
        }

        if (negotiated.Supports(ProtocolFeature.OpenTelemetry))
        {
            WriteTraceContext(writer, Activity.Current);
        }

        if (negotiated.Supports(ProtocolFeature.ParallelReplicasClientInfo))
        {
            writer.WriteVarUInt(0);                 // collaborate_with_initiator
            writer.WriteVarUInt(0);                 // count_participating_replicas
            writer.WriteVarUInt(0);                 // number_of_current_replica
        }
    }

    /// <summary>
    /// Writes the W3C trace context of <paramref name="activity"/>, so the spans the server records for this
    /// query (<c>system.opentelemetry_span_log</c>) hang off the caller's trace rather than starting their own.
    /// Writes the absent form when there is no span, or when its id is not W3C — a hierarchical id has no
    /// trace id to send.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="activity">The span to propagate, or null for none.</param>
    internal static void WriteTraceContext(ClickHouseBinaryWriter writer, Activity activity)
    {
        if (activity is null || activity.IdFormat != ActivityIdFormat.W3C)
        {
            writer.WriteByte(0);                    // has_trace = 0
            return;
        }

        writer.WriteByte(1);                        // has_trace = 1

        // trace_id is a UUID on the wire: two 8-byte halves, each written little-endian. A W3C trace id is 16
        // big-endian bytes, so each half is reversed. span_id is a plain little-endian UInt64, so it reverses
        // whole.
        Span<byte> id = stackalloc byte[16];
        activity.TraceId.CopyTo(id);
        id[..8].Reverse();
        id[8..].Reverse();
        writer.WriteBytes(id);

        Span<byte> span = stackalloc byte[8];
        activity.SpanId.CopyTo(span);
        span.Reverse();
        writer.WriteBytes(span);

        writer.WriteString(activity.TraceStateString ?? string.Empty);
        writer.WriteByte((byte)activity.ActivityTraceFlags);
    }

    private static string TryGet(Func<string> get)
    {
        try
        {
            return get() ?? string.Empty;
        }
        catch (Exception)
        {
            return string.Empty;
        }
    }
}
