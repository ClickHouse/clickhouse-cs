using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Native;

/// <summary>
/// A single ClickHouse Native (TCP) protocol connection. Performs the handshake on connect and
/// can execute one query at a time, returning the fully-buffered result.
/// </summary>
/// <remarks>
/// This is an MVP: the protocol I/O over the socket is synchronous and blocking (only the initial
/// TCP connect is awaited), one connection is used per query (no pooling), compression and chunked
/// framing are disabled, and columns using custom serialization (LowCardinality, sparse, etc.) are
/// not yet supported. The negotiated protocol revision is min(client 54460, server).
/// </remarks>
internal sealed class NativeConnection : IDisposable
{
    private readonly TcpClient tcp;
    private readonly ExtendedBinaryReader reader;
    private readonly ExtendedBinaryWriter writer;
    private bool disposed;

    private NativeConnection(TcpClient tcp)
    {
        this.tcp = tcp;
        var stream = tcp.GetStream();
        reader = new ExtendedBinaryReader(stream);
        writer = new ExtendedBinaryWriter(stream);
    }

    public string ServerName { get; private set; }

    public ulong ServerVersionMajor { get; private set; }

    public ulong ServerVersionMinor { get; private set; }

    public ulong ServerRevision { get; private set; }

    public ulong ServerVersionPatch { get; private set; }

    public string ServerTimezone { get; private set; }

    public string ServerDisplayName { get; private set; }

    /// <summary>Effective protocol revision: min(client, server).</summary>
    public int NegotiatedRevision { get; private set; }

    public static async Task<NativeConnection> ConnectAsync(string host, int port, string database, string user, string password, CancellationToken cancellationToken)
    {
        var tcp = new TcpClient { NoDelay = true };
        try
        {
            await tcp.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
            var connection = new NativeConnection(tcp);
            connection.Handshake(database, user, password);
            return connection;
        }
        catch
        {
            tcp.Dispose();
            throw;
        }
    }

    private void Handshake(string database, string user, string password)
    {
        // --- ClientHello ---
        NativeWire.WriteVarUInt(writer, (ulong)ClientPacketType.Hello);
        NativeWire.WriteString(writer, NativeConstants.ClientName);
        NativeWire.WriteVarUInt(writer, NativeConstants.ClientVersionMajor);
        NativeWire.WriteVarUInt(writer, NativeConstants.ClientVersionMinor);
        NativeWire.WriteVarUInt(writer, NativeConstants.ClientProtocolVersion);
        NativeWire.WriteString(writer, database ?? string.Empty);
        NativeWire.WriteString(writer, user ?? string.Empty);
        NativeWire.WriteString(writer, password ?? string.Empty);
        writer.Flush();

        // --- ServerHello / Exception ---
        var packet = (ServerPacketType)NativeWire.ReadVarUInt(reader);
        if (packet == ServerPacketType.Exception)
            throw ReadException();
        if (packet != ServerPacketType.Hello)
            throw new InvalidOperationException($"Expected ServerHello during handshake, got packet type {(int)packet}");

        ServerName = NativeWire.ReadString(reader);
        ServerVersionMajor = NativeWire.ReadVarUInt(reader);
        ServerVersionMinor = NativeWire.ReadVarUInt(reader);
        ServerRevision = NativeWire.ReadVarUInt(reader);
        NegotiatedRevision = (int)Math.Min(ServerRevision, NativeConstants.ClientProtocolVersion);

        if (NegotiatedRevision >= NativeConstants.MinRevisionWithServerTimezone)
            ServerTimezone = NativeWire.ReadString(reader);
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithServerDisplayName)
            ServerDisplayName = NativeWire.ReadString(reader);
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithVersionPatch)
            ServerVersionPatch = NativeWire.ReadVarUInt(reader);

        // --- Addendum (quota_key only at this revision; chunked/parallel-replicas fields are above 54460) ---
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithAddendum)
        {
            NativeWire.WriteString(writer, string.Empty); // quota_key
            writer.Flush();
        }
    }

    /// <summary>
    /// Sends a query and reads the full response into memory. <paramref name="settings"/> are sent
    /// as per-query settings. Throws <see cref="ClickHouseServerException"/> on a server Exception.
    /// </summary>
    public NativeQueryResult ExecuteQuery(string sql, string queryId, string user, IReadOnlyDictionary<string, object> settings, TypeSettings typeSettings)
    {
        SendQuery(sql, queryId, user, settings);
        return ReadResponse(typeSettings);
    }

    private void SendQuery(string sql, string queryId, string user, IReadOnlyDictionary<string, object> settings)
    {
        NativeWire.WriteVarUInt(writer, (ulong)ClientPacketType.Query);
        NativeWire.WriteString(writer, queryId ?? string.Empty);

        WriteClientInfo(queryId, user);

        // Settings (key, flags VarUInt, value); empty-key terminator.
        if (settings != null)
        {
            foreach (var kv in settings)
            {
                NativeWire.WriteString(writer, kv.Key);
                NativeWire.WriteVarUInt(writer, 0); // flags: not important, not custom
                NativeWire.WriteString(writer, Convert.ToString(kv.Value, CultureInfo.InvariantCulture) ?? string.Empty);
            }
        }

        NativeWire.WriteString(writer, string.Empty); // end of settings

        // External roles list is gated above the negotiated revision, so it is omitted here.
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithInterserverSecret)
            NativeWire.WriteString(writer, string.Empty); // cluster_secret

        NativeWire.WriteVarUInt(writer, NativeConstants.StageComplete);
        NativeWire.WriteVarUInt(writer, 0); // compression disabled
        NativeWire.WriteString(writer, sql);

        if (NegotiatedRevision >= NativeConstants.MinRevisionWithParameters)
            NativeWire.WriteString(writer, string.Empty); // end of (empty) parameters list

        // Empty Data block: tells the server there are no external tables and to start executing.
        NativeWire.WriteVarUInt(writer, (ulong)ClientPacketType.Data);
        NativeWire.WriteString(writer, string.Empty); // table name
        WriteEmptyBlock();

        writer.Flush();
    }

    private void WriteClientInfo(string queryId, string user)
    {
        writer.Write((byte)1); // query_kind = InitialQuery
        NativeWire.WriteString(writer, user ?? string.Empty); // initial_user
        NativeWire.WriteString(writer, queryId ?? string.Empty); // initial_query_id
        NativeWire.WriteString(writer, "127.0.0.1:0"); // initial_address (must be non-empty host:port)

        if (NegotiatedRevision >= NativeConstants.MinRevisionWithInitialQueryStartTime)
            writer.Write((long)0); // initial_time (Int64 LE, microseconds)

        writer.Write((byte)1); // query_interface = TCP

        NativeWire.WriteString(writer, string.Empty); // os_user
        NativeWire.WriteString(writer, string.Empty); // client_hostname
        NativeWire.WriteString(writer, NativeConstants.ClientName); // client_name
        NativeWire.WriteVarUInt(writer, NativeConstants.ClientVersionMajor);
        NativeWire.WriteVarUInt(writer, NativeConstants.ClientVersionMinor);
        NativeWire.WriteVarUInt(writer, NativeConstants.ClientProtocolVersion);

        if (NegotiatedRevision >= NativeConstants.MinRevisionWithQuotaKeyInClientInfo)
            NativeWire.WriteString(writer, string.Empty); // quota_key
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithDistributedDepth)
            NativeWire.WriteVarUInt(writer, 0); // distributed_depth
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithVersionPatch)
            NativeWire.WriteVarUInt(writer, 0); // version_patch
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithOpenTelemetry)
            writer.Write((byte)0); // open telemetry: no trace
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithParallelReplicas)
        {
            NativeWire.WriteVarUInt(writer, 0); // collaborate_with_initiator
            NativeWire.WriteVarUInt(writer, 0); // count_participating_replicas
            NativeWire.WriteVarUInt(writer, 0); // number_of_current_replica
        }
    }

    private void WriteEmptyBlock()
    {
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithBlockInfo)
        {
            NativeWire.WriteVarUInt(writer, 1); // field: is_overflows
            writer.Write((byte)0);
            NativeWire.WriteVarUInt(writer, 2); // field: bucket_number
            writer.Write(-1); // Int32 LE
            NativeWire.WriteVarUInt(writer, 0); // end of block info
        }

        NativeWire.WriteVarUInt(writer, 0); // num_columns
        NativeWire.WriteVarUInt(writer, 0); // num_rows
    }

    private NativeQueryResult ReadResponse(TypeSettings typeSettings)
    {
        string[] names = null;
        ClickHouseType[] types = null;
        var rows = new List<object[]>();

        while (true)
        {
            var packet = (ServerPacketType)NativeWire.ReadVarUInt(reader);
            switch (packet)
            {
                case ServerPacketType.Data:
                    AppendBlock(ReadDataBlock(typeSettings), ref names, ref types, rows);
                    break;

                case ServerPacketType.EndOfStream:
                    return new NativeQueryResult(names ?? Array.Empty<string>(), types ?? Array.Empty<ClickHouseType>(), rows);

                case ServerPacketType.Exception:
                    throw ReadException();

                case ServerPacketType.Progress:
                    SkipProgress();
                    break;

                case ServerPacketType.ProfileInfo:
                    SkipProfileInfo();
                    break;

                // These all carry a table name + Block; decode and discard to stay in sync.
                case ServerPacketType.Totals:
                case ServerPacketType.Extremes:
                case ServerPacketType.Log:
                case ServerPacketType.ProfileEvents:
                    ReadDataBlock(typeSettings);
                    break;

                case ServerPacketType.TableColumns:
                    NativeWire.ReadString(reader); // external table name
                    NativeWire.ReadString(reader); // columns description
                    break;

                case ServerPacketType.TimezoneUpdate:
                    NativeWire.ReadString(reader);
                    break;

                default:
                    throw new NotSupportedException($"Unsupported server packet type {(int)packet} in Native protocol MVP");
            }
        }
    }

    private NativeBlock ReadDataBlock(TypeSettings typeSettings)
    {
        NativeWire.ReadString(reader); // table name (usually empty)
        return NativeBlockReader.ReadBlock(reader, NegotiatedRevision, typeSettings);
    }

    private static void AppendBlock(NativeBlock block, ref string[] names, ref ClickHouseType[] types, List<object[]> rows)
    {
        if (block.IsEmptyMarker)
            return;

        names ??= block.Names;
        types ??= block.Types;

        for (var r = 0; r < block.RowCount; r++)
        {
            var row = new object[block.Columns.Length];
            for (var c = 0; c < block.Columns.Length; c++)
                row[c] = block.Columns[c][r];
            rows.Add(row);
        }
    }

    private void SkipProgress()
    {
        NativeWire.ReadVarUInt(reader); // rows
        NativeWire.ReadVarUInt(reader); // bytes
        NativeWire.ReadVarUInt(reader); // total_rows
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithTotalBytesInProgress)
            NativeWire.ReadVarUInt(reader); // total_bytes
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithClientInfo)
        {
            NativeWire.ReadVarUInt(reader); // wrote_rows
            NativeWire.ReadVarUInt(reader); // wrote_bytes
        }

        if (NegotiatedRevision >= NativeConstants.MinRevisionWithServerQueryTimeInProgress)
            NativeWire.ReadVarUInt(reader); // elapsed_ns
    }

    private void SkipProfileInfo()
    {
        NativeWire.ReadVarUInt(reader); // rows
        NativeWire.ReadVarUInt(reader); // blocks
        NativeWire.ReadVarUInt(reader); // bytes
        reader.ReadByte(); // applied_limit
        NativeWire.ReadVarUInt(reader); // rows_before_limit
        reader.ReadByte(); // calculated_rows_before_limit
        if (NegotiatedRevision >= NativeConstants.MinRevisionWithRowsBeforeAggregation)
        {
            reader.ReadByte(); // applied_aggregation
            NativeWire.ReadVarUInt(reader); // rows_before_aggregation
        }
    }

    private ClickHouseServerException ReadException()
    {
        // Exception (possibly a nested chain). Use the first (outermost) frame's code + message.
        var firstCode = 0;
        string firstText = null;
        while (true)
        {
            var code = reader.ReadInt32();
            var name = NativeWire.ReadString(reader);
            var message = NativeWire.ReadString(reader);
            NativeWire.ReadString(reader); // stack_trace
            var hasNested = reader.ReadByte() != 0;

            if (firstText is null)
            {
                firstCode = code;
                firstText = $"Code: {code}. {name}: {message}";
            }

            if (!hasNested)
                break;
        }

        return new ClickHouseServerException(firstText, null, firstCode);
    }

    public void Dispose()
    {
        if (disposed)
            return;
        disposed = true;
        tcp.Dispose();
    }
}
