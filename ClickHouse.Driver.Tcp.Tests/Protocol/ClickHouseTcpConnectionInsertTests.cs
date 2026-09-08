using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

// Drives InsertAsync over a scripted server stream (the client's own writes go to an inspectable sink, so the
// scripted side only supplies the server's schema/terminal packets). A real INSERT round-trip lives in the
// integration suite; the row-splitting maths is unit-tested against PlanInsertBlocks directly.
[TestFixture]
public class ClickHouseTcpConnectionInsertTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly ClientHandshakeParameters Handshake = new()
    {
        Username = "default",
    };

    [Test]
    public async Task InsertAsync_SchemaThenEndOfStream_CompletesAndStaysReady()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await SchemaBlockAsync(("x", "UInt64")),
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1, 2, 3)), cancellationToken: None);

        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task InsertAsync_MaxRowsPerBlock_StreamsMultipleBlocksAndStaysReady()
    {
        // The scripted server accepts whatever the client streams; forcing a small row cap exercises the
        // multi-block write path end-to-end without desyncing.
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await SchemaBlockAsync(("x", "UInt64")),
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1, 2, 3, 4, 5)), maxRowsPerBlock: 2, cancellationToken: None);

        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task InsertAsync_ProgressInAcknowledgement_InvokesHandlerAndStaysReady()
    {
        // The server interleaves a Progress packet into the acknowledgement before end-of-stream.
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await SchemaBlockAsync(("x", "UInt64")),
            await ProgressPacketAsync(),
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        var progresses = new List<Progress>();
        await connection.InsertAsync(
            "INSERT INTO t VALUES",
            Columns(UInt64Column(1, 2, 3)),
            handlers: new MetadataHandlers { OnProgress = progresses.Add },
            cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(progresses, Has.Count.EqualTo(1));
            Assert.That(progresses[0].Rows, Is.EqualTo(1UL));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task InsertAsync_StringColumnMaxRowsPerBlock_SlicesAndStreamsAndStaysReady()
    {
        // A variable-width column forced across multiple blocks exercises the array-slice + string write path.
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await SchemaBlockAsync(("s", "String")),
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        IColumn[] columns = { new ArrayColumn<string>("s", "String", new[] { "a", "bb", "ccc", "dddd" }) };
        await connection.InsertAsync("INSERT INTO t VALUES", columns, maxRowsPerBlock: 2, cancellationToken: None);

        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task InsertAsync_ZeroRows_CompletesAsNoOpAndStaysReady()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await SchemaBlockAsync(("x", "UInt64")),
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column()), cancellationToken: None);

        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task InsertAsync_CleanEndOfStreamBeforeSchema_ThrowsProtocolAndTerminates()
    {
        byte[] script = Concat(await ServerHelloBytesAsync(54476), EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1)), cancellationToken: None));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task InsertAsync_ServerExceptionInsteadOfSchema_ThrowsButStaysReusable()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await ExceptionPacketAsync(60, "unknown table"));
        using var connection = await ConnectedAsync(script);

        var thrown = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1)), cancellationToken: None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Code, Is.EqualTo(60));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task InsertAsync_ServerExceptionDuringAcknowledgement_ThrowsButStaysReusable()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await SchemaBlockAsync(("x", "UInt64")),
            await ExceptionPacketAsync(241, "memory limit exceeded"));
        using var connection = await ConnectedAsync(script);

        var thrown = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1, 2)), cancellationToken: None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Code, Is.EqualTo(241));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task InsertAsync_TransportTruncatedDuringSchema_Terminates()
    {
        // A Data packet type with no block body: reading the schema hits end of stream mid-way.
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await BytesAsync(w => w.WriteVarUInt((ulong)ServerPacketType.Data)));
        using var connection = await ConnectedAsync(script);

        Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1)), cancellationToken: None));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Terminated));
    }

    [Test]
    public async Task InsertAsync_TokenAlreadyCancelled_ThrowsWithoutClaimingConnection()
    {
        var transport = new ScriptedDuplexStream(await ServerHelloBytesAsync(54476));
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);
        byte[] writtenBeforeInsert = transport.Written;

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1)), cancellationToken: new CancellationToken(canceled: true)));

        // A pre-cancelled insert must not claim the connection or send anything, leaving it Ready and reusable.
        Assert.Multiple(() =>
        {
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(transport.Written, Is.EqualTo(writtenBeforeInsert));
        });
    }

    [Test]
    public async Task InsertAsync_ColumnCountDisagreesWithSchema_ThrowsArgumentButStaysReady()
    {
        byte[] script = Concat(
            await ServerHelloBytesAsync(54476),
            await SchemaBlockAsync(("a", "UInt64"), ("b", "UInt64")), // schema wants two columns
            EndOfStreamPacket());
        using var connection = await ConnectedAsync(script);

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1)), cancellationToken: None));
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
    }

    [Test]
    public async Task InsertAsync_ColumnsWithDifferingRowCounts_ThrowsWithoutClaimingConnection()
    {
        var transport = new ScriptedDuplexStream(await ServerHelloBytesAsync(54476));
        using var connection = new ClickHouseTcpConnection(transport, socket: null);
        await connection.HandshakeAsync(Handshake, None);
        byte[] writtenBeforeInsert = transport.Written;

        IColumn[] columns = { UInt64Column(1, 2), UInt64Column(3) };
        Assert.ThrowsAsync<ArgumentException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", columns, cancellationToken: None));

        // Validation happens before the connection is claimed, so nothing is sent and it stays Ready.
        Assert.Multiple(() =>
        {
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
            Assert.That(transport.Written, Is.EqualTo(writtenBeforeInsert));
        });
    }

    [Test]
    public void InsertAsync_NonPositiveMaxRowsPerBlock_ThrowsArgumentOutOfRange()
    {
        using var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(Array.Empty<byte>()), socket: null);

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await connection.InsertAsync("INSERT INTO t VALUES", Columns(UInt64Column(1)), maxRowsPerBlock: 0, cancellationToken: None));
    }

    [Test]
    public void PlanInsertBlocks_NoRowCap_ProducesOneBlock()
    {
        // With no row cap the whole insert is one block; peak memory is bounded by the flush backstop, not a split.
        var plan = ClickHouseTcpConnection.PlanInsertBlocks(rowCount: 5, maxRowsPerBlock: null);

        CollectionAssert.AreEqual(new[] { (0, 5) }, plan);
    }

    [Test]
    public void PlanInsertBlocks_RowCapSmallerThanRowCount_SplitsByRowCap()
    {
        var plan = ClickHouseTcpConnection.PlanInsertBlocks(rowCount: 5, maxRowsPerBlock: 2);

        CollectionAssert.AreEqual(new[] { (0, 2), (2, 2), (4, 1) }, plan);
    }

    [Test]
    public void PlanInsertBlocks_RowCapExactMultiple_SplitsEvenly()
    {
        var plan = ClickHouseTcpConnection.PlanInsertBlocks(rowCount: 4, maxRowsPerBlock: 2);

        CollectionAssert.AreEqual(new[] { (0, 2), (2, 2) }, plan);
    }

    [Test]
    public void PlanInsertBlocks_RowCapAtLeastRowCount_ProducesOneBlock()
    {
        var plan = ClickHouseTcpConnection.PlanInsertBlocks(rowCount: 3, maxRowsPerBlock: 10);

        CollectionAssert.AreEqual(new[] { (0, 3) }, plan);
    }

    [Test]
    public void PlanInsertBlocks_RowCapOfOne_EmitsOneRowPerBlock()
    {
        var plan = ClickHouseTcpConnection.PlanInsertBlocks(rowCount: 3, maxRowsPerBlock: 1);

        CollectionAssert.AreEqual(new[] { (0, 1), (1, 1), (2, 1) }, plan);
    }

    private static async Task<ClickHouseTcpConnection> ConnectedAsync(byte[] script)
    {
        var connection = new ClickHouseTcpConnection(new ScriptedDuplexStream(script), socket: null);
        await connection.HandshakeAsync(Handshake, None);
        return connection;
    }

    private static IColumn[] Columns(params IColumn[] columns) => columns;

    private static IColumn UInt64Column(params ulong[] values) => PrimitiveColumn<ulong>.FromValues("x", "UInt64", values);

    // A zero-row Data block carrying only column headers — the shape the server sends as the INSERT schema.
    private static Task<byte[]> SchemaBlockAsync(params (string Name, string Type)[] columns)
        => BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Data);
            w.WriteString(string.Empty);
            BlockWriter.WriteBlockInfo(w, BlockInfo.Default);
            w.WriteVarUInt((ulong)columns.Length); // num_columns
            w.WriteVarUInt(0);                      // num_rows
            foreach ((string name, string type) in columns)
            {
                w.WriteString(name);
                w.WriteString(type);
                w.WriteBool(false); // has_custom_serialization (supported at 54476)
            }
        });

    private static Task<byte[]> ExceptionPacketAsync(int code, string message)
        => BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Exception);
            w.WriteInt32(code);
            w.WriteString("DB::Exception");
            w.WriteString(message);
            w.WriteString("stack trace");
            w.WriteBool(false); // has_nested
        });

    private static byte[] EndOfStreamPacket()
        => BytesAsync(w => w.WriteVarUInt((ulong)ServerPacketType.EndOfStream)).GetAwaiter().GetResult();

    private static Task<byte[]> ProgressPacketAsync()
        => BytesAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Progress);
            w.WriteVarUInt(1); // rows
            w.WriteVarUInt(2); // bytes
            w.WriteVarUInt(3); // total_rows
            w.WriteVarUInt(0); // wrote_rows
            w.WriteVarUInt(0); // wrote_bytes
            w.WriteVarUInt(0); // elapsed_ns
        });

    private static Task<byte[]> ServerHelloBytesAsync(int revision) => BytesAsync(w =>
    {
        w.WriteVarUInt((ulong)ServerPacketType.Hello);
        w.WriteString("ClickHouse");
        w.WriteVarUInt(25);
        w.WriteVarUInt(8);
        w.WriteVarUInt((ulong)revision);
        w.WriteString("UTC");
        w.WriteString("clickhouse-server");
        w.WriteVarUInt(0);
    });

    private static async Task<byte[]> BytesAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static byte[] Concat(params byte[][] arrays)
    {
        int length = 0;
        foreach (byte[] array in arrays)
        {
            length += array.Length;
        }

        byte[] result = new byte[length];
        int offset = 0;
        foreach (byte[] array in arrays)
        {
            Buffer.BlockCopy(array, 0, result, offset, array.Length);
            offset += array.Length;
        }

        return result;
    }
}
