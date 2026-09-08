using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class ServerPacketDecoderTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task Progress_AtCurrentTarget_ReadsAllGatedCounters()
    {
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteVarUInt(10);   // rows
            w.WriteVarUInt(20);   // bytes
            w.WriteVarUInt(30);   // total_rows
            w.WriteVarUInt(40);   // wrote_rows (>= 54420)
            w.WriteVarUInt(50);   // wrote_bytes (>= 54420)
            w.WriteVarUInt(60);   // elapsed_ns (>= 54460)
        });
        using var reader = ReaderOver(bytes);

        Progress progress = await Progress.ReadAsync(reader, new NegotiatedProtocol(NegotiatedProtocol.ClientTcpProtocolVersion), None);

        Assert.Multiple(() =>
        {
            Assert.That(progress.Rows, Is.EqualTo(10UL));
            Assert.That(progress.Bytes, Is.EqualTo(20UL));
            Assert.That(progress.TotalRows, Is.EqualTo(30UL));
            Assert.That(progress.WroteRows, Is.EqualTo(40UL));
            Assert.That(progress.WroteBytes, Is.EqualTo(50UL));
            Assert.That(progress.ElapsedNs, Is.EqualTo(60UL));
        });
    }

    [Test]
    public async Task Progress_BelowWriteInfoGate_ReadsOnlyBaseCounters()
    {
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteVarUInt(10);   // rows
            w.WriteVarUInt(20);   // bytes
            w.WriteVarUInt(30);   // total_rows
        });
        using var reader = ReaderOver(bytes);

        // A server negotiating below 54420 sends no wrote_rows/wrote_bytes/elapsed_ns.
        Progress progress = await Progress.ReadAsync(reader, new NegotiatedProtocol(54419), None);

        Assert.Multiple(() =>
        {
            Assert.That(progress.TotalRows, Is.EqualTo(30UL));
            Assert.That(progress.WroteRows, Is.EqualTo(0UL));
            Assert.That(progress.ElapsedNs, Is.EqualTo(0UL));
        });
    }

    [Test]
    public async Task ProfileInfo_RoundTrips()
    {
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteVarUInt(5);    // rows
            w.WriteVarUInt(1);    // blocks
            w.WriteVarUInt(400);  // bytes
            w.WriteBool(true);    // applied_limit
            w.WriteVarUInt(9);    // rows_before_limit
            w.WriteBool(true);    // calculated_rows_before_limit
        });
        using var reader = ReaderOver(bytes);

        ProfileInfo info = await ProfileInfo.ReadAsync(reader, None);

        Assert.Multiple(() =>
        {
            Assert.That(info.Rows, Is.EqualTo(5UL));
            Assert.That(info.Blocks, Is.EqualTo(1UL));
            Assert.That(info.Bytes, Is.EqualTo(400UL));
            Assert.That(info.AppliedLimit, Is.True);
            Assert.That(info.RowsBeforeLimit, Is.EqualTo(9UL));
            Assert.That(info.CalculatedRowsBeforeLimit, Is.True);
        });
    }

    [Test]
    public async Task TableColumns_RoundTrips()
    {
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteString("ext");
            w.WriteString("a UInt64, b String");
        });
        using var reader = ReaderOver(bytes);

        TableColumns columns = await TableColumns.ReadAsync(reader, None);

        Assert.Multiple(() =>
        {
            Assert.That(columns.ExternalTableName, Is.EqualTo("ext"));
            Assert.That(columns.ColumnsDescription, Is.EqualTo("a UInt64, b String"));
        });
    }

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
