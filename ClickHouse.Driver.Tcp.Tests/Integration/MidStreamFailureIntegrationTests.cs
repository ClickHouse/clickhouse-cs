using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// A server error raised part-way through a result, after rows have already been handed to the caller. The other
/// integration error cases all fail at submit time, before any data block, and the mid-stream case was covered
/// only by scripted bytes — which proves the reader agrees with our own idea of the packet order, not the
/// server's.
///
/// <para>
/// Compression is what makes this worth running per codec. The <c>Exception</c> packet body is <b>not</b> framed
/// while the data blocks around it are, so it is read straight from the raw reader; routing it through the frame
/// reader instead would read the error text as a frame header and lose the error. That also means the framed
/// bodies before it must have been consumed to the byte, or the raw reader starts the error text in the wrong
/// place. The connection is retired either way, so the damage would be a wrong or missing exception rather than
/// a corrupted next query.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class MidStreamFailureIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // 395 is FUNCTION_THROW_IF_VALUE_IS_NON_ZERO. ClickHouseErrorCode names a subset of the server's codes, so
    // this one arrives as Unknown with the raw number intact; the raw number is the part that is a contract.
    private const int ThrowIfValueIsNonZero = 395;

    private static IEnumerable<TestCaseData> Codecs()
    {
        yield return new TestCaseData(null).SetName("{m}(uncompressed)");
        yield return new TestCaseData(Lz4Compressor.Default).SetName("{m}(LZ4)");
        yield return new TestCaseData(ZstdCompressor.Default).SetName("{m}(ZSTD)");
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task StreamAsync_ServerThrowsPartWayThroughTheResult_ReportsTheErrorAfterTheRowsThatArrived(
        IClickHouseCompressor codec)
    {
        // One connection, so the queries after the failure can only run on what the failed one left behind.
        await using var client = new ClickHouseTcpClient(
            TcpServerFixture.Options() with { Compressor = codec, MaxPoolSize = 1 });

        // A temporary table is scoped to the connection, so it answers "retired or pooled?" without reaching
        // into the pool: a pooled connection still has it, a retired one is replaced by a dial to a server that
        // never saw it.
        string marker = $"tcp_mid_stream_{Guid.NewGuid():N}";
        await client.ExecuteAsync($"CREATE TEMPORARY TABLE {marker} (value UInt64)", cancellationToken: None);
        Assert.That(
            await client.ExecuteScalarAsync($"SELECT count() FROM {marker}", cancellationToken: None),
            Is.EqualTo(0UL),
            "the marker must survive an ordinary query, or it cannot testify about the failing one");

        int blocks = 0;
        long rows = 0;
        var failure = Assert.ThrowsAsync<ClickHouseTcpServerException>(async () =>
        {
            await foreach (Block block in client.StreamAsync(
                "SELECT number, throwIf(number = 50000) FROM system.numbers SETTINGS max_block_size = 100",
                cancellationToken: None))
            {
                blocks++;
                rows += block.RowCount;
            }
        });

        // How many rows arrive is not fixed — the server drops whatever output it had not flushed when it threw —
        // but the packet order is, so rows cannot arrive after the error.
        Assert.Multiple(() =>
        {
            Assert.That(blocks, Is.GreaterThan(1), "the error must arrive mid-stream, not at submit time");
            Assert.That(rows, Is.GreaterThan(1000));
            Assert.That(failure.RawCode, Is.EqualTo(ThrowIfValueIsNonZero));
            Assert.That(failure.Message, Does.Contain("throwIf"), "the error text survives an unframed body");
        });

        // Retired, not pooled. This is the uniform policy for a server exception rather than anything specific to
        // failing mid-stream: only end-of-stream marks a connection reusable.
        Assert.That(
            async () => await client.ExecuteScalarAsync($"SELECT count() FROM {marker}", cancellationToken: None),
            Throws.TypeOf<ClickHouseTcpServerException>(),
            "a connection that failed mid-stream must not go back to the pool");

        // And the pool got its one permit back, so the replacement is dialled rather than waited for.
        Assert.That(
            await client.ExecuteScalarAsync("SELECT toUInt64(7)", cancellationToken: None),
            Is.EqualTo(7UL));
    }
}
