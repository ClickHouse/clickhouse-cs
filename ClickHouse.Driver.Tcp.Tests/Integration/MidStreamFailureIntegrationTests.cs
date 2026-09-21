using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Verifies server errors after data blocks. Exception packets are unframed even when surrounding blocks are
/// compressed, so each compression mode must preserve packet alignment and surface the original error.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MidStreamFailureIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO is not named by ClickHouseErrorCode; preserve its raw code.
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
        // A pool of one makes connection retirement observable.
        await using var client = new ClickHouseTcpClient(
            TcpServerFixture.Options() with { Compressor = codec, MaxPoolSize = 1 });

        // A temporary table distinguishes reuse from replacement.
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

        // The server may flush any number of rows before the error.
        Assert.Multiple(() =>
        {
            Assert.That(blocks, Is.GreaterThan(1), "the error must arrive mid-stream, not at submit time");
            Assert.That(rows, Is.GreaterThan(1000));
            Assert.That(failure.RawCode, Is.EqualTo(ThrowIfValueIsNonZero));
            Assert.That(failure.Message, Does.Contain("throwIf"), "the error text survives an unframed body");
        });

        // Server exceptions retire the connection; only end-of-stream permits reuse.
        Assert.That(
            async () => await client.ExecuteScalarAsync($"SELECT count() FROM {marker}", cancellationToken: None),
            Throws.TypeOf<ClickHouseTcpServerException>(),
            "a connection that failed mid-stream must not go back to the pool");

        // The replacement query also verifies that the permit was returned.
        Assert.That(
            await client.ExecuteScalarAsync("SELECT toUInt64(7)", cancellationToken: None),
            Is.EqualTo(7UL));
    }
}
