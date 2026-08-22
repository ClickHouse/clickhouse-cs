using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Read-path cases that assert the connection is left Ready, which no client-level test can express because the
// client does not expose connection state. The cases that do not need that assertion live in
// ClickHouseTcpClientQueryIntegrationTests, driven through the client.
//
// A yielded Block is borrowed — valid only for its iteration — so every test reads or copies what it needs
// inside the await foreach, never retaining the block.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpConnectionQueryIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task QueryAsync_SelectLiteralInteger_ReturnsSingleUInt8()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int blockCount = 0;
        string typeName = null;
        byte value = 0;
        await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
        {
            blockCount++;
            typeName = block[0].TypeName;
            value = ((IColumn<byte>)block[0]).Values[0];
        }

        Assert.Multiple(() =>
        {
            Assert.That(blockCount, Is.EqualTo(1));
            Assert.That(typeName, Is.EqualTo("UInt8"));
            Assert.That(value, Is.EqualTo((byte)1));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_LargeResultWithSmallBlocks_StreamsAllRowsAcrossBlocks()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        var settings = new Dictionary<string, string> { ["max_block_size"] = "1000" };

        int blockCount = 0;
        long rows = 0;
        BigInteger sum = BigInteger.Zero;
        await foreach (Block block in connection.QueryAsync("SELECT number FROM system.numbers LIMIT 100000", settings, cancellationToken: None))
        {
            blockCount++;
            foreach (ulong value in ((IColumn<ulong>)block[0]).Values)
            {
                rows++;
                sum += value;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(blockCount, Is.GreaterThan(1), "small max_block_size should split the result into multiple blocks");
            Assert.That(rows, Is.EqualTo(100000));
            Assert.That(sum, Is.EqualTo(new BigInteger(4999950000))); // 0 + 1 + ... + 99999
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_EmptyResult_YieldsNoRowBearingBlocksAndStaysReady()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int blockCount = 0;
        await foreach (Block block in connection.QueryAsync("SELECT 1 WHERE 0", cancellationToken: None))
        {
            _ = block;
            blockCount++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(blockCount, Is.EqualTo(0));
            Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));
        });
    }

    [Test]
    public async Task QueryAsync_ServerError_ThrowsThenConnectionIsReusable()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        var thrown = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (Block block in connection.QueryAsync("SELECT * FROM table_that_does_not_exist_xyz", cancellationToken: None))
            {
                _ = block;
            }
        });
        Assert.That(thrown.Code, Is.GreaterThan(0));

        // The Exception is a complete response, so the same connection can run another query.
        Assert.That(connection.State, Is.EqualTo(TcpConnectionState.Ready));

        byte value = 0;
        await foreach (Block block in connection.QueryAsync("SELECT 1", cancellationToken: None))
        {
            value = ((IColumn<byte>)block[0]).Values[0];
        }

        Assert.That(value, Is.EqualTo((byte)1));
    }
}
