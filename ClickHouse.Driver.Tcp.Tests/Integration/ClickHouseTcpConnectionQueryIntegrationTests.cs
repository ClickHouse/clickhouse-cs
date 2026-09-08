using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

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
    public async Task QueryAsync_SelectStringLiteral_ReturnsString()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        string value = null;
        await foreach (Block block in connection.QueryAsync("SELECT 'hello'", cancellationToken: None))
        {
            value = ((IColumn<string>)block[0]).Values[0];
        }

        Assert.That(value, Is.EqualTo("hello"));
    }

    [Test]
    public async Task QueryAsync_NumbersWithToString_ReturnsUInt64AndStringColumns()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        var numbers = new List<ulong>();
        var strings = new List<string>();
        await foreach (Block block in connection.QueryAsync("SELECT number, toString(number) FROM system.numbers LIMIT 5", cancellationToken: None))
        {
            numbers.AddRange(((IColumn<ulong>)block[0]).Values.ToArray());
            strings.AddRange(((IColumn<string>)block[1]).Values.ToArray());
        }

        Assert.Multiple(() =>
        {
            CollectionAssert.AreEqual(new ulong[] { 0, 1, 2, 3, 4 }, numbers);
            CollectionAssert.AreEqual(new[] { "0", "1", "2", "3", "4" }, strings);
        });
    }

    [Test]
    public async Task QueryAsync_AllIntegerWidths_RoundTripEachValue()
    {
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        int blockCount = 0;
        byte u8 = 0;
        sbyte i8 = 0;
        ushort u16 = 0;
        short i16 = 0;
        uint u32 = 0;
        int i32 = 0;
        ulong u64 = 0;
        long i64 = 0;
        UInt128 u128 = 0;
        Int128 i128 = 0;
        BigInteger u256 = 0;
        BigInteger i256 = 0;
        await foreach (Block row in connection.QueryAsync(
            "SELECT toUInt8(1), toInt8(-1), toUInt16(2), toInt16(-2), toUInt32(3), toInt32(-3), " +
            "toUInt64(4), toInt64(-4), toUInt128(5), toInt128(-5), toUInt256(6), toInt256(-6)",
            cancellationToken: None))
        {
            blockCount++;
            u8 = ((IColumn<byte>)row[0]).Values[0];
            i8 = ((IColumn<sbyte>)row[1]).Values[0];
            u16 = ((IColumn<ushort>)row[2]).Values[0];
            i16 = ((IColumn<short>)row[3]).Values[0];
            u32 = ((IColumn<uint>)row[4]).Values[0];
            i32 = ((IColumn<int>)row[5]).Values[0];
            u64 = ((IColumn<ulong>)row[6]).Values[0];
            i64 = ((IColumn<long>)row[7]).Values[0];
            u128 = ((IColumn<UInt128>)row[8]).Values[0];
            i128 = ((IColumn<Int128>)row[9]).Values[0];
            u256 = ((IColumn<UInt256>)row[10]).Values[0].ToBigInteger();
            i256 = ((IColumn<Int256>)row[11]).Values[0].ToBigInteger();
        }

        Assert.Multiple(() =>
        {
            Assert.That(blockCount, Is.EqualTo(1));
            Assert.That(u8, Is.EqualTo((byte)1));
            Assert.That(i8, Is.EqualTo((sbyte)-1));
            Assert.That(u16, Is.EqualTo((ushort)2));
            Assert.That(i16, Is.EqualTo((short)-2));
            Assert.That(u32, Is.EqualTo(3u));
            Assert.That(i32, Is.EqualTo(-3));
            Assert.That(u64, Is.EqualTo(4ul));
            Assert.That(i64, Is.EqualTo(-4L));
            Assert.That(u128, Is.EqualTo((UInt128)5));
            Assert.That(i128, Is.EqualTo((Int128)(-5)));
            Assert.That(u256, Is.EqualTo(new BigInteger(6)));
            Assert.That(i256, Is.EqualTo(new BigInteger(-6)));
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
