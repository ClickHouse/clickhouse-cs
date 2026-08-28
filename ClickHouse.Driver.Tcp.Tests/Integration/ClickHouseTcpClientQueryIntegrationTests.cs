using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Read-path behavior through the client's block tier. The cases that also assert the connection stays Ready live in
// ClickHouseTcpConnectionQueryIntegrationTests, because the client does not expose connection state.
//
// A yielded Block is borrowed — valid only for its iteration — so every test reads or copies what it needs inside
// the await foreach, never retaining the block.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpClientQueryIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task StreamAsync_SelectStringLiteral_ReturnsString()
    {
        await using var client = TcpServerFixture.CreateClient();

        string value = null;
        await foreach (Block block in client.StreamAsync("SELECT 'hello'", cancellationToken: None))
        {
            value = ((IColumn<string>)block[0]).Values[0];
        }

        Assert.That(value, Is.EqualTo("hello"));
    }

    [Test]
    public async Task StreamAsync_NumbersWithToString_ReturnsUInt64AndStringColumns()
    {
        await using var client = TcpServerFixture.CreateClient();

        var numbers = new List<ulong>();
        var strings = new List<string>();
        await foreach (Block block in client.StreamAsync("SELECT number, toString(number) FROM system.numbers LIMIT 5", cancellationToken: None))
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
    public async Task StreamAsync_AllIntegerWidths_RoundTripEachValue()
    {
        await using var client = TcpServerFixture.CreateClient();

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
        await foreach (Block row in client.StreamAsync(
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
    public async Task StreamAsync_ServerGeneratedArrayViaRange_ReadsBackEveryRow()
    {
        await using var client = TcpServerFixture.CreateClient();

        // range(number) yields a server-authored Array(UInt64) whose offsets and values the client never wrote:
        // row n is [0, 1, ..., n-1], so row 0 is empty and later rows grow. This validates the Array read path
        // against offsets the server produced, independent of the client's own write path.
        const int rowCount = 64;
        var readBack = new List<ulong[]>(rowCount);
        await foreach (Block block in client.StreamAsync($"SELECT range(number) AS r FROM numbers({rowCount}) ORDER BY number", cancellationToken: None))
        {
            Assert.That(block[0].TypeName, Is.EqualTo("Array(UInt64)"));
            foreach (ulong[] row in ((IColumn<ulong[]>)block[0]).Values)
            {
                readBack.Add(row);
            }
        }

        Assert.That(readBack, Has.Count.EqualTo(rowCount));
        for (int n = 0; n < rowCount; n++)
        {
            var expected = new ulong[n];
            for (int i = 0; i < n; i++)
            {
                expected[i] = (ulong)i;
            }

            Assert.That(readBack[n], Is.EqualTo(expected), $"row {n}");
        }
    }

    [Test]
    public async Task StreamAsync_SessionTimezoneSetting_ResolvesTimezoneLessColumnAgainstSessionTimezone()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["session_timezone"] = "Asia/Kolkata" },
        };

        DateTimeOffset value = default;
        // toDateTime of a Unix instant yields a timezone-less DateTime column; the client applies its own
        // session_timezone setting as that column's presentation timezone, so the value presents at +05:30.
        await foreach (Block block in client.StreamAsync("SELECT toDateTime(1700000000)", options, None))
        {
            value = ((DateTimeColumn)block[0]).GetDateTimeOffset(0);
        }

        Assert.Multiple(() =>
        {
            Assert.That(value.ToUnixTimeSeconds(), Is.EqualTo(1_700_000_000));
            Assert.That(value.Offset, Is.EqualTo(new TimeSpan(5, 30, 0)));
        });
    }

    [Test]
    public async Task StreamAsync_SessionTimezoneSetting_DoesNotLeakIntoNextQuery()
    {
        // One client reuses one connection, so all three queries run on the same session — which is what makes the
        // leak observable at all.
        await using var client = TcpServerFixture.CreateClient();

        // Baseline: the server's own timezone, with no override in play.
        TimeSpan baseline = await SelectOffsetAsync(client, null);

        // An overriding query, then a follow-up with no setting: the follow-up must match the baseline, not
        // carry the previous query's override.
        _ = await SelectOffsetAsync(client, "Asia/Kolkata");
        TimeSpan afterOverride = await SelectOffsetAsync(client, null);

        Assert.That(afterOverride, Is.EqualTo(baseline), "the session_timezone override must not persist to the next query");
    }

    private static async Task<TimeSpan> SelectOffsetAsync(IClickHouseTcpClient client, string sessionTimezone)
    {
        ClickHouseTcpQueryOptions options = sessionTimezone is null
            ? null
            : new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["session_timezone"] = sessionTimezone },
            };

        TimeSpan offset = default;
        await foreach (Block block in client.StreamAsync("SELECT toDateTime(1700000000)", options, None))
        {
            offset = ((DateTimeColumn)block[0]).GetDateTimeOffset(0).Offset;
        }

        return offset;
    }
}
