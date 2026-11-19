using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The bytes of writes that a read of the column cannot show: which values share a LowCardinality dictionary entry, the
/// duplicate keys of a Map row, the discriminators of a Variant, the value under a NULL. The literals are the bytes
/// (state prefix and body) of the codec write of the same values, captured once.
/// </summary>
[TestFixture]
public class WireBytePinTests
{
    private static readonly long NoonTicks = new DateTime(2024, 1, 15, 12, 0, 0).Ticks;
    private static readonly DateTime Second = DateTime.UnixEpoch.AddSeconds(1_700_000_000);
    private static readonly DateTimeOffset SecondOffset = DateTimeOffset.FromUnixTimeSeconds(1_700_000_000);
    private static readonly IPAddress Ipv4 = IPAddress.Parse("192.0.2.1");

    public static IEnumerable<TestCaseData> Writes()
    {
        // A UTC instant and the same ticks as a New York wall clock are two instants: two entries and the default.
        yield return Case(
            "LowCardinality(DateTime('America/New_York'))",
            new[] { new DateTime(NoonTicks, DateTimeKind.Utc), new DateTime(NoonTicks, DateTimeKind.Unspecified) },
            "01000000000000000006000000000000030000000000000000000000401EA5659064A56502000000000000000102",
            "two instants of equal ticks");
        yield return Case(
            "LowCardinality(DateTime64(0, 'America/New_York'))",
            new[] { new DateTime(NoonTicks, DateTimeKind.Utc), new DateTime(NoonTicks, DateTimeKind.Unspecified) },
            "0100000000000000000600000000000003000000000000000000000000000000401EA565000000009064A5650000000002000000000000000102",
            "two instants of equal ticks");
        yield return Case(
            "LowCardinality(Nullable(DateTime('America/New_York')))",
            new DateTime?[] { new DateTime(NoonTicks, DateTimeKind.Utc), null, new DateTime(NoonTicks, DateTimeKind.Unspecified) },
            "0100000000000000000600000000000004000000000000000000000000000000401EA5659064A5650300000000000000020003",
            "two instants of equal ticks and a NULL");

        // Values of different ticks in one second encode as one second: one entry and the default.
        yield return Case(
            "LowCardinality(DateTime('UTC'))",
            new[] { Second.AddMilliseconds(100), Second.AddMilliseconds(900) },
            "0100000000000000000600000000000002000000000000000000000000F1536502000000000000000101",
            "DateTimes in one second");
        yield return Case(
            "LowCardinality(DateTime('UTC'))",
            new[] { SecondOffset.AddMilliseconds(100), SecondOffset.AddMilliseconds(900) },
            "0100000000000000000600000000000002000000000000000000000000F1536502000000000000000101",
            "DateTimeOffsets in one second");

        // The stored value of the default shares slot 0, and a repeated value its entry.
        yield return Case(
            "LowCardinality(DateTime('UTC'))",
            new uint[] { 0, 7, 7 },
            "01000000000000000006000000000000020000000000000000000000070000000300000000000000000101",
            "the default and a repeated second");
        yield return Case(
            "LowCardinality(DateTime64(3, 'UTC'))",
            new long[] { 0, 7, 7 },
            "010000000000000000060000000000000200000000000000000000000000000007000000000000000300000000000000000101",
            "the default and a repeated count");
        yield return Case(
            "LowCardinality(Time)",
            new[] { 0, 7, 7 },
            "01000000000000000006000000000000020000000000000000000000070000000300000000000000000101",
            "the default and a repeated second");
        yield return Case(
            "LowCardinality(Time64(3))",
            new long[] { 0, 7, 7 },
            "010000000000000000060000000000000200000000000000000000000000000007000000000000000300000000000000000101",
            "the default and a repeated count");

        // CLR values that differ below the precision of the type: one entry and the default.
        yield return Case(
            "LowCardinality(BFloat16)",
            new[] { BitConverter.Int32BitsToSingle(0x3F80_0000 + 10_001), BitConverter.Int32BitsToSingle(0x3F80_0000 + 10_002) },
            "0100000000000000000600000000000002000000000000000000803F02000000000000000101",
            "floats with the same high 16 bits");
        yield return Case(
            "LowCardinality(Time)",
            new[] { TimeSpan.FromTicks(11_000_001), TimeSpan.FromTicks(11_999_999) },
            "010000000000000000060000000000000200000000000000000000000100000002000000000000000101",
            "TimeSpans in one second");
        yield return Case(
            "LowCardinality(Time64(3))",
            new[] { TimeSpan.FromTicks(10_001), TimeSpan.FromTicks(10_999) },
            "0100000000000000000600000000000002000000000000000000000000000000010000000000000002000000000000000101",
            "TimeSpans in one millisecond");
        yield return Case(
            "LowCardinality(IPv6)",
            new[] { Ipv4, Ipv4.MapToIPv6() },
            "0100000000000000000600000000000002000000000000000000000000000000000000000000000000000000000000000000FFFFC000020102000000000000000101",
            "an IPv4 address and its mapped IPv6 address");

        // A Map row keeps its duplicate keys, in order.
        yield return Case(
            "Map(String, UInt8)",
            new[] { new[] { new KeyValuePair<string, byte>("A", 1), new KeyValuePair<string, byte>("A", 2), new KeyValuePair<string, byte>("B", 3) }, Array.Empty<KeyValuePair<string, byte>>() },
            "03000000000000000300000000000000014101410142010203",
            "duplicate keys in a row");
    }

    [TestCaseSource(nameof(Writes))]
    public Task Write_Values_GiveThePinnedBytes(string type, Array values, string bytes)
        => (Task)ConverterHarness.InvokeGeneric(typeof(WireBytePinTests), nameof(AssertBytesAsync), new[] { values.GetType().GetElementType() }, type, values, bytes);

    /// <summary>
    /// The NULL of a <c>DateTime</c> in a zone that <see cref="TimeZoneInfo"/> cannot hold writes the epoch, and does not
    /// resolve the zone.
    /// </summary>
    [Test]
    public async Task Write_NullIntoAZoneThatTimeZoneInfoCannotHold_WritesTheEpoch()
    {
        const string type = "Nullable(DateTime('Fixed/UTC+19:00:00'))";
        ColumnWriter<DateTime?> writer = ConverterDerivation.Default.Writer<DateTime?>(type, ResolveContext.ForWrite);

        Assert.That(Convert.ToHexString(await ConverterHarness.WriteNewAsync(writer, new DateTime?[] { null }, 0, 1)), Is.EqualTo("0100000000"));
    }

    /// <summary>
    /// A decoded Variant column inserted into a Variant whose alternatives are in another order gets the discriminators of
    /// the target: the <c>UInt64</c> is 1 and the <c>String</c> 0 in <c>Variant(String, UInt64)</c>.
    /// </summary>
    [Test]
    public async Task Write_DecodedVariantOfAnotherOrder_WritesTheDiscriminatorsOfTheTarget()
    {
        const string type = "Variant(String, UInt64)";
        using var dense = new VariantColumn(
            "v",
            "Variant(UInt64, String)",
            new byte[] { 0, 1 },
            new[] { DecodedColumns.Of("v", "UInt64", new ulong[] { 42 }), DecodedColumns.Of("v", "String", "hi") },
            rowCount: 2,
            pooledDiscriminators: false,
            ownsColumns: false);
        InsertColumnWrite write = InsertColumnWrite.For(ConverterHarness.Codec(type), dense, type, ConverterHarness.Context, ConverterDerivation.Default);

        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            IColumnWriteState state = write.Begin(dense, 0, 2);
            try
            {
                write.WritePrefix(w, dense, 0, 2, state);
                write.Write(w, dense, 0, 2, state);
            }
            finally
            {
                state?.Dispose();
            }
        });

        // The discriminators mode, the discriminators (UInt64, String), the String run "hi", the UInt64 run 42.
        Assert.That(Convert.ToHexString(bytes), Is.EqualTo("0000000000000000" + "0100" + "026869" + "2A00000000000000"));
    }

    private static TestCaseData Case<T>(string type, T[] values, string bytes, string what)
        => new TestCaseData(type, values, bytes).SetArgDisplayNames(type, what);

    private static async Task AssertBytesAsync<T>(string type, T[] values, string bytes)
    {
        ColumnWriter<T> writer = ConverterDerivation.Default.Writer<T>(type, ConverterHarness.Context);

        Assert.That(Convert.ToHexString(await ConverterHarness.WriteNewAsync(writer, values, 0, values.Length)), Is.EqualTo(bytes));
    }
}
