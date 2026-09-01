using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Sample columns for the INSERT → SELECT round-trip integration tests, in the spirit of the HTTP suite's
/// <c>TestCases</c>: one place that enumerates a representative value set per supported type, so a single
/// parameterized test exercises every type rather than a hand-written test each. The test creates a matching
/// one-column table, inserts the case's column, selects it back, and asserts the read-back column equals the
/// expected column. Covers the types the native codecs support today — the fixed-width integers, the raw enum
/// aliases, <c>String</c>, and <c>DateTime</c>.
///
/// <para>
/// A case usually inserts and reads back the same CLR type, so the inserted column doubles as the expected one.
/// Some cases differ, though — inserting a <c>DateTimeOffset</c> into a <c>DateTime</c> column reads back a
/// <c>DateTime</c> — so a case carries both an insert-column builder and an expected-column builder; the common
/// factories set them to the same builder.
/// </para>
/// </summary>
public sealed class InsertRoundTripCase
{
    private readonly Func<string, IColumn> buildInsert;
    private readonly Func<string, IColumn> buildExpected;

    private InsertRoundTripCase(
        string label,
        string clickHouseType,
        Func<string, IColumn> buildInsert,
        Func<string, IColumn> buildExpected,
        IReadOnlyDictionary<string, string> settings)
    {
        Label = label;
        ClickHouseType = clickHouseType;
        this.buildInsert = buildInsert;
        this.buildExpected = buildExpected;
        Settings = settings;
    }

    /// <summary>The ClickHouse type for the target column (used both to create the table and as the column header).</summary>
    public string ClickHouseType { get; }

    /// <summary>
    /// Per-query settings the round-trip must run with (applied to the CREATE, INSERT, and SELECT), or null for
    /// none — used to enable the experimental-type flags some newer types require (e.g. <c>Time</c>, <c>BFloat16</c>).
    /// </summary>
    public IReadOnlyDictionary<string, string> Settings { get; }

    private string Label { get; }

    /// <summary>Builds the column to insert, stamped with <paramref name="columnName"/>.</summary>
    /// <param name="columnName">The column name, which must match the target table column.</param>
    /// <returns>The column to insert.</returns>
    internal IColumn BuildInsertColumn(string columnName) => buildInsert(columnName);

    /// <summary>Builds the column whose values the read-back column is expected to equal.</summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>The expected column.</returns>
    internal IColumn BuildExpectedColumn(string columnName) => buildExpected(columnName);

    public override string ToString() => Label;

    /// <summary>All round-trip cases, for use as an NUnit <c>TestCaseSource</c>.</summary>
    public static IEnumerable<InsertRoundTripCase> Cases()
    {
        yield return Primitive("UInt8", new byte[] { 0, 1, 128, 255 });
        yield return Primitive("Int8", new sbyte[] { -128, -1, 0, 127 });
        yield return Primitive("UInt16", new ushort[] { 0, 258, ushort.MaxValue });
        yield return Primitive("Int16", new short[] { short.MinValue, -1, 0, short.MaxValue });
        yield return Primitive("UInt32", new uint[] { 0, 1, uint.MaxValue });
        yield return Primitive("Int32", new[] { int.MinValue, -1, 0, int.MaxValue });
        yield return Primitive("UInt64", new ulong[] { 0, 1, ulong.MaxValue });
        yield return Primitive("Int64", new[] { long.MinValue, -1, 0, long.MaxValue });
        yield return Primitive("UInt128", new[] { UInt128.Zero, UInt128.One, UInt128.MaxValue });
        yield return Primitive("Int128", new[] { Int128.MinValue, -Int128.One, Int128.Zero, Int128.MaxValue });
        // 2^200 pins the limb order; the values after it pin the top limb and the sign bit, which everything
        // below 2^255 leaves clear.
        yield return Primitive("UInt256", new[]
        {
            UInt256.Zero,
            UInt256.FromBigInteger(1),
            UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)),
            UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 255)),
            UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 256) - 1),
        });
        yield return Primitive("Int256", new[]
        {
            Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 200)),
            Int256.FromBigInteger(-1),
            Int256.Zero,
            Int256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)),
            Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 255)),
            Int256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 255) - 1),
        });

        // Enum columns are inserted and read as their raw underlying ordinals; the ordinals must be declared members.
        yield return Primitive("Enum8('a' = -1, 'b' = 127)", new sbyte[] { -1, 127 });
        yield return Primitive("Enum16('x' = -32768, 'y' = 32767)", new short[] { -32768, 32767 });

        // A column of labels is the other write shape: it converts to the declared ordinals, which is what reads back.
        yield return EnumLabels("Enum8('a' = -1, 'b' = 127)", new sbyte[] { -1, 127 }, "a", "b");
        yield return EnumLabels("Enum16('x' = -32768, 'y' = 32767)", new short[] { -32768, 32767 }, "x", "y");

        // A label carrying an escape. The header spells it 'a\nb' — on 26.6 the label's stored bytes are 61 0A 62 —
        // so the label a caller writes has to be the decoded one, and only a server proves the two agree.
        yield return EnumLabels(@"Enum8('a\nb' = 1, 't\tab' = 2)", new sbyte[] { 1, 2 }, "a\nb", "t\tab");

        // Labels holding the grammar's own separators: a comma, an escaped quote, and an equals sign. The member
        // splitter must not cut on the comma or end the token at the quote, and ToString() has to re-emit the raw
        // spelling into the insert header while ToOrdinal looks the decoded label up.
        yield return EnumLabels(@"Enum8('a,b' = 1, 'c\'d' = 2, 'e = f' = 3)", new sbyte[] { 1, 2, 3 }, "a,b", "c'd", "e = f");

        // And through the wrappers, where the shape has to survive composition: the nullable substitute needs a
        // placeholder label for its null rows, and the array path flattens the labels before the enum sees them.
        yield return NullableEnumLabels("Enum8('a' = -1, 'b' = 127)", new sbyte?[] { -1, null, 127 }, "a", null, "b");
        yield return ArrayEnumLabels("Enum8('a' = -1, 'b' = 127)", new[] { new sbyte[] { -1, 127 }, Array.Empty<sbyte>() }, new[] { "a", "b" }, Array.Empty<string>());

        // Floats and Bool are direct blittable maps, so the primitive factory covers them. NaN and the infinities
        // are the patterns a conversion through a decimal text form would lose; signed zero rides along, and
        // FloatSpecialValueIntegrationTests is where its sign is actually observable.
        yield return Primitive("Float32", new[] { 0f, -0f, 1.5f, -1.5f, float.MinValue, float.MaxValue, float.NaN, float.PositiveInfinity, float.NegativeInfinity });
        yield return Primitive("Float64", new[] { 0d, -0d, 1.5, -1.5e100, double.MinValue, double.MaxValue, double.NaN, double.PositiveInfinity, double.NegativeInfinity });
        yield return Primitive("Bool", new[] { false, true, true, false });

        yield return Strings("String", string.Empty, "hello", "héllo✓", "a\0b", new string('x', 500));

        // A String is a byte string, so a byte[] per row is the other write shape; it reads back as the text those
        // bytes spell. The non-UTF-8 case is in StringBytesIntegrationTests, where the point is that it survives.
        yield return StringBytes(new[] { new byte[] { 0x61 }, Array.Empty<byte>(), new byte[] { 0x62, 0x63 } }, "a", string.Empty, "bc");
        yield return NullableStringBytes(new[] { new byte[] { 0x61 }, null, Array.Empty<byte>() }, "a", null, string.Empty);

        // FixedString(N): N contiguous bytes per row, surfaced as a per-row byte[] of exactly N bytes. The bytes
        // are byte-oriented, so embedded NULs and non-UTF-8 bytes ride along unchanged. A wider N crosses the
        // stride past a single row so a mis-strided blit could not pass unnoticed.
        yield return FixedStrings(4, new byte[] { 0, 0, 0, 0 }, new byte[] { 1, 2, 3, 4 }, new byte[] { 0xFF, 0x00, 0xFF, 0x00 });
        yield return FixedStrings(200, Enumerable.Range(0, 200).Select(i => (byte)i).ToArray(), new byte[200]);

        yield return Dates("Date", new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2149, 6, 6));
        yield return Dates("Date32", new DateOnly(1900, 1, 1), new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2299, 12, 31));

        // DateTime reads back as the raw UInt32 epoch seconds. Insert as DateTime (UTC) and expect the epoch
        // seconds of the same instants, regardless of the timezone the server presents.
        // 2100 is past the signed-32-bit second count that a narrowing cast would wrap, and the last value is the
        // largest a DateTime column holds: 2106-02-07 06:28:15 UTC, uint.MaxValue seconds.
        yield return DateTimes(
            "DateTime",
            new DateTime(1988, 8, 28, 11, 22, 33, DateTimeKind.Utc),
            new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            DateTime.UnixEpoch,
            new DateTime(2100, 6, 15, 12, 0, 0, DateTimeKind.Utc),
            DateTime.UnixEpoch.AddSeconds(uint.MaxValue));

        // A DateTimeOffset with a non-UTC offset survives as the same instant (i.e. the same epoch seconds).
        var dateTimeOffsets = new[]
        {
            new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)),
            new DateTimeOffset(1988, 8, 28, 11, 22, 33, TimeSpan.FromHours(-8)),
            new DateTimeOffset(2100, 6, 15, 12, 0, 0, TimeSpan.FromHours(5)),
        };
        yield return new InsertRoundTripCase(
            "DateTime <- DateTimeOffset",
            "DateTime",
            name => new ArrayColumn<DateTimeOffset>(name, "DateTime", dateTimeOffsets),
            name => new ArrayColumn<uint>(name, "DateTime", Array.ConvertAll(dateTimeOffsets, o => (uint)o.ToUnixTimeSeconds())),
            settings: null);

        // DateTime64 surfaces as the raw Int64 count at the column's scale, so it retains the exact wire value at
        // any scale. Scale 9 (nanoseconds) is finer than a .NET tick, proving precision no DateTimeOffset can hold.
        yield return DateTime64s("DateTime64(3)", 0L, 1_700_000_000_123L, -6_000_000_000_000L);

        // Both Int64 ends, which at scale 9 are the instants the type stops at (2262-04-11 23:47:16.854775807 and
        // its negative mirror). The count is the wire value, so these pin the limbs of the widest column value.
        yield return DateTime64s("DateTime64(9)", 0L, 1_700_000_000_123_456_789L, -1_000_000_001L, long.MaxValue, long.MinValue);

        // DateTime64 also accepts a DateTimeOffset on write, converting the instant to the column's scale; the
        // read-back is still the raw count, so at scale 3 the expected column carries epoch milliseconds. The
        // mirror of the "DateTime <- DateTimeOffset" case above.
        var dateTime64Offsets = new[]
        {
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123),
            new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)),
            new DateTimeOffset(2299, 12, 31, 23, 59, 59, TimeSpan.Zero),
        };
        yield return new InsertRoundTripCase(
            "DateTime64(3) <- DateTimeOffset",
            "DateTime64(3)",
            name => new ArrayColumn<DateTimeOffset>(name, "DateTime64(3)", dateTime64Offsets),
            name => new ArrayColumn<long>(name, "DateTime64(3)", Array.ConvertAll(dateTime64Offsets, o => o.ToUnixTimeMilliseconds())),
            settings: null);

        // The last instant a scale-9 column can take from a DateTimeOffset: .NET ticks are 100 ns, so the finest
        // value expressible is 2262-04-11 23:47:16.8547758, and scaling it up lands 7 nanoseconds short of
        // Int64.MaxValue. The expected count is written out rather than computed, so the multiply under test is not
        // also the oracle. One tick more overflows, which DateTime64ColumnCodecTests covers.
        var dateTime64NanosecondOffsets = new[] { new DateTimeOffset(2262, 4, 11, 23, 47, 16, TimeSpan.Zero).AddTicks(8_547_758) };
        yield return new InsertRoundTripCase(
            "DateTime64(9) <- DateTimeOffset [latest instant]",
            "DateTime64(9)",
            name => new ArrayColumn<DateTimeOffset>(name, "DateTime64(9)", dateTime64NanosecondOffsets),
            name => new ArrayColumn<long>(name, "DateTime64(9)", new[] { 9_223_372_036_854_775_800L }),
            settings: null);

        yield return Uuids("UUID", Guid.Empty, new Guid("00112233-4455-6677-8899-aabbccddeeff"), new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"));

        yield return IpAddresses("IPv4", "0.0.0.0", "127.0.0.1", "192.168.1.1", "255.255.255.255");
        yield return IpAddresses("IPv6", "::", "::1", "2001:db8::1", "fe80::1", "2001:db8:85a3:8d3:1319:8a2e:370:7348");

        // An IPv4 address written to an IPv6 column comes back in its IPv4-mapped form, so the insert value and
        // the expected value differ. Nothing else in the corpus reads that mapping back off a server.
        var mappedIpv4 = new[] { IPAddress.Parse("192.168.1.1"), IPAddress.Parse("0.0.0.0") };
        yield return new InsertRoundTripCase(
            "IPv6 <- IPv4",
            "IPv6",
            name => new ArrayColumn<IPAddress>(name, "IPv6", mappedIpv4),
            name => new ArrayColumn<IPAddress>(name, "IPv6", new[] { IPAddress.Parse("::ffff:192.168.1.1"), IPAddress.Parse("::ffff:0.0.0.0") }),
            settings: null);

        // Decimal32/64 surface as System.Decimal; Decimal128/256 as ClickHouseTcpDecimal.
        yield return Decimals("Decimal(9, 2)", 0m, 1.23m, -1.23m, 9999999.99m);
        yield return Decimals("Decimal(18, 4)", 0m, 12345.6789m, -12345.6789m, 99999999999999.9999m);
        yield return WideDecimals("Decimal(38, 10)", "0", "12345.6789", "-98765.4321");
        yield return WideDecimals("Decimal(76, 20)", "0", "1.00000000000000000001", "-1.00000000000000000001");

        // Scale 0 takes neither of FixedPointScaling.ShiftDecimalPlaces's branches, and scale == precision leaves
        // no integer part at all. The last case holds the largest magnitude a Decimal(76, 0) can, which pins the
        // top limb of the 256-bit mantissa.
        yield return WideDecimals("Decimal(38, 0)", "0", "-1", "99999999999999999999999999999999999999");
        yield return WideDecimals("Decimal(38, 38)", "0.00000000000000000000000000000000000001", "-0.99999999999999999999999999999999999999");
        yield return WideDecimals("Decimal(76, 0)", "0", "9999999999999999999999999999999999999999999999999999999999999999999999999999", "-9999999999999999999999999999999999999999999999999999999999999999999999999999");

        // The DecimalN(S) alias spellings resolve to the same codecs as Decimal(P, S); one case proves the server
        // round-trips the alias type name as declared.
        yield return Decimals("Decimal64(4)", 0m, 12345.6789m, -12345.6789m);

        // Interval<Unit> surfaces its underlying Int64 count; the unit is kept in the type name.
        yield return Primitive("IntervalSecond", new[] { 0L, 1L, -5L, long.MaxValue });
        yield return Primitive("IntervalDay", new[] { 0L, 7L, -30L });

        // Newer/experimental server types: enable their flag on the round-trip
        yield return BFloat16s("BFloat16", BFloat16Settings, 0f, -0f, 1f, -2f, 0.5f, 100f, float.NaN, float.PositiveInfinity, float.NegativeInfinity);
        // Time surfaces as the raw Int32 seconds; Time64 as the raw Int64 count at the column's scale. The
        // inserted values are the exact wire values, returned verbatim.
        yield return TimeSeconds("Time", TimeSettings, 0, (12 * 3600) + (34 * 60) + 56, -((1 * 3600) + (2 * 60) + 3));
        yield return Time64Counts("Time64(3)", TimeSettings, 0L, (((1 * 3600) + (2 * 60) + 3) * 1000L) + 456, -((((1 * 3600) + (2 * 60) + 3) * 1000L) + 456));
        yield return Time64Counts("Time64(9)", TimeSettings, 0L, 3_723_123_456_789L, -3_723_123_456_789L);

        // Time/Time64 also accept a TimeSpan on write, truncating toward zero at the column's scale. The read-back
        // is the raw count, so the expected column carries the truncated count — integer division on Ticks, which
        // is the truncation the codecs perform. The sub-scale entries prove the truncation survives the server.
        var timeSpans = new[] { TimeSpan.Zero, new TimeSpan(12, 34, 56), new TimeSpan(0, 0, 0, 1, 500) };
        yield return new InsertRoundTripCase(
            "Time <- TimeSpan",
            "Time",
            name => new ArrayColumn<TimeSpan>(name, "Time", timeSpans),
            name => new ArrayColumn<int>(name, "Time", Array.ConvertAll(timeSpans, t => (int)(t.Ticks / TimeSpan.TicksPerSecond))),
            TimeSettings);

        var time64Spans = new[] { TimeSpan.Zero, new TimeSpan(0, 1, 2, 3, 456), TimeSpan.FromTicks(4_560_789) };
        yield return new InsertRoundTripCase(
            "Time64(3) <- TimeSpan",
            "Time64(3)",
            name => new ArrayColumn<TimeSpan>(name, "Time64(3)", time64Spans),
            name => new ArrayColumn<long>(name, "Time64(3)", Array.ConvertAll(time64Spans, t => t.Ticks / TimeSpan.TicksPerMillisecond)),
            TimeSettings);

        // Nullable(T): one case per supported inner type. A value inner surfaces as T?, a reference inner as the
        // nullable reference; each case interleaves nulls with present values, and the all-null cases exercise
        // the placeholder-only values stream. IMPORTANT: when adding a new type to this list, add a Nullable(that
        // type) case here too — Nullable exercises a distinct write path (null-map + per-type null placeholder)
        // that the bare type does not.
        yield return NullableValues<byte>("UInt8", 0, null, byte.MaxValue);
        yield return NullableValues<sbyte>("Int8", sbyte.MinValue, null, sbyte.MaxValue);
        yield return NullableValues<ushort>("UInt16", 0, null, ushort.MaxValue);
        yield return NullableValues<short>("Int16", short.MinValue, null, short.MaxValue);
        yield return NullableValues<uint>("UInt32", 0, null, uint.MaxValue);
        yield return NullableValues<int>("Int32", int.MinValue, null, 0, int.MaxValue);
        yield return NullableValues<int>("Int32", (int?)null, null); // every row null: the values stream is all placeholder
        yield return NullableValues<ulong>("UInt64", 0, null, ulong.MaxValue);
        yield return NullableValues<long>("Int64", long.MinValue, null, long.MaxValue);
        yield return NullableValues<UInt128>("UInt128", UInt128.Zero, null, UInt128.MaxValue);
        yield return NullableValues<Int128>("Int128", Int128.MinValue, null, Int128.MaxValue);
        yield return NullableValues<UInt256>("UInt256", UInt256.Zero, null, UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)));
        yield return NullableValues<Int256>("Int256", Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 200)), null, Int256.Zero);
        // A special next to a null, because the null map and the value run are written separately: the placeholder
        // a null row contributes must not be mistaken for the NaN beside it, or the other way round.
        yield return NullableValues<float>("Float32", 0f, null, -1.5f, float.MaxValue, float.NaN, null, float.PositiveInfinity, -0f);
        yield return NullableValues<double>("Float64", 1.5, null, -1.5e100, null, double.NaN, double.NegativeInfinity, -0d);
        yield return NullableValues<bool>("Bool", true, null, false);

        // Nullable over a composite, which the server allows for Tuple behind a setting. The write path has to
        // project the inner column before the state-prefix phase: the tuple builds its write state there and
        // needs a column of (byte, string), not of (byte, string)?. A null row beside a row of inner defaults
        // keeps the two distinguishable.
        yield return Same(
            "Nullable(Tuple(UInt8, String))",
            "Nullable(Tuple(UInt8, String))",
            name => new ArrayColumn<(byte, string)?>(name, "Nullable(Tuple(UInt8, String))", new (byte, string)?[]
            {
                ((byte)7, "x"),
                null,
                ((byte)0, string.Empty),
            }),
            NullableTupleSettings);
        yield return NullableValues<sbyte>("Enum8('a' = -1, 'b' = 127)", -1, null, 127);
        yield return NullableValues<short>("Enum16('x' = -32768, 'y' = 32767)", -32768, null, 32767);
        yield return NullableValues<DateOnly>("Date", new DateOnly(1970, 1, 1), null, new DateOnly(2149, 6, 6));
        yield return NullableValues<DateOnly>("Date32", new DateOnly(1900, 1, 1), null, new DateOnly(2299, 12, 31));
        yield return NullableValues<Guid>("UUID", Guid.Empty, null, new Guid("00112233-4455-6677-8899-aabbccddeeff"));
        yield return NullableValues<decimal>("Decimal(9, 2)", 1.23m, null, -1.23m, 9999999.99m);
        yield return NullableValues<decimal>("Decimal(18, 4)", 12345.6789m, null, -12345.6789m);
        yield return NullableWideDecimals("Decimal(38, 10)", "12345.6789", null, "-98765.4321");
        yield return NullableWideDecimals("Decimal(76, 20)", "1.00000000000000000001", null, "-1.00000000000000000001");
        yield return NullableValues<long>("IntervalSecond", 0L, null, -5L);

        // DateTime reads back a DateTimeOffset?; equality is by instant, so the presented offset does not matter.
        yield return NullableDateTimes(
            new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero),
            null,
            new DateTimeOffset(1988, 8, 28, 11, 22, 33, TimeSpan.Zero),
            DateTimeOffset.UnixEpoch.AddSeconds(uint.MaxValue));
        yield return NullableDateTime64s(3, 0L, null, 1_700_000_000_123L, null);
        yield return NullableDateTime64s(9, 1_700_000_000_123_456_789L, null, -1_000_000_001L, long.MaxValue);

        // Nullable re-offers every CLR write spelling the bare inner accepts, each with its own-typed null
        // placeholder — so Nullable(DateTime) takes DateTimeOffset? or DateTime?, and Nullable(DateTime64) takes
        // long?, DateTimeOffset? or DateTime?. The cases above only cover the first spelling of each, which left
        // the alternates proven by unit tests alone; these send them to a server.
        var nullableDateTimes = new DateTime?[] { DateTime.UnixEpoch.AddSeconds(1_700_000_000), null, DateTime.UnixEpoch };
        yield return new InsertRoundTripCase(
            "Nullable(DateTime) <- DateTime?",
            "Nullable(DateTime)",
            name => new ArrayColumn<DateTime?>(name, "Nullable(DateTime)", nullableDateTimes),
            name => new ArrayColumn<uint?>(name, "Nullable(DateTime)", Array.ConvertAll(nullableDateTimes, v => v is null ? (uint?)null : (uint)new DateTimeOffset(v.Value.ToUniversalTime()).ToUnixTimeSeconds())),
            settings: null);

        var nullableOffsets = new DateTimeOffset?[] { DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123), null };
        yield return new InsertRoundTripCase(
            "Nullable(DateTime64(3)) <- DateTimeOffset?",
            "Nullable(DateTime64(3))",
            name => new ArrayColumn<DateTimeOffset?>(name, "Nullable(DateTime64(3))", nullableOffsets),
            name => new ArrayColumn<long?>(name, "Nullable(DateTime64(3))", Array.ConvertAll(nullableOffsets, o => o?.ToUnixTimeMilliseconds())),
            settings: null);

        var nullable64DateTimes = new DateTime?[] { DateTime.UnixEpoch.AddMilliseconds(1_700_000_000_123), null };
        yield return new InsertRoundTripCase(
            "Nullable(DateTime64(3)) <- DateTime?",
            "Nullable(DateTime64(3))",
            name => new ArrayColumn<DateTime?>(name, "Nullable(DateTime64(3))", nullable64DateTimes),
            name => new ArrayColumn<long?>(name, "Nullable(DateTime64(3))", Array.ConvertAll(nullable64DateTimes, v => v is null ? (long?)null : new DateTimeOffset(v.Value.ToUniversalTime()).ToUnixTimeMilliseconds())),
            settings: null);

        // Experimental server types: enable their flag on the round-trip (same as their non-nullable cases).
        yield return NullableValues<float>("BFloat16", BFloat16Settings, 0f, null, 1f, -2f, float.NaN, null, float.PositiveInfinity);
        yield return NullableValues<int>("Time", TimeSettings, 0, null, (12 * 3600) + (34 * 60) + 56);
        yield return NullableValues<long>("Time64(3)", TimeSettings, 0L, null, (((1 * 3600) + (2 * 60) + 3) * 1000L) + 456);

        yield return NullableStrings("hello", null, "world", string.Empty);
        yield return NullableStrings(null, null); // every row null

        // Nullable(FixedString(N)): byte[] is reference-typed, so a null row surfaces as null; present rows are
        // exactly N bytes. A null row must not reach the FixedString codec (the nullable write substitutes the
        // N-zero-byte placeholder instead), so the all-null case proves the placeholder-only values stream.
        yield return NullableFixedStrings(4, new byte[] { 1, 2, 3, 4 }, null, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });
        yield return NullableFixedStrings(4, null, null); // every row null

        // IPv4/IPv6 are reference-typed (IPAddress) but fixed-width; a null row must not reach the IP codec (it
        // dereferences the address), so the nullable write substitutes a placeholder instead.
        yield return NullableIps("IPv4", "127.0.0.1", null, "255.255.255.255");
        yield return NullableIps("IPv6", "::1", null, "2001:db8::1");
        yield return NullableIps("IPv4", null, null); // every row null

        // Array(T): one case per supported inner element type, so every type also survives being wrapped in an
        // Array — a distinct write path (offsets stream + a single flattened values stream). Each row surfaces as
        // the inner element array; empty rows (equal consecutive offsets) and all-empty columns exercise the
        // zero-length paths. Array is the one exception to the "wrap every type in Nullable" rule — the server
        // rejects Nullable(Array(T)) — so nullability is composed the other way here, as Array(Nullable(T)).
        yield return Arrays("UInt8", new byte[] { 0, 128, 255 }, Array.Empty<byte>(), new byte[] { 1 });
        yield return Arrays("Int8", new sbyte[] { -128, -1, 0, 127 }, Array.Empty<sbyte>());
        yield return Arrays("UInt16", new ushort[] { 0, 258, ushort.MaxValue }, new ushort[] { 1 });
        yield return Arrays("Int16", new short[] { short.MinValue, -1, 0, short.MaxValue });
        yield return Arrays("UInt32", new uint[] { 10, 20, 30 }, Array.Empty<uint>(), new uint[] { 40, 50 });
        yield return Arrays("Int32", new[] { int.MinValue, -1, 0, int.MaxValue }, Array.Empty<int>());
        yield return Arrays("UInt64", new ulong[] { 0, 1, ulong.MaxValue });
        yield return Arrays("Int64", new[] { long.MinValue, 0L }, new[] { long.MaxValue });
        yield return Arrays("UInt128", new[] { UInt128.Zero, UInt128.One, UInt128.MaxValue });
        yield return Arrays("Int128", new[] { Int128.MinValue, Int128.Zero, Int128.MaxValue });
        yield return Arrays("UInt256", new[] { UInt256.Zero, UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)) });
        yield return Arrays("Int256", new[] { Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 200)), Int256.Zero });
        yield return Arrays("Enum8('a' = -1, 'b' = 127)", new sbyte[] { -1, 127 }, Array.Empty<sbyte>());
        yield return Arrays("Enum16('x' = -32768, 'y' = 32767)", new short[] { -32768, 32767 });
        yield return Arrays("Float32", new[] { 0f, 1.5f, -1.5f, float.MaxValue }, Array.Empty<float>(), new[] { float.NaN, float.PositiveInfinity, float.NegativeInfinity, -0f });
        yield return Arrays("Float64", new[] { 0d, -1.5e100, double.MaxValue }, new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity, -0d });
        yield return Arrays("Bool", new[] { true, false, true }, Array.Empty<bool>());
        yield return Arrays("String", new[] { "a", "bb" }, Array.Empty<string>(), new[] { string.Empty, "héllo✓" });
        yield return Arrays<byte[]>("FixedString(4)", new[] { new byte[] { 1, 2, 3, 4 }, new byte[] { 0xFF, 0, 0xFF, 0 } }, Array.Empty<byte[]>());
        yield return Arrays("Date", new[] { new DateOnly(1970, 1, 1), new DateOnly(2149, 6, 6) }, Array.Empty<DateOnly>());
        yield return Arrays("Date32", new[] { new DateOnly(1900, 1, 1), new DateOnly(2299, 12, 31) });

        // Array(DateTime) reads back raw uint epoch seconds; Array(DateTime64) raw long counts at the column scale.
        // The shared corpus uses canonical CLR types; lifted types have focused coverage.
        yield return Arrays<uint>("DateTime", new uint[] { 1_700_000_000, 0, uint.MaxValue }, Array.Empty<uint>());
        yield return Arrays<long>("DateTime64(3)", new[] { 0L, 1_700_000_000_123L });
        yield return Arrays<long>("DateTime64(9)", new[] { 1_700_000_000_123_456_789L, long.MaxValue, long.MinValue }, Array.Empty<long>());

        // The dense shape built by a caller rather than received from a read: flat elements plus per-row offsets,
        // which is what the codec writes with no rebuilding. Same rows as the jagged Array(UInt32) case above.
        yield return DenseArrays("UInt32", new uint[] { 10, 20, 30, 40, 50 }, new[] { 0, 3, 3, 5 });

        yield return Arrays("UUID", new[] { Guid.Empty }, new[] { new Guid("00112233-4455-6677-8899-aabbccddeeff"), new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff") });
        yield return Arrays<IPAddress>("IPv4", new[] { IPAddress.Parse("0.0.0.0"), IPAddress.Parse("255.255.255.255") }, Array.Empty<IPAddress>());
        yield return Arrays<IPAddress>("IPv6", new[] { IPAddress.Parse("::1"), IPAddress.Parse("2001:db8::1") });

        yield return Arrays("Decimal(9, 2)", new[] { 0m, 1.23m, -1.23m, 9999999.99m }, Array.Empty<decimal>());
        yield return Arrays("Decimal(18, 4)", new[] { 12345.6789m, -12345.6789m });
        yield return Arrays<ClickHouseTcpDecimal>("Decimal(38, 10)", new[] { ParseWide("12345.6789"), ParseWide("-98765.4321") });
        yield return Arrays<ClickHouseTcpDecimal>("Decimal(76, 20)", new[] { ParseWide("1.00000000000000000001"), ParseWide("-1.00000000000000000001") });

        yield return Arrays("IntervalSecond", new[] { 0L, 1L, -5L }, Array.Empty<long>());
        yield return Arrays("IntervalDay", new[] { 7L, -30L });

        // Experimental server types: enable their flag on the round-trip (same as their bare cases).
        yield return Arrays("BFloat16", BFloat16Settings, new[] { 0f, 1f, -2f, 0.5f }, Array.Empty<float>(), new[] { float.NaN, float.PositiveInfinity, float.NegativeInfinity, -0f });
        yield return Arrays("Time", TimeSettings, new[] { 0, (12 * 3600) + (34 * 60) + 56 }, Array.Empty<int>());
        yield return Arrays("Time64(3)", TimeSettings, new[] { 0L, (((1 * 3600) + (2 * 60) + 3) * 1000L) + 456 });

        // Array(Nullable(T)): nullability composed inside the array, for both a value inner and a reference inner.
        yield return Arrays<uint?>("Nullable(UInt32)", new uint?[] { 1, null, 3 }, Array.Empty<uint?>(), new uint?[] { null });
        yield return Arrays<int?>("Nullable(Int32)", new int?[] { int.MinValue, null, 0 });
        yield return Arrays<string>("Nullable(String)", new[] { "x", null, string.Empty }, new string[] { null });
        yield return Arrays<IPAddress>("Nullable(IPv4)", new[] { IPAddress.Parse("127.0.0.1"), null });

        // Nested arrays: the same offsets-plus-values shape recurses one level down.
        yield return Arrays<byte[]>("Array(UInt8)", new[] { new byte[] { 1, 2 } }, Array.Empty<byte[]>(), new[] { new byte[] { 3 }, new byte[] { 4, 5 } });
        yield return Arrays<string[]>("Array(String)", new[] { new[] { "a" }, new[] { "b", "c" } }, Array.Empty<string[]>());

        // Every row empty: the offsets stream is all zeroes and the values stream is absent entirely. The cases
        // above all have at least one non-empty row, so nothing exercised that shape end to end.
        yield return Arrays("UInt32", Array.Empty<uint>(), Array.Empty<uint>(), Array.Empty<uint>());

        // An empty *inner* array — the only shape where the inner codec's own offsets stream holds an equal
        // consecutive pair. The nested cases above only ever have an empty outer row.
        yield return Arrays<byte[]>("Array(UInt8)", new[] { Array.Empty<byte>(), new byte[] { 1 } }, Array.Empty<byte[]>());

        // Array(Array(Nullable(T))) composes the array recursion with the nullable state prefix, which the
        // Array(Nullable(T)) and Array(Array(T)) cases each cover only half of.
        yield return Arrays<uint?[]>("Array(Nullable(UInt32))", new[] { new uint?[] { 1, null } }, Array.Empty<uint?[]>());

        // The deep-nesting ladder, Array(UInt8) wrapped 2 to 5 levels plus a String and a Nullable leaf. Every level
        // carries an empty row and an uneven run, so no level's offsets stream is trivial. See NestedArrayShape; the
        // block-splitting tests read the same shapes.
        foreach (NestedArrayShape shape in NestedArrayShape.Shapes())
        {
            yield return Same(shape.ToString(), shape.ClickHouseType, shape.BuildColumn);
        }

        // Tuple(...): a heterogeneous fixed-arity composite serialized as N side-by-side element columns. The
        // cases below collectively touch every supported element type across various arities (1 through the
        // supported maximum of 7), then compose tuples with named elements, nesting, Nullable and Array elements,
        // and an Array(Tuple(...)) that flattens through the tuple codec's per-element write path. Element names
        // do not change the CLR value (a ValueTuple either way); they only ride along in the type string.
        yield return Same(
            "Tuple(Int32) [arity 1]",
            "Tuple(Int32)",
            name => new TupleColumn<int>(name, "Tuple(Int32)", new[] { new ValueTuple<int>(1), new ValueTuple<int>(int.MinValue), new ValueTuple<int>(int.MaxValue) }));

        // FixedString(N) as a tuple element: the write path reaches the FixedString codec through a
        // TupleFieldColumn projection rather than a dense blob, so it takes the strict per-value branch instead of
        // the bulk blit — the one entrance the bare, Nullable and Array cases all miss.
        yield return Same(
            "Tuple(FixedString(4), String)",
            "Tuple(FixedString(4), String)",
            name => new TupleColumn<byte[], string>(name, "Tuple(FixedString(4), String)", new (byte[], string)[]
            {
                (new byte[] { 1, 2, 3, 4 }, "a"),
                (new byte[] { 0xFF, 0x00, 0xFF, 0x00 }, string.Empty),
            }));

        // Arity 3 was the one arity between 1 and 7 with no case at all.
        yield return Same(
            "Tuple(Int32, String, Float64) [arity 3]",
            "Tuple(Int32, String, Float64)",
            name => new TupleColumn<int, string, double>(name, "Tuple(Int32, String, Float64)", new (int, string, double)[]
            {
                (1, "a", 1.5),
                (-2, string.Empty, -1.5e100),
            }));

        // A flat ArrayColumn<ValueTuple> is not an ITupleColumn, so a top-level Tuple supplied that way takes the
        // ergonomic boxed per-element projection instead of the dense child-column path. Every other Tuple case
        // builds the dense TupleColumn, so the projection was only reachable at top level from a unit test; the
        // read still comes back dense, hence the differing expected builder.
        var flatTupleRows = new (int, string)[] { (1, "a"), (2, "bb"), (3, "ccc") };
        yield return new InsertRoundTripCase(
            "Tuple(Int32, String) <- flat ArrayColumn",
            "Tuple(Int32, String)",
            name => new ArrayColumn<(int, string)>(name, "Tuple(Int32, String)", flatTupleRows),
            name => new TupleColumn<int, string>(name, "Tuple(Int32, String)", flatTupleRows),
            settings: null);

        yield return Same(
            "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32)",
            "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32)",
            name => new TupleColumn<byte, sbyte, ushort, short, uint, int>(name, "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32)", new (byte, sbyte, ushort, short, uint, int)[]
            {
                (0, -128, 0, short.MinValue, 0, int.MinValue),
                (255, 127, ushort.MaxValue, short.MaxValue, uint.MaxValue, int.MaxValue),
            }));

        yield return Same(
            "Tuple(UInt64, Int64, UInt128, Int128, UInt256, Int256)",
            "Tuple(UInt64, Int64, UInt128, Int128, UInt256, Int256)",
            name => new TupleColumn<ulong, long, UInt128, Int128, UInt256, Int256>(name, "Tuple(UInt64, Int64, UInt128, Int128, UInt256, Int256)", new (ulong, long, UInt128, Int128, UInt256, Int256)[]
            {
                (0, long.MinValue, UInt128.Zero, Int128.MinValue, UInt256.Zero, Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 200))),
                (ulong.MaxValue, long.MaxValue, UInt128.MaxValue, Int128.MaxValue, UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)), Int256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200))),
            }));

        yield return Same(
            "Tuple(Float32, Float64, Bool, String)",
            "Tuple(Float32, Float64, Bool, String)",
            name => new TupleColumn<float, double, bool, string>(name, "Tuple(Float32, Float64, Bool, String)", new (float, double, bool, string)[]
            {
                (0f, 0d, false, string.Empty),
                (1.5f, -1.5e100, true, "héllo✓"),
            }));

        yield return Same(
            "Tuple(Enum8, Enum16)",
            "Tuple(Enum8('a' = -1, 'b' = 127), Enum16('x' = -32768, 'y' = 32767))",
            name => new TupleColumn<sbyte, short>(name, "Tuple(Enum8('a' = -1, 'b' = 127), Enum16('x' = -32768, 'y' = 32767))", new (sbyte, short)[]
            {
                (-1, -32768),
                (127, 32767),
            }));

        // DateTime reads back the raw uint epoch seconds and DateTime64 the raw long count at the column scale.
        yield return Same(
            "Tuple(Date, Date32, DateTime, DateTime64(3), UUID)",
            "Tuple(Date, Date32, DateTime, DateTime64(3), UUID)",
            name => new TupleColumn<DateOnly, DateOnly, uint, long, Guid>(name, "Tuple(Date, Date32, DateTime, DateTime64(3), UUID)", new (DateOnly, DateOnly, uint, long, Guid)[]
            {
                (new DateOnly(1970, 1, 1), new DateOnly(1900, 1, 1), 0u, 0L, Guid.Empty),
                (new DateOnly(2149, 6, 6), new DateOnly(2299, 12, 31), (uint)new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero).ToUnixTimeSeconds(), 1_700_000_000_123L, new Guid("00112233-4455-6677-8899-aabbccddeeff")),
            }));

        yield return Same(
            "Tuple(IPv4, IPv6)",
            "Tuple(IPv4, IPv6)",
            name => new TupleColumn<IPAddress, IPAddress>(name, "Tuple(IPv4, IPv6)", new (IPAddress, IPAddress)[]
            {
                (IPAddress.Parse("0.0.0.0"), IPAddress.Parse("::")),
                (IPAddress.Parse("255.255.255.255"), IPAddress.Parse("2001:db8::1")),
            }));

        // Decimal32/64 surface as System.Decimal, Decimal128/256 as ClickHouseTcpDecimal — one tuple spans all four.
        yield return Same(
            "Tuple(Decimal(9, 2), Decimal(18, 4), Decimal(38, 10), Decimal(76, 20))",
            "Tuple(Decimal(9, 2), Decimal(18, 4), Decimal(38, 10), Decimal(76, 20))",
            name => new TupleColumn<decimal, decimal, ClickHouseTcpDecimal, ClickHouseTcpDecimal>(name, "Tuple(Decimal(9, 2), Decimal(18, 4), Decimal(38, 10), Decimal(76, 20))", new (decimal, decimal, ClickHouseTcpDecimal, ClickHouseTcpDecimal)[]
            {
                (0m, 0m, ParseWide("0"), ParseWide("0")),
                (1.23m, 12345.6789m, ParseWide("12345.6789"), ParseWide("1.00000000000000000001")),
            }));

        yield return Same(
            "Tuple(IntervalSecond, IntervalDay)",
            "Tuple(IntervalSecond, IntervalDay)",
            name => new TupleColumn<long, long>(name, "Tuple(IntervalSecond, IntervalDay)", new (long, long)[] { (0L, 0L), (-5L, 7L) }));

        // Experimental element types need their enabling flag on the round-trip.
        yield return Same(
            "Tuple(BFloat16, Float32)",
            "Tuple(BFloat16, Float32)",
            name => new TupleColumn<float, float>(name, "Tuple(BFloat16, Float32)", new (float, float)[] { (0f, 0f), (1f, 1.5f), (-2f, 100f) }),
            BFloat16Settings);

        yield return Same(
            "Tuple(Time, Time64(3))",
            "Tuple(Time, Time64(3))",
            name => new TupleColumn<int, long>(name, "Tuple(Time, Time64(3))", new (int, long)[]
            {
                (0, 0L),
                ((12 * 3600) + (34 * 60) + 56, (((1 * 3600) + (2 * 60) + 3) * 1000L) + 456),
            }),
            TimeSettings);

        // A named tuple: element names ride in the type string; the value is the same ValueTuple as the unnamed form.
        yield return Same(
            "Tuple(a Int32, b String) [named]",
            "Tuple(a Int32, b String)",
            name => new TupleColumn<int, string>(name, "Tuple(a Int32, b String)", new (int, string)[] { (1, "a"), (-5, string.Empty), (int.MaxValue, "héllo✓") }));

        // A field name the server has to quote. The comma inside the backticks would split the argument list, and
        // the type name the client rebuilds is what the insert header carries, so only a real server proves both
        // spellings agree. The server also normalizes a double-quoted name into a backticked one.
        yield return Same(
            "Tuple(`a,b` Int64, c String) [quoted field name]",
            "Tuple(`a,b` Int64, c String)",
            name => new TupleColumn<long, string>(name, "Tuple(`a,b` Int64, c String)", new (long, string)[] { (1L, "x"), (long.MinValue, string.Empty) }));

        // A named tuple whose elements are themselves parametric — the name/type split must survive nesting.
        yield return Same(
            "Tuple(a Array(Int32), b Nullable(String)) [named parametric]",
            "Tuple(a Array(Int32), b Nullable(String))",
            name => new TupleColumn<int[], string>(name, "Tuple(a Array(Int32), b Nullable(String))", new (int[], string)[] { (new[] { 1, 2, 3 }, "x"), (Array.Empty<int>(), null), (new[] { -1 }, string.Empty) }));

        // A nested tuple recurses through the same codec one level down.
        yield return Same(
            "Tuple(Int32, Tuple(String, Float64)) [nested]",
            "Tuple(Int32, Tuple(String, Float64))",
            name => new TupleColumn<int, (string, double)>(name, "Tuple(Int32, Tuple(String, Float64))", new (int, (string, double))[] { (1, ("a", 1.5)), (2, (string.Empty, -1.5e100)) }));

        // Nullable elements, interleaving nulls with present values.
        yield return Same(
            "Tuple(Nullable(Int32), Nullable(String)) [nullable elements]",
            "Tuple(Nullable(Int32), Nullable(String))",
            name => new TupleColumn<int?, string>(name, "Tuple(Nullable(Int32), Nullable(String))", new (int?, string)[] { (1, "a"), (null, null), (int.MinValue, string.Empty) }));

        // An Array element inside a tuple.
        yield return Same(
            "Tuple(Array(UInt32), String) [array element]",
            "Tuple(Array(UInt32), String)",
            name => new TupleColumn<uint[], string>(name, "Tuple(Array(UInt32), String)", new (uint[], string)[] { (new uint[] { 1, 2, 3 }, "a"), (Array.Empty<uint>(), "b") }));

        // A Map element inside a tuple: the map's own offsets and key/value streams have to be emitted from the
        // per-element column the tuple projects, not from the tuple's own column.
        yield return Same(
            "Tuple(Map(String, UInt32), String) [map element]",
            "Tuple(Map(String, UInt32), String)",
            name => new TupleColumn<KeyValuePair<string, uint>[], string>(
                name,
                "Tuple(Map(String, UInt32), String)",
                new (KeyValuePair<string, uint>[], string)[]
                {
                    (Pairs<string, uint>(("a", 1), ("b", 2)), "first"),
                    (Array.Empty<KeyValuePair<string, uint>>(), "second"),
                }));

        // A max-arity (7) tuple mixing fixed-width and variable-width elements.
        yield return Same(
            "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32, String) [arity 7]",
            "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32, String)",
            name => new TupleColumn<byte, sbyte, ushort, short, uint, int, string>(name, "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32, String)", new (byte, sbyte, ushort, short, uint, int, string)[]
            {
                (0, -128, 0, short.MinValue, 0, int.MinValue, string.Empty),
                (255, 127, ushort.MaxValue, short.MaxValue, uint.MaxValue, int.MaxValue, "héllo✓"),
            }));

        // Tuple(): the zero-element tuple is a legal type, but it has none of the layout above — no element
        // streams and no state prefix, just one placeholder byte per row, the way Nothing is serialized. Rows
        // carry no data, so the row count is the only thing the round-trip can get wrong.
        yield return Same(
            "Tuple() [3 rows]",
            "Tuple()",
            name => new ArrayColumn<ValueTuple>(name, "Tuple()", new ValueTuple[3]));

        // The empty tuple as an element of a real one: the per-element codec has to interleave a child that
        // contributes a placeholder byte run between children that contribute values.
        yield return Same(
            "Tuple(Int32, Tuple(), String)",
            "Tuple(Int32, Tuple(), String)",
            name => new TupleColumn<int, ValueTuple, string>(name, "Tuple(Int32, Tuple(), String)", new (int, ValueTuple, string)[]
            {
                (1, default, "a"),
                (int.MinValue, default, string.Empty),
            }));

        // Naming the empty element makes the parser rebuild its node from the glued "y Tuple" token, which is where
        // the empty argument list is easiest to drop.
        yield return Same(
            "Tuple(x Int32, y Tuple())",
            "Tuple(x Int32, y Tuple())",
            name => new TupleColumn<int, ValueTuple>(name, "Tuple(x Int32, y Tuple())", new (int, ValueTuple)[]
            {
                (1, default),
                (-2, default),
            }));

        // Array(Tuple()) flattens to a placeholder byte per element rather than per row, so the offsets are the
        // only thing carrying the shape; an empty row and an uneven run keep them non-trivial.
        yield return Arrays<ValueTuple>("Tuple()", new ValueTuple[2], Array.Empty<ValueTuple>(), new ValueTuple[1]);

        // Array(Tuple(...)): the array flattens its jagged tuple rows into one values stream handed to the tuple
        // codec, exercising the boxed per-element write path; empty rows and an empty column ride along.
        yield return Same(
            "Array(Tuple(Int32, String))",
            "Array(Tuple(Int32, String))",
            name => new ArrayColumn<(int, string)[]>(name, "Array(Tuple(Int32, String))", new[]
            {
                new[] { (1, "a"), (2, "b") },
                Array.Empty<(int, string)>(),
                new[] { (3, "c") },
            }));

        // Map(K, V): byte-identical to Array(Tuple(K, V)) — offsets + a keys stream + a values stream. Each row
        // surfaces as a KeyValuePair<K, V>[] (not a Dictionary), so pair order round-trips; empty-map rows and an
        // all-empty column ride along. Keys within a row are kept unique here because the server rejects duplicate
        // keys on insert — duplicate-key preservation is a wire property proven by the codec unit test instead.
        // Map is, like Array/Tuple, an exception to the "wrap every type in Nullable" rule (the server rejects
        // Nullable(Map(...))), so nullability is composed inside the value as Map(K, Nullable(V)); Map keys are
        // themselves non-nullable in ClickHouse.
        yield return Maps<string, uint>("String", "UInt32", Pairs<string, uint>(("a", 1), ("b", 2)), Array.Empty<KeyValuePair<string, uint>>(), Pairs<string, uint>(("x", uint.MaxValue)));
        yield return Maps<byte, string>("UInt8", "String", Pairs<byte, string>((1, "a"), (2, "héllo✓")), Array.Empty<KeyValuePair<byte, string>>());
        yield return Maps<string, uint>("String", "UInt32", Array.Empty<KeyValuePair<string, uint>>(), Array.Empty<KeyValuePair<string, uint>>()); // every row empty

        // Value composites: Nullable (the Nullable stand-in) and Array inside the value.
        yield return Maps<string, uint?>("String", "Nullable(UInt32)", Pairs<string, uint?>(("a", 1), ("b", null)), Array.Empty<KeyValuePair<string, uint?>>(), Pairs<string, uint?>(("c", null)));
        yield return Maps<string, int[]>("String", "Array(Int32)", Pairs<string, int[]>(("a", new[] { 1, 2, 3 }), ("b", Array.Empty<int>())), Pairs<string, int[]>(("c", new[] { -1 })));
        yield return Maps<string, (int, string)>("String", "Tuple(Int32, String)", Pairs<string, (int, string)>(("a", (1, "x")), ("b", (-5, string.Empty))), Array.Empty<KeyValuePair<string, (int, string)>>());

        // A value that carries its own state prefix: the map has to emit LowCardinality's dictionary prefix for the
        // value stream, which none of the value composites above have (Nullable, Array and Tuple of leaves emit no
        // prefix of their own). Repeated values across rows exercise one dictionary spanning the whole value run.
        yield return Maps<string, string>(
            "String",
            "LowCardinality(String)",
            Pairs<string, string>(("a", "x"), ("b", "y")),
            Array.Empty<KeyValuePair<string, string>>(),
            Pairs<string, string>(("c", "x"), ("d", string.Empty)));

        // Both key and value fixed-width: the cases above pair a variable key with a fixed value or the reverse,
        // never both fixed.
        yield return Maps<byte, byte>("UInt8", "UInt8", Pairs<byte, byte>((1, 10), (2, 20)), Array.Empty<KeyValuePair<byte, byte>>(), Pairs<byte, byte>((3, 30)));

        // A Map whose value is itself a Map — the only path that recurses the map shape through itself. Neither a
        // unit test nor an integration case reached it; Array(Map(...)) covers a different composition.
        yield return Maps<string, KeyValuePair<string, uint>[]>(
            "String",
            "Map(String, UInt32)",
            Pairs<string, KeyValuePair<string, uint>[]>(("outer", Pairs<string, uint>(("x", 1), ("y", 2)))),
            Array.Empty<KeyValuePair<string, KeyValuePair<string, uint>[]>>());

        // Array(Map(...)) recurses the offsets-plus-streams shape one level up through the array codec.
        yield return Arrays<KeyValuePair<string, uint>[]>("Map(String, UInt32)", new[]
        {
            new[] { Pairs<string, uint>(("a", 1)), Pairs<string, uint>(("b", 2), ("c", 3)) },
            Array.Empty<KeyValuePair<string, uint>[]>(),
        });

        // Nested(...) at flatten_nested = 0: a single wire column laid out byte-identically to Array(Tuple(...)),
        // surfaced as a columnar NestedColumn (flat field columns + shared offsets), arity-agnostic. The insert
        // source is the dense NestedColumn itself. flatten_nested = 0 must apply at CREATE so the column is stored
        // as one Nested column rather than flattened into parallel dotted Array columns. Like Array/Tuple/Map, the
        // server rejects Nullable(Nested), so nullability composes inside a field.
        // Rows: [(1,'a'),(2,'b')], [], [(3,'c')].
        yield return Same(
            "Nested(a UInt8, b String)",
            "Nested(a UInt8, b String)",
            name => new NestedColumn(
                name,
                "Nested(a UInt8, b String)",
                new[] { "a", "b" },
                new IColumn[]
                {
                    new ArrayColumn<byte>(name, "UInt8", new byte[] { 1, 2, 3 }),
                    new ArrayColumn<string>(name, "String", new[] { "a", "b", "c" }),
                },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
            NestedSettings);

        // A field name the server has to quote: it arrives backticked in the header, and the insert header has to
        // carry the same spelling back or the server rejects the block.
        yield return Same(
            "Nested(`a b` UInt8) [quoted field name]",
            "Nested(`a b` UInt8)",
            name => new NestedColumn(
                name,
                "Nested(`a b` UInt8)",
                new[] { "a b" },
                new IColumn[] { new ArrayColumn<byte>(name, "UInt8", new byte[] { 1, 2, 3 }) },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
            NestedSettings);

        // Composite fields recurse: a nullable field and an array field. The inner array lengths [1, 2, 0]
        // deliberately differ from the Nested row lengths [2, 0, 1], so the two offsets streams cannot be mixed up.
        yield return Same(
            "Nested(a Nullable(Int32), b Array(String)) [nullable + array fields]",
            "Nested(a Nullable(Int32), b Array(String))",
            name => new NestedColumn(
                name,
                "Nested(a Nullable(Int32), b Array(String))",
                new[] { "a", "b" },
                new IColumn[]
                {
                    new ArrayColumn<int?>(name, "Nullable(Int32)", new int?[] { 1, null, -5 }),
                    new ArrayColumn<string[]>(name, "Array(String)", new[] { new[] { "x" }, new[] { "y", "z" }, Array.Empty<string>() }),
                },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
            NestedSettings);

        // The remaining previously implemented composites as fields: Tuple contributes side-by-side child
        // streams, while Map contributes its own offsets plus key/value streams. Together with the nullable +
        // array case above, every older composite is exercised inside Nested against a real server.
        yield return Same(
            "Nested(a Tuple(UInt8, String), b Map(String, UInt32)) [tuple + map fields]",
            "Nested(a Tuple(UInt8, String), b Map(String, UInt32))",
            name => new NestedColumn(
                name,
                "Nested(a Tuple(UInt8, String), b Map(String, UInt32))",
                new[] { "a", "b" },
                new IColumn[]
                {
                    new ArrayColumn<(byte, string)>(name, "Tuple(UInt8, String)", new[] { ((byte)1, "p"), ((byte)2, "q"), ((byte)3, "r") }),
                    new ArrayColumn<KeyValuePair<string, uint>[]>(name, "Map(String, UInt32)", new[]
                    {
                        Pairs<string, uint>(("x", 1)),
                        Pairs<string, uint>(("y", 2), ("z", uint.MaxValue)),
                        Array.Empty<KeyValuePair<string, uint>>(),
                    }),
                },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
            NestedSettings);

        // flatten_nested = 0 permits arbitrary nesting, including a Nested field inside another Nested. The outer
        // offsets delimit three records; the child Nested has one value per record and its own independent shape.
        yield return Same(
            "Nested(a Nested(b UInt8)) [nested field]",
            "Nested(a Nested(b UInt8))",
            name => new NestedColumn(
                name,
                "Nested(a Nested(b UInt8))",
                new[] { "a" },
                new IColumn[] { ByteNested(name, "b", new byte[] { 1, 2, 3, 4, 5, 6 }, new[] { 0, 2, 2, 3, 6 }) },
                new[] { 0, 1, 3, 4 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
            NestedSettings);

        // The inverse composite directions are legal too. These use dense wire-shaped outer columns because a
        // Nested has deliberately no row-oriented write form: each outer codec forwards the actual NestedColumn
        // child rather than attempting to flatten object[][] values back into named field columns.
        yield return Same(
            "Array(Nested(a UInt8)) [dense nested inner]",
            "Array(Nested(a UInt8))",
            name => new ArrayValueColumn<object[][]>(
                name,
                "Array(Nested(a UInt8))",
                ByteNested(name, "a", new byte[] { 1, 2, 3, 4, 5, 6 }, new[] { 0, 1, 3, 3, 6 }),
                new[] { 0, 2, 2, 4 },
                rowCount: 3,
                pooledOffsets: false),
            NestedSettings);

        yield return Same(
            "Tuple(Nested(a UInt8), String) [dense nested child]",
            "Tuple(Nested(a UInt8), String)",
            name => new TupleColumn<object[][], string>(
                name,
                "Tuple(Nested(a UInt8), String)",
                new IColumn[]
                {
                    ByteNested(name, "a", new byte[] { 1, 2, 3, 4, 5 }, new[] { 0, 2, 2, 5 }),
                    new ArrayColumn<string>(name, "String", new[] { "first", "empty", "last" }),
                },
                fieldNames: null,
                ownsChildren: false),
            NestedSettings);

        yield return Same(
            "Map(String, Nested(a UInt8)) [dense nested value]",
            "Map(String, Nested(a UInt8))",
            name => new MapColumn<string, object[][]>(
                name,
                "Map(String, Nested(a UInt8))",
                new ArrayColumn<string>(name, "String", new[] { "w", "x", "y", "z" }),
                ByteNested(name, "a", new byte[] { 1, 2, 3, 4, 5, 6 }, new[] { 0, 1, 3, 3, 6 }),
                new[] { 0, 2, 2, 4 },
                rowCount: 3,
                pooledOffsets: false),
            NestedSettings);

        // Map drives its key and value codecs independently, so cover Nested in the key position too. The map row
        // lengths [1, 2, 0] deliberately differ from the Nested key lengths [2, 0, 3].
        yield return Same(
            "Map(Nested(a UInt8), UInt32) [dense nested key]",
            "Map(Nested(a UInt8), UInt32)",
            name => new MapColumn<object[][], uint>(
                name,
                "Map(Nested(a UInt8), UInt32)",
                ByteNested(name, "a", new byte[] { 1, 2, 3, 4, 5 }, new[] { 0, 2, 2, 5 }),
                new ArrayColumn<uint>(name, "UInt32", new uint[] { 7, 8, uint.MaxValue }),
                new[] { 0, 1, 3, 3 },
                rowCount: 3,
                pooledOffsets: false),
            NestedSettings);

        // Eight fields: proves the dedicated codec is not bound by the tuple's 7-element cap. Rows of 2 and 1 elements.
        yield return Same(
            "Nested(8 fields) [uncapped]",
            "Nested(a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8, h UInt8)",
            name =>
            {
                var names = new[] { "a", "b", "c", "d", "e", "f", "g", "h" };
                var fields = new IColumn[8];
                for (int i = 0; i < 8; i++)
                {
                    fields[i] = new ArrayColumn<byte>(name, "UInt8", new byte[] { (byte)i, (byte)(i + 10), (byte)(i + 20) });
                }

                return new NestedColumn(
                    name,
                    "Nested(a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8, h UInt8)",
                    names,
                    fields,
                    new[] { 0, 2, 3 },
                    rowCount: 2,
                    pooledOffsets: false,
                    ownsFields: false);
            },
            NestedSettings);

        // LowCardinality(T): the inner values are replaced by a block-local dictionary plus per-row keys. Values
        // repeat (and include the inner default) so the dedup and the reserved slot-0 default are both exercised.
        // Like Array/Tuple/Map/Nested, LowCardinality is an exception to the "wrap every type in Nullable" rule —
        // the server rejects Nullable(LowCardinality(T)); nullability composes the other way as
        // LowCardinality(Nullable(T)), covered by its own cases further below. A numeric inner is
        // "suspicious" and needs allow_suspicious_low_cardinality_types; String/FixedString are allowed by default.
        yield return Same(
            "LowCardinality(String)",
            "LowCardinality(String)",
            name => new ArrayColumn<string>(name, "LowCardinality(String)", new[] { "a", "b", "a", "c", "b", string.Empty }));

        yield return Same(
            "LowCardinality(UInt32)",
            "LowCardinality(UInt32)",
            name => PrimitiveColumn<uint>.FromValues(name, "LowCardinality(UInt32)", new uint[] { 7, 7, 42, 7, 42, 0 }),
            LowCardinalitySettings);

        yield return Same(
            "LowCardinality(FixedString(4))",
            "LowCardinality(FixedString(4))",
            name => new ArrayColumn<byte[]>(name, "LowCardinality(FixedString(4))", new[]
            {
                new byte[] { 1, 2, 3, 4 },
                new byte[] { 1, 2, 3, 4 },
                new byte[] { 0xFF, 0, 0xFF, 0 },
            }));

        // The dictionary is a bare column of the inner type, so its element width is the inner's and not the key
        // stream's. The cases above are all four bytes wide; these are one, eight and sixteen.
        yield return Same(
            "LowCardinality(UInt8)",
            "LowCardinality(UInt8)",
            name => PrimitiveColumn<byte>.FromValues(name, "LowCardinality(UInt8)", new byte[] { 3, 3, 0, 255, 3 }),
            LowCardinalitySettings);

        yield return Same(
            "LowCardinality(Int64)",
            "LowCardinality(Int64)",
            name => PrimitiveColumn<long>.FromValues(name, "LowCardinality(Int64)", new[] { long.MinValue, 0L, long.MaxValue, 0L }),
            LowCardinalitySettings);

        yield return Same(
            "LowCardinality(UUID)",
            "LowCardinality(UUID)",
            name => new ArrayColumn<Guid>(name, "LowCardinality(UUID)", new[]
            {
                Guid.Empty,
                new Guid("00112233-4455-6677-8899-aabbccddeeff"),
                Guid.Empty,
            }),
            LowCardinalitySettings);

        // Array(LowCardinality(String)) flattens its jagged rows into one values stream handed to the
        // low-cardinality codec; empty rows and repeated values ride along.
        yield return Arrays("LowCardinality(String)", new[] { "a", "b" }, Array.Empty<string>(), new[] { "a", "a", "c" });

        // Two levels of offsets over a dictionary-bearing leaf, where the flattening view is built over a view.
        // The deep-nesting ladder uses prefix-free leaves only.
        yield return Same(
            "Array(Array(LowCardinality(String)))",
            "Array(Array(LowCardinality(String)))",
            name => new ArrayColumn<string[][]>(name, "Array(Array(LowCardinality(String)))", new[]
            {
                new[] { new[] { "a", "b" }, Array.Empty<string>(), new[] { "a" } },
                Array.Empty<string[]>(),
                new[] { new[] { "c", "a", "c" } },
            }));

        yield return Same(
            "Array(Array(LowCardinality(Nullable(String))))",
            "Array(Array(LowCardinality(Nullable(String))))",
            name => new ArrayColumn<string[][]>(name, "Array(Array(LowCardinality(Nullable(String))))", new[]
            {
                new[] { new[] { "a", null }, Array.Empty<string>() },
                new[] { new string[] { null, null }, new[] { "a", "b" } },
            }));

        // A dictionary past 255 entries forces the client to widen the key stream from UInt8 to UInt16
        // (SelectKeyWidthCode switches on dictSize < byte.MaxValue). Unit tests assert the client picks that
        // width; no case had more than three distinct values, so nothing proved the server accepts a
        // client-written wide key stream.
        yield return Same(
            "LowCardinality(String) [wide keys]",
            "LowCardinality(String)",
            name => new ArrayColumn<string>(name, "LowCardinality(String)", Enumerable.Range(0, 300).Select(i => $"v{i}").ToArray()));

        // Array(LowCardinality(Nullable(String))) puts the reserved key-0-is-NULL dictionary underneath the array
        // offsets — exactly where a reserved-slot off-by-one would surface. Neither half-case reaches it.
        yield return Arrays("LowCardinality(Nullable(String))", new[] { "a", null }, Array.Empty<string>(), new[] { "a", null, "c" });

        // LowCardinality(Nullable(T)): nullability is expressed by a reserved dictionary slot (key 0 = NULL), not a
        // null-map — the dictionary is still bare T. This is the nullable coverage for LowCardinality (the server
        // rejects Nullable(LowCardinality(T))). A present value equal to the inner default (empty string, 0) rides
        // alongside NULL to prove the two are distinct on the wire.
        yield return Same(
            "LowCardinality(Nullable(String))",
            "LowCardinality(Nullable(String))",
            name => new ArrayColumn<string>(name, "LowCardinality(Nullable(String))", new[] { "a", null, string.Empty, "b", "a", null }));

        yield return Same(
            "LowCardinality(Nullable(UInt32))",
            "LowCardinality(Nullable(UInt32))",
            name => new ArrayColumn<uint?>(name, "LowCardinality(Nullable(UInt32))", new uint?[] { 7, null, 0, 7, 42, null }),
            LowCardinalitySettings);

        yield return Same(
            "LowCardinality(Nullable(FixedString(4)))",
            "LowCardinality(Nullable(FixedString(4)))",
            name => new ArrayColumn<byte[]>(name, "LowCardinality(Nullable(FixedString(4)))", new[]
            {
                new byte[] { 1, 2, 3, 4 },
                null,
                new byte[] { 1, 2, 3, 4 },
                new byte[] { 0xFF, 0, 0xFF, 0 },
            }));

        // Variant(...): a discriminated union — each row is a value of one alternative or NULL. The ergonomic
        // insert source is a flat IColumn<object>; each value's runtime CLR type picks its alternative, and null
        // marks a NULL row. Like Array/Tuple/Map/Nested, the server rejects Nullable(Variant(...)) — NULL is the
        // discriminator's job — so there is no Nullable(...) case; NULL rides inside the variant instead. The
        // alternatives arrive server-canonicalized (sorted by name), so the type strings here are already in
        // discriminator order. A present value equal to a type's default (0, empty string) rides alongside NULL to
        // prove the two are distinct.
        yield return Same(
            "Variant(String, UInt64)",
            "Variant(String, UInt64)",
            name => new ArrayColumn<object>(name, "Variant(String, UInt64)", new object[] { 42UL, "hi", null, 0UL, string.Empty, null }),
            VariantSettings);

        // Three alternatives, exercising more than one non-null discriminator interleaved with NULL.
        yield return Same(
            "Variant(Bool, Int32, String)",
            "Variant(Bool, Int32, String)",
            name => new ArrayColumn<object>(name, "Variant(Bool, Int32, String)", new object[] { true, 42, "x", null, false, -1 }),
            VariantSettings);

        // Two alternatives surfacing one CLR type, settled by the value. IPv4 and IPv6 are both IPAddress, so the
        // runtime type alone picks neither; the address family does, and the server's own view of which
        // alternative each row took is what the read-back compares. A String rides along to keep the rest of the
        // resolution honest: it collides with nothing and must still take its own discriminator.
        yield return Same(
            "Variant(IPv4, IPv6, String)",
            "Variant(IPv4, IPv6, String)",
            name => new ArrayColumn<object>(name, "Variant(IPv4, IPv6, String)", new object[]
            {
                IPAddress.Parse("10.0.0.1"),
                IPAddress.Parse("2001:db8::1"),
                "not an address",
                null,
                IPAddress.Parse("0.0.0.0"),
                IPAddress.Parse("::"),
            }),
            VariantSettings);

        // A composite alternative: an Array as one of the variant types. A row selecting it carries the inner
        // element array (ulong[]); the other rows carry a String or NULL.
        yield return Same(
            "Variant(Array(UInt64), String)",
            "Variant(Array(UInt64), String)",
            name => new ArrayColumn<object>(name, "Variant(Array(UInt64), String)", new object[] { "hello", new ulong[] { 1, 2, 3 }, null, Array.Empty<ulong>() }),
            VariantSettings);

        // A Tuple alternative: the variant projects the rows that selected it into a column of its own, which the
        // tuple codec then distributes across its per-element children. A row selecting it carries the ValueTuple.
        yield return Same(
            "Variant(String, Tuple(UInt8, String))",
            "Variant(String, Tuple(UInt8, String))",
            name => new ArrayColumn<object>(
                name,
                "Variant(String, Tuple(UInt8, String))",
                new object[] { "hi", ((byte)1, "x"), null, ((byte)0, string.Empty) }),
            VariantSettings);

        // The same shape, but with an element inside the tuple that carries its own state prefix, so LowCardinality's
        // dictionary prefix has to travel two composites down to reach the wire. Losing it desyncs the block
        // mid-stream instead of failing cleanly.
        yield return Same(
            "Variant(String, Tuple(LowCardinality(String), UInt8))",
            "Variant(String, Tuple(LowCardinality(String), UInt8))",
            name => new ArrayColumn<object>(
                name,
                "Variant(String, Tuple(LowCardinality(String), UInt8))",
                new object[] { "hi", ("lc", (byte)7), null, (string.Empty, (byte)0) }),
            VariantSettings);

        // A Map alternative, whose value in turn carries a state prefix. The map is the composite whose own write
        // state is most easily bypassed — it reaches its key and value codecs through a shape object rather than
        // directly — so this pins that a map nested in a variant still emits its offsets, keys, and the value
        // stream's dictionary prefix.
        yield return Same(
            "Variant(Map(String, LowCardinality(String)), UInt64)",
            "Variant(Map(String, LowCardinality(String)), UInt64)",
            name => new ArrayColumn<object>(
                name,
                "Variant(Map(String, LowCardinality(String)), UInt64)",
                new object[]
                {
                    1UL,
                    Pairs<string, string>(("a", "x"), ("b", "x")),
                    null,
                    Array.Empty<KeyValuePair<string, string>>(),
                }),
            VariantSettings);

        // An alternative that carries a state prefix but is selected by no row in the block. The variant's
        // alternatives are fixed by its type, not by the data, so every alternative's prefix belongs on the wire
        // whether or not any row picks it — here LowCardinality's dictionary prefix, with every row a UInt64 or
        // NULL. Omitting it desyncs the block, so the server rejects the insert rather than storing bad data.
        yield return Same(
            "Variant(LowCardinality(String), UInt64) [absent alternative]",
            "Variant(LowCardinality(String), UInt64)",
            name => new ArrayColumn<object>(
                name,
                "Variant(LowCardinality(String), UInt64)",
                new object[] { 1UL, null, 2UL, null }),
            VariantSettings);

        // The same, one level deeper: the absent alternative is a Tuple whose own element carries the prefix, so the
        // tuple's children have to be walked over an empty slice for that prefix to be emitted at all.
        yield return Same(
            "Variant(Tuple(LowCardinality(String), UInt8), UInt64) [absent alternative]",
            "Variant(Tuple(LowCardinality(String), UInt8), UInt64)",
            name => new ArrayColumn<object>(
                name,
                "Variant(Tuple(LowCardinality(String), UInt8), UInt64)",
                new object[] { 7UL, null, 9UL }),
            VariantSettings);

        // Array(Variant(...)) composition: the array flattens its elements into one Variant value stream, so each
        // element is a variant value (or NULL) and empty rows ride along.
        yield return Arrays(
            "Variant(String, UInt64)",
            VariantSettings,
            new object[] { 1UL, "a" },
            Array.Empty<object>(),
            new object[] { "b", 2UL, null });

        // Dynamic: a column whose per-row value type is discovered at runtime. The ergonomic insert source is a
        // flat IColumn<object>; each value's CLR type is inferred to a ClickHouse type, and null marks a NULL row.
        // Like Array/Tuple/Map/Nested/Variant, the server rejects Nullable(Dynamic) — NULL rides the discriminator
        // — so there is no Nullable(...) case; NULL is intrinsic. A present value equal to a type's default (0,
        // empty string) rides alongside NULL to prove the two are distinct.
        yield return Same(
            "Dynamic [scalars + null]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[] { 42UL, "hi", null, 0UL, string.Empty, null }),
            DynamicSettings);

        // Dynamic(max_types=N) is the one shape where the server reshapes the dynamic structure: types beyond N go
        // to the shared/overflow bucket. Create_MaxTypesArgument_IsAccepted only checks the TypeName string, and no
        // case used a parameterized Dynamic, so the client's read of that reshaped structure was untested.
        yield return Same(
            "Dynamic(max_types=2) [overflow bucket]",
            "Dynamic(max_types=2)",
            name => new ArrayColumn<object>(name, "Dynamic(max_types=2)", new object[] { 42UL, "hi", 1.5, true, null }),
            DynamicSettings);

        // A broader scalar mix, exercising the inference table across several runtime types in one column.
        yield return Same(
            "Dynamic [mixed scalar types]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[]
            {
                true, 42, 3.5d, 1.5f, (byte)7, new Guid("00112233-4455-6677-8899-aabbccddeeff"), null, "text",
            }),
            DynamicSettings);

        // Composite values inside a Dynamic: an array, a map, and a tuple value, inferred recursively.
        yield return Same(
            "Dynamic [array value]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[] { new ulong[] { 1, 2, 3 }, "x", null, Array.Empty<ulong>() }),
            DynamicSettings);

        yield return Same(
            "Dynamic [map value]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[] { Pairs<string, uint>(("a", 1), ("b", 2)), 5, null }),
            DynamicSettings);

        // A map whose key and value types only the pair values can settle: an IPAddress picks IPv4 or IPv6 by its
        // address family, and a ClickHouseTcpDecimal carries its own scale. Inferring from the CLR type alone cannot
        // reach either, so this covers the Map slots the Array and Tuple cases above already cover.
        yield return Same(
            "Dynamic [map value, value-disambiguated key and value]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[]
            {
                Pairs<IPAddress, ClickHouseTcpDecimal>((IPAddress.Parse("10.0.0.1"), ParseWide("12345.6789")), (IPAddress.Parse("10.0.0.2"), ParseWide("-1.0002"))),
                null,
            }),
            DynamicSettings);

        yield return Same(
            "Dynamic [tuple value]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[] { (1, "a"), "plain", null }),
            DynamicSettings);

        // One Dynamic column holding a value of (basically) every supported type — each row's runtime CLR type is
        // inferred to a distinct ClickHouse type, so the block's type list spans them all at once. Uses the
        // canonical read-back CLR types (e.g. ClickHouseTcpDecimal) so insert equals read-back; the
        // DateTimeOffset/DateTime/decimal inputs, whose read-back type differs, are covered separately below.
        yield return Same(
            "Dynamic [every type + composites]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[]
            {
                (byte)7, (sbyte)-5, (ushort)300, (short)-300, 100000u, -100000, 42UL, -42L,
                UInt128.MaxValue, Int128.MinValue,
                UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)),
                Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 200)),
                1.5f, 3.5d, true, "héllo✓", new Guid("00112233-4455-6677-8899-aabbccddeeff"),
                new DateOnly(2024, 1, 15), IPAddress.Parse("192.168.1.1"), IPAddress.Parse("2001:db8::1"),
                new ClickHouseTcpDecimal(System.Numerics.BigInteger.Parse("1234567890123456789012345"), 5),
                new ulong[] { 1, 2, 3 },
                Pairs<string, uint>(("k", 9), ("m", 10)),
                (1, "t"),
                null,
            }),
            DynamicSettings);

        // Inputs whose inferred ClickHouse type reads back as a different (canonical) CLR type: a DateTimeOffset
        // and a DateTime infer to DateTime64(9) (read back as the raw long nanosecond count), and a System.Decimal
        // infers to Decimal128 (read back as ClickHouseTcpDecimal, equal by value).
        yield return new InsertRoundTripCase(
            "Dynamic [datetime + decimal inference]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[]
            {
                new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)),
                new DateTime(1988, 8, 28, 11, 22, 33, DateTimeKind.Utc),
                12345.6789m,
                null,
            }),
            name => new ArrayColumn<object>(name, "Dynamic", new object[]
            {
                (new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)).UtcDateTime.Ticks - DateTime.UnixEpoch.Ticks) * 100,
                (new DateTime(1988, 8, 28, 11, 22, 33, DateTimeKind.Utc).Ticks - DateTime.UnixEpoch.Ticks) * 100,
                new ClickHouseTcpDecimal(new System.Numerics.BigInteger(123456789), 4),
                null,
            }),
            DynamicSettings);

        // Array(Dynamic): Dynamic nested inside a composite. The array flattens its element values into one Dynamic
        // stream, so the Dynamic type list (state prefix) precedes the array offsets on the wire; empty rows and a
        // NULL element ride along.
        yield return Arrays(
            "Dynamic",
            DynamicSettings,
            new object[] { 1UL, "a" },
            Array.Empty<object>(),
            new object[] { "b", 2UL, null });

        // Array(Dynamic) with every row empty: the inner Dynamic has no rows while the block has, so the array
        // still writes and reads the Dynamic prefix, and the zero-row body path runs with the prefix consumed
        // rather than skipped. The only all-empty case otherwise is Array(UInt32), a leaf that carries no prefix.
        yield return Same(
            "Array(Dynamic) [every row empty]",
            "Array(Dynamic)",
            name => new ArrayColumn<object[]>(name, "Array(Dynamic)", new[] { Array.Empty<object>(), Array.Empty<object>() }),
            DynamicSettings);

        // Tuple(Dynamic, String): a Dynamic element inside a tuple. Each element position is its own child column,
        // so the Dynamic child's type list (state prefix) is written from its own projected values.
        yield return Same(
            "Tuple(Dynamic, String)",
            "Tuple(Dynamic, String)",
            name => new TupleColumn<object, string>(name, "Tuple(Dynamic, String)", new (object, string)[]
            {
                (42UL, "a"), ("x", "b"), (null, "c"),
            }),
            DynamicSettings);

        // The key column has its own state, built separately from the value's: MapShape.WriteStatePrefix writes
        // the key's prefix first. Every other map case has a prefix-free key, so nothing reached that order.
        yield return Maps<string, byte>(
            "LowCardinality(String)",
            "UInt8",
            Pairs<string, byte>(("a", 1), ("b", 2)),
            Array.Empty<KeyValuePair<string, byte>>(),
            Pairs<string, byte>(("a", 3)));

        // A composite key, where the key column is itself two child columns.
        yield return Maps<(int, int), int>(
            "Tuple(Int32, Int32)",
            "Int32",
            Pairs<(int, int), int>(((1, 2), 3), ((-1, 0), 4)),
            Array.Empty<KeyValuePair<(int, int), int>>());

        // Map(String, Dynamic): a Dynamic value column inside a map, flattened like Array(Tuple(String, Dynamic)).
        yield return Maps<string, object>("String", "Dynamic", DynamicSettings,
            Pairs<string, object>(("a", 1UL), ("b", "x")),
            Array.Empty<KeyValuePair<string, object>>(),
            Pairs<string, object>(("c", null)));

        // Nested(a Dynamic, b String): a Dynamic field inside a Nested column.
        yield return Same(
            "Nested(a Dynamic, b String)",
            "Nested(a Dynamic, b String)",
            name => new NestedColumn(
                name,
                "Nested(a Dynamic, b String)",
                new[] { "a", "b" },
                new IColumn[]
                {
                    new ArrayColumn<object>(name, "Dynamic", new object[] { 1UL, "x", 3UL }),
                    new ArrayColumn<string>(name, "String", new[] { "a", "b", "c" }),
                },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
            MergeSettings(NestedSettings, DynamicSettings));

        // Two Dynamic elements in one tuple, each with its own runtime type list.
        yield return Same(
            "Tuple(Dynamic, Dynamic)",
            "Tuple(Dynamic, Dynamic)",
            name => new TupleColumn<object, object>(name, "Tuple(Dynamic, Dynamic)", new (object, object)[]
            {
                (1UL, "a"), ("x", 2), (null, null),
            }),
            DynamicSettings);

        // Dynamic two composite levels deep: Array(Tuple(Dynamic, String)) — the array flattens its tuple rows
        // into the tuple codec, which projects the Dynamic element's own values into its type list.
        yield return Same(
            "Array(Tuple(Dynamic, String))",
            "Array(Tuple(Dynamic, String))",
            name => new ArrayColumn<(object, string)[]>(name, "Array(Tuple(Dynamic, String))", new[]
            {
                new (object, string)[] { (1UL, "a"), ("b", "c") },
                Array.Empty<(object, string)>(),
                new (object, string)[] { (null, "d") },
            }),
            DynamicSettings);

        // JSON in its String serialization: each value is its compact JSON text, so the column is an
        // IColumn<string>. The server parses the text on insert and re-serializes it on read, so a case whose
        // values are not already canonical reads back normalized — see the normalization case below. These values
        // are canonical (keys ordinally sorted, no whitespace, no JSON null), so insert equals read-back.
        yield return JsonTexts("{}", "{\"a\":1}", "{\"a\":1,\"b\":\"hi\"}", "{\"n\":{\"deep\":[1,2,3]}}");

        // Every value empty: the whole column carries no path at all.
        yield return JsonTexts("{}", "{}");

        // Typed paths are declared in the type string and always materialize, so a value omitting one reads back
        // with that path at its type's default ("" for String) rather than absent — hence the separate expected
        // builder. The undeclared path rides along as a dynamic path.
        yield return new InsertRoundTripCase(
            "JSON(a UInt32, b String) [typed paths default when absent]",
            "JSON(a UInt32, b String)",
            name => new ArrayColumn<string>(name, "JSON(a UInt32, b String)", new[]
            {
                "{\"a\":1,\"x\":\"p\"}",
                "{\"a\":2,\"b\":\"q\"}",
            }),
            name => new ArrayColumn<string>(name, "JSON(a UInt32, b String)", new[]
            {
                "{\"a\":1,\"b\":\"\",\"x\":\"p\"}",
                "{\"a\":2,\"b\":\"q\"}",
            }),
            JsonSettings);

        // A typed path the server has to quote. The paren inside the backticks would end the argument list early,
        // and the read fails on the header before a row decodes, so this case only ever fails at the header.
        yield return new InsertRoundTripCase(
            "JSON(`a(b` Int64) [quoted typed path]",
            "JSON(`a(b` Int64)",
            name => new ArrayColumn<string>(name, "JSON(`a(b` Int64)", new[] { "{\"a(b\":5}", "{}" }),
            name => new ArrayColumn<string>(name, "JSON(`a(b` Int64)", new[] { "{\"a(b\":5}", "{\"a(b\":0}" }),
            JsonSettings);

        // The server parses a JSON value rather than storing the text, so what comes back is its own rendering:
        // keys ordinally sorted (so "A" precedes "a"), whitespace dropped, numbers re-rendered canonically
        // (1.0 -> 1, 1e3 -> 1000, -0 -> 0), a dotted key read as nesting, and a JSON null or an empty object
        // contributing no path at all. Nothing about that is the client's doing, but a client that mangled the text
        // would show up here.
        yield return new InsertRoundTripCase(
            "JSON [server normalizes the text]",
            "JSON",
            name => new ArrayColumn<string>(name, "JSON", new[]
            {
                "{  \"b\" : 1 ,  \"a\" : 2 }",
                "{\"a\":1.0,\"b\":1e3,\"c\":-0}",
                "{\"a\":null,\"b\":{}}",
                "{\"a.b\":1}",
                "{\"a\":\"x\",\"A\":\"y\"}",
            }),
            name => new ArrayColumn<string>(name, "JSON", new[]
            {
                "{\"a\":2,\"b\":1}",
                "{\"a\":1,\"b\":1000,\"c\":0}",
                "{}",
                "{\"a\":{\"b\":1}}",
                "{\"A\":\"y\",\"a\":\"x\"}",
            }),
            JsonSettings);

        // Nullable(JSON): unlike Array/Tuple/Map/Variant, the server does accept it. A NULL row still occupies the
        // values stream, and those bytes are parsed like any other, so the placeholder written there must be
        // parseable JSON ("{}") — an empty string is rejected outright. The all-null case is the one that proves it,
        // since it is placeholder and nothing else.
        yield return NullableJsonTexts("{\"a\":1}", null, "{}", "{\"b\":\"hi\"}", null);
        yield return NullableJsonTexts(null, null);

        // Array(JSON): JSON carries a state prefix, so the array has to emit the version once ahead of its offsets
        // rather than treat JSON as a flat leaf. Empty rows and an all-empty column ride along.
        yield return Arrays("JSON", JsonSettings, new[] { "{\"a\":1}", "{}" }, Array.Empty<string>(), new[] { "{\"b\":\"hi\"}" });

        // Array(JSON) with every row empty: the same shape as the all-empty Array(Dynamic) case, for the codec
        // whose prefix is a version word rather than a type list.
        yield return Same(
            "Array(JSON) [every row empty]",
            "Array(JSON)",
            name => new ArrayColumn<string[]>(name, "Array(JSON)", new[] { Array.Empty<string>(), Array.Empty<string>() }),
            JsonSettings);

        // Tuple(JSON, String): each element is its own child column, so the JSON version is written from the
        // element the tuple projects.
        yield return Same(
            "Tuple(JSON, String)",
            "Tuple(JSON, String)",
            name => new TupleColumn<string, string>(name, "Tuple(JSON, String)", new (string, string)[]
            {
                ("{\"a\":1}", "x"),
                ("{}", string.Empty),
            }),
            JsonSettings);

        // Map(String, JSON): the JSON value stream's version prefix has to travel through the map's shape object,
        // the same path LowCardinality's dictionary prefix takes.
        yield return Maps<string, string>(
            "String",
            "JSON",
            JsonSettings,
            Pairs<string, string>(("a", "{\"a\":1}"), ("b", "{}")),
            Array.Empty<KeyValuePair<string, string>>(),
            Pairs<string, string>(("c", "{\"b\":\"hi\"}")));

        // Variant(JSON, UInt64): JSON as a variant alternative. Variant is the trickiest prefix carrier — it writes
        // every alternative's prefix from that alternative's own row slice, zero-length ones included — so this is
        // where a JSON version word is most easily lost or duplicated. The alternatives arrive canonicalized and
        // "JSON" sorts before "UInt64", so JSON is discriminator 0. UInt64 is chosen as the second alternative on
        // purpose: pairing JSON with String would make the two indistinguishable to the ergonomic write path, which
        // picks an alternative by runtime CLR type and would send every string to the JSON arm.
        yield return Same(
            "Variant(JSON, UInt64)",
            "Variant(JSON, UInt64)",
            name => new ArrayColumn<object>(name, "Variant(JSON, UInt64)", new object[]
            {
                "{\"a\":1}", 42UL, null, "{}", 0UL,
            }),
            MergeSettings(VariantSettings, JsonSettings));

        // Nested(a JSON, b String): a JSON field inside a Nested column.
        yield return Same(
            "Nested(a JSON, b String)",
            "Nested(a JSON, b String)",
            name => new NestedColumn(
                name,
                "Nested(a JSON, b String)",
                new[] { "a", "b" },
                new IColumn[]
                {
                    new ArrayColumn<string>(name, "JSON", new[] { "{\"a\":1}", "{}", "{\"b\":\"hi\"}" }),
                    new ArrayColumn<string>(name, "String", new[] { "a", "b", "c" }),
                },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
            MergeSettings(NestedSettings, JsonSettings));

        // The geo aliases name structures already covered above — Point is Tuple(Float64, Float64) and the rest
        // are arrays over it — so what these cases prove is not the layout but that the alias resolves to it: the
        // server puts "Point"/"Ring"/… in the column header, and the client has to accept that name in both
        // directions. Nullable(Point) is covered below, behind enable_nullable_tuple_type; the array-shaped
        // aliases take no Nullable case, the server rejecting a Nullable array outright.
        // Supplied as a flat column of tuples rather than a dense TupleColumn: that column's convenience
        // constructor derives its children's types by re-parsing its own type name, which an alias is not. The
        // dense shape is still covered — the read comes back as one, and the dense-readback case re-inserts it.
        yield return Same(
            "Point",
            "Point",
            name => new ArrayColumn<(double, double)>(name, "Point", new[] { (0d, 0d), (1.5d, -2.5d), (double.MinValue, double.MaxValue) }));

        // Ring and LineString share a structure and differ only in name, so each needs its own case — a
        // registration that mapped one of them to the wrong codec would still pass the other's.
        yield return Same(
            "Ring",
            "Ring",
            name => new ArrayColumn<(double, double)[]>(name, "Ring", new[]
            {
                new[] { (0d, 0d), (1d, 0d), (1d, 1d), (0d, 0d) },
                Array.Empty<(double, double)>(),
            }));

        yield return Same(
            "LineString",
            "LineString",
            name => new ArrayColumn<(double, double)[]>(name, "LineString", new[]
            {
                new[] { (0d, 0d), (1.5d, 2.5d) },
                Array.Empty<(double, double)>(),
            }));

        // Polygon and MultiLineString likewise share a structure (an array of the array-of-Point aliases) and
        // differ only in which alias sits underneath.
        yield return Same(
            "Polygon",
            "Polygon",
            name => new ArrayColumn<(double, double)[][]>(name, "Polygon", new[]
            {
                new[] { new[] { (0d, 0d), (2d, 0d), (2d, 2d), (0d, 0d) }, new[] { (0.5d, 0.5d), (1d, 0.5d), (1d, 1d), (0.5d, 0.5d) } },
                Array.Empty<(double, double)[]>(),
            }));

        yield return Same(
            "MultiLineString",
            "MultiLineString",
            name => new ArrayColumn<(double, double)[][]>(name, "MultiLineString", new[]
            {
                new[] { new[] { (0d, 0d), (1d, 1d) }, new[] { (2d, 2d), (3d, 3d) } },
                Array.Empty<(double, double)[]>(),
            }));

        yield return Same(
            "MultiPolygon",
            "MultiPolygon",
            name => new ArrayColumn<(double, double)[][][]>(name, "MultiPolygon", new[]
            {
                new[] { new[] { new[] { (0d, 0d), (2d, 0d), (2d, 2d), (0d, 0d) } } },
                Array.Empty<(double, double)[][]>(),
            }));

        // The alias inside a composite: the registry has to reach it while resolving a child node, not only as a
        // whole column type.
        yield return Arrays("Point", new[] { (0d, 0d), (1d, 2d) }, Array.Empty<(double, double)>());

        // Nullable over an alias for a tuple: the null map sits outside, and the inner tuple is still resolved
        // from the alias name. The bare Nullable(Tuple(...)) case is in the nullable section above; this one adds
        // the alias.
        yield return Same(
            "Nullable(Point)",
            "Nullable(Point)",
            name => new ArrayColumn<(double, double)?>(name, "Nullable(Point)", new (double, double)?[]
            {
                (1.5d, -2.5d),
                null,
                (0d, 0d),
            }),
            NullableTupleSettings);

        // Geometry is a Variant over the six aliases, and the column header carries only "Geometry", so the client
        // expands it and picks the discriminator order itself. A row against each of the six discriminators, plus a
        // NULL on 255, catches an order that disagrees with the server for Point and MultiPolygon, whose layouts
        // are unique. It cannot catch a transposition within the two structurally identical pairs: the block is
        // byte-identical, and the read applies the same order the write used, so the value survives either way.
        // GeometryIntegrationTests asks the server for its own name for each row instead.
        // The insert source is the dense column: a gathered row of one of those pairs would name neither
        // alternative, so only explicit discriminators can express this column.
        if (TcpServerFeatures.Has(TcpFeature.Geometry))
        {
            yield return Same("Geometry", "Geometry", name => BuildGeometryColumn(name));
        }

        if (TcpServerFeatures.Has(TcpFeature.QBit))
        {
            yield return Same(
                "QBit(Float32, 4)",
                "QBit(Float32, 4)",
                name => new ArrayColumn<float[]>(name, "QBit(Float32, 4)", new[]
                {
                    new[] { 1f, 2f, 3f, 4f },
                    new[] { 0f, -0f, float.MaxValue, float.MinValue },
                    new[] { float.Epsilon, float.PositiveInfinity, float.NegativeInfinity, float.NaN },
                }));

            yield return Same(
                "QBit(Float64, 3)",
                "QBit(Float64, 3)",
                name => new ArrayColumn<double[]>(name, "QBit(Float64, 3)", new[]
                {
                    new[] { 1d, -2d, 3.5d },
                    new[] { double.MaxValue, double.MinValue, double.Epsilon },
                    new[] { 0d, -0d, double.NaN },
                }));

            yield return Same(
                "QBit(Float32, 17)",
                "QBit(Float32, 17)",
                name => new ArrayColumn<float[]>(name, "QBit(Float32, 17)", new[]
                {
                    Ramp(17, i => (i * 0.5f) - 4f),
                    Ramp(17, i => i % 2 == 0 ? float.MaxValue : float.MinValue),
                }));

            // An embedding-shaped width: 768 is the dimension of a common sentence embedding, and the widest the
            // suite reaches otherwise is 17. Every row spans 96 bytes per plane, so QBitLayout's stride arithmetic
            // and the dense plane copy are exercised at a size where an off-by-one row stride cannot look right.
            yield return Same(
                "QBit(Float32, 768)",
                "QBit(Float32, 768)",
                name => new ArrayColumn<float[]>(name, "QBit(Float32, 768)", new[]
                {
                    Ramp(768, i => (i * 0.25f) - 96f),
                    Ramp(768, i => i % 3 == 0 ? float.MaxValue : (i % 3 == 1 ? -0f : float.NaN)),
                }));

            yield return Same(
                "QBit(Float64, 17)",
                "QBit(Float64, 17)",
                name => new ArrayColumn<double[]>(name, "QBit(Float64, 17)", new[]
                {
                    Ramp(17, i => (i * 0.25d) - 2d),
                    Ramp(17, i => i % 2 == 0 ? double.MaxValue : double.MinValue),
                }));

            yield return new InsertRoundTripCase(
                "QBit(BFloat16, 4)",
                "QBit(BFloat16, 4)",
                name => new ArrayColumn<float[]>(name, "QBit(BFloat16, 4)", new[]
                {
                    new[] { 1.0001f, 2f, -3f, 0f },
                    new[] { -0f, 0.5f, -0.5f, 256f },
                }),
                name => new ArrayColumn<float[]>(name, "QBit(BFloat16, 4)", new[]
                {
                    new[] { 1f, 2f, -3f, 0f },
                    new[] { -0f, 0.5f, -0.5f, 256f },
                }),
                settings: null);

            yield return Same(
                "Nullable(QBit(Float32, 4))",
                "Nullable(QBit(Float32, 4))",
                name => new ArrayColumn<float[]>(name, "Nullable(QBit(Float32, 4))", new[]
                {
                    new[] { 1f, 2f, 3f, 4f },
                    null,
                    new[] { -1f, -2f, -3f, -4f },
                }));

            yield return Same(
                "Nullable(QBit(BFloat16, 4))",
                "Nullable(QBit(BFloat16, 4))",
                name => new ArrayColumn<float[]>(name, "Nullable(QBit(BFloat16, 4))", new[]
                {
                    new[] { 1f, 2f, -3f, 0f },
                    null,
                    new[] { -0f, 0.5f, -0.5f, 256f },
                }));

            yield return Same(
                "Nullable(QBit(Float64, 3))",
                "Nullable(QBit(Float64, 3))",
                name => new ArrayColumn<double[]>(name, "Nullable(QBit(Float64, 3))", new[]
                {
                    new[] { 1d, -2d, 3.5d },
                    null,
                    new[] { 0d, -0d, double.NaN },
                }));

            if (TcpServerFeatures.Has(TcpFeature.QBitInt8))
            {
                yield return Same(
                    "QBit(Int8, 17)",
                    "QBit(Int8, 17)",
                    name => new ArrayColumn<sbyte[]>(name, "QBit(Int8, 17)", new[]
                    {
                        Ramp(17, i => (sbyte)(i - 8)),
                        Ramp(17, i => i % 2 == 0 ? sbyte.MaxValue : sbyte.MinValue),
                        Ramp(17, i => i == 0 ? (sbyte)-1 : (sbyte)0),
                    }));

                yield return Same(
                    "Nullable(QBit(Int8, 4))",
                    "Nullable(QBit(Int8, 4))",
                    name => new ArrayColumn<sbyte[]>(name, "Nullable(QBit(Int8, 4))", new[]
                    {
                        new sbyte[] { 1, -2, sbyte.MaxValue, sbyte.MinValue },
                        null,
                        new sbyte[] { 0, -1, 0, -1 },
                    }));
            }
        }

        // SimpleAggregateFunction(func, T) encodes as a bare T — the function only tells the server how to merge
        // rows — so these cases prove the alias is transparent, including when T is itself composite or nullable
        // and when the function carries parameters.
        // A Memory table stores the column as declared, so the round-trip never merges and the value is what was
        // written.
        yield return Same(
            "SimpleAggregateFunction(sum, UInt64)",
            "SimpleAggregateFunction(sum, UInt64)",
            name => PrimitiveColumn<ulong>.FromValues(name, "SimpleAggregateFunction(sum, UInt64)", new ulong[] { 0, 1, ulong.MaxValue }));

        yield return Same(
            "SimpleAggregateFunction(anyLast, Nullable(String))",
            "SimpleAggregateFunction(anyLast, Nullable(String))",
            name => new ArrayColumn<string>(name, "SimpleAggregateFunction(anyLast, Nullable(String))", new[] { "a", null, string.Empty }));

        yield return Same(
            "SimpleAggregateFunction(groupArrayArray, Array(UInt64))",
            "SimpleAggregateFunction(groupArrayArray, Array(UInt64))",
            name => new ArrayColumn<ulong[]>(name, "SimpleAggregateFunction(groupArrayArray, Array(UInt64))", new[]
            {
                new ulong[] { 1, 2, 3 },
                Array.Empty<ulong>(),
            }));

        // A prefix-carrying inner: the alias has to echo the type name into the insert header exactly as declared
        // and still write JSON's version word, which the four cases above (prefix-free inners) cannot show.
        yield return Same(
            "SimpleAggregateFunction(anyLast, JSON)",
            "SimpleAggregateFunction(anyLast, JSON)",
            name => new ArrayColumn<string>(name, "SimpleAggregateFunction(anyLast, JSON)", new[]
            {
                "{\"a\":1}",
                "{}",
            }),
            JsonSettings);

        // A parameterized function keeps its parameters in the type name, on the wire as well as in the DDL. It
        // parses as one node carrying its own arguments, so the type still has exactly two arguments and the
        // parameters are never mistaken for a second inner type.
        yield return Same(
            "SimpleAggregateFunction(groupArrayLastArray(10), Array(String))",
            "SimpleAggregateFunction(groupArrayLastArray(10), Array(String))",
            name => new ArrayColumn<string[]>(name, "SimpleAggregateFunction(groupArrayLastArray(10), Array(String))", new[]
            {
                new[] { "x", string.Empty },
                Array.Empty<string>(),
            }));
    }

    // One row per Geometry alternative, in declared discriminator order, plus a NULL. Each alternative column holds
    // only the rows that selected it — one each here — so every child is a single-row column.
    private static IColumn BuildGeometryColumn(string name)
    {
        var square = new[] { (0d, 0d), (2d, 0d), (2d, 2d), (0d, 0d) };
        IColumn[] alternatives =
        {
            new ArrayColumn<(double, double)[]>(name, "LineString", new[] { new[] { (0d, 0d), (1d, 1d) } }),
            new ArrayColumn<(double, double)[][]>(name, "MultiLineString", new[] { new[] { new[] { (2d, 2d), (3d, 3d) } } }),
            new ArrayColumn<(double, double)[][][]>(name, "MultiPolygon", new[] { new[] { new[] { square } } }),
            new ArrayColumn<(double, double)>(name, "Point", new[] { (1.5d, -2.5d) }),
            new ArrayColumn<(double, double)[][]>(name, "Polygon", new[] { new[] { square } }),
            new ArrayColumn<(double, double)[]>(name, "Ring", new[] { square }),
        };

        var discriminators = new byte[] { 0, 1, 2, 3, 4, 5, IVariantColumn.NullDiscriminator };
        return new VariantColumn(name, "Geometry", discriminators, alternatives, rowCount: discriminators.Length, pooledDiscriminators: false, ownsColumns: false);
    }

    // Merges two settings dictionaries into one (later entries win) for cases needing both flags.
    private static IReadOnlyDictionary<string, string> MergeSettings(IReadOnlyDictionary<string, string> a, IReadOnlyDictionary<string, string> b)
    {
        var merged = new Dictionary<string, string>(a, StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> entry in b)
        {
            merged[entry.Key] = entry.Value;
        }

        return merged;
    }

    // Map(K, V) inserts and reads back the ergonomic jagged column of KeyValuePair arrays, which doubles as expected.
    private static InsertRoundTripCase Maps<TKey, TValue>(string keyType, string valueType, params KeyValuePair<TKey, TValue>[][] rows)
        => Maps(keyType, valueType, settings: null, rows);

    private static InsertRoundTripCase Maps<TKey, TValue>(string keyType, string valueType, IReadOnlyDictionary<string, string> settings, params KeyValuePair<TKey, TValue>[][] rows)
    {
        string type = $"Map({keyType}, {valueType})";
        return Same($"{type} [{rows.Length} rows]", type, name => new ArrayColumn<KeyValuePair<TKey, TValue>[]>(name, type, rows), settings);
    }

    // Builds one map row's pairs, preserving the given order.
    private static KeyValuePair<TKey, TValue>[] Pairs<TKey, TValue>(params (TKey Key, TValue Value)[] pairs)
    {
        var result = new KeyValuePair<TKey, TValue>[pairs.Length];
        for (int i = 0; i < pairs.Length; i++)
        {
            result[i] = new KeyValuePair<TKey, TValue>(pairs[i].Key, pairs[i].Value);
        }

        return result;
    }

    // Builds the dense single-field Nested used as a child by the recursive composition cases above.
    private static NestedColumn ByteNested(string name, string fieldName, byte[] values, int[] offsets)
    {
        string type = $"Nested({fieldName} UInt8)";
        return new NestedColumn(
            name,
            type,
            new[] { fieldName },
            new IColumn[] { new ArrayColumn<byte>(name, "UInt8", values) },
            offsets,
            rowCount: offsets.Length - 1,
            pooledOffsets: false,
            ownsFields: false);
    }

    // Array(T) inserts and reads back the inner element arrays; the ergonomic jagged column doubles as expected.
    private static InsertRoundTripCase Arrays<T>(string innerType, params T[][] rows)
        => Arrays(innerType, settings: null, rows);

    private static InsertRoundTripCase Arrays<T>(string innerType, IReadOnlyDictionary<string, string> settings, params T[][] rows)
    {
        string type = $"Array({innerType})";
        return Same($"{type} [{rows.Length} rows]", type, name => new ArrayColumn<T[]>(name, type, rows), settings);
    }

    // Inserted from the dense shape (flat elements + offsets), read back as the rows those offsets describe.
    private static InsertRoundTripCase DenseArrays<T>(string innerType, T[] elements, int[] offsets)
    {
        string type = $"Array({innerType})";
        var rows = new T[offsets.Length - 1][];
        for (int row = 0; row < rows.Length; row++)
        {
            rows[row] = elements[offsets[row]..offsets[row + 1]];
        }

        return new InsertRoundTripCase(
            $"{type} dense [{rows.Length} rows]",
            type,
            name => ClickHouseTcpColumn.CreateArray(name, ClickHouseTcpColumn.Create(name, elements), offsets),
            name => new ArrayColumn<T[]>(name, type, rows),
            settings: null);
    }

    private static InsertRoundTripCase NullableValues<T>(string innerType, params T?[] values)
        where T : struct
        => NullableValues(innerType, settings: null, values);

    private static InsertRoundTripCase NullableValues<T>(string innerType, IReadOnlyDictionary<string, string> settings, params T?[] values)
        where T : struct
    {
        string type = $"Nullable({innerType})";
        return new InsertRoundTripCase($"{type} [{values.Length} rows]", type, name => new ArrayColumn<T?>(name, type, values), name => new ArrayColumn<T?>(name, type, values), settings);
    }

    // Nullable(DateTime) inserts a DateTimeOffset? but reads back the raw UInt32 epoch seconds (uint?); the
    // expected column carries each present instant's epoch seconds, with nulls preserved.
    private static InsertRoundTripCase NullableDateTimes(params DateTimeOffset?[] values)
        => new(
            $"Nullable(DateTime) [{values.Length} rows]",
            "Nullable(DateTime)",
            name => new ArrayColumn<DateTimeOffset?>(name, "Nullable(DateTime)", values),
            name => new ArrayColumn<uint?>(name, "Nullable(DateTime)", Array.ConvertAll(values, v => v is null ? (uint?)null : (uint)v.Value.ToUnixTimeSeconds())),
            settings: null);

    // Nullable(DateTime64(scale)) surfaces a long? raw count at the column's scale; a null count maps to a null row.
    private static InsertRoundTripCase NullableDateTime64s(int scale, params long?[] counts)
    {
        string type = $"Nullable(DateTime64({scale}))";
        return Same($"{type} [{counts.Length} rows]", type, name => new ArrayColumn<long?>(name, type, counts));
    }

    // Nullable of a wide decimal (Decimal128/256) surfaces a ClickHouseTcpDecimal?; a null string maps to a null row.
    private static InsertRoundTripCase NullableWideDecimals(string innerType, params string[] values)
    {
        string type = $"Nullable({innerType})";
        return Same($"{type} [{values.Length} rows]", type, name => new ArrayColumn<ClickHouseTcpDecimal?>(
            name, type, values.Select(v => v is null ? (ClickHouseTcpDecimal?)null : ParseWide(v)).ToArray()));
    }

    private static InsertRoundTripCase NullableStrings(params string[] values)
        => Same($"Nullable(String) [{values.Length} rows]", "Nullable(String)", name => new ArrayColumn<string>(name, "Nullable(String)", values));

    // Written from the bytes, read back as the text they spell.
    private static InsertRoundTripCase StringBytes(byte[][] rows, params string[] expected)
        => new(
            $"String from bytes [{rows.Length} rows]",
            "String",
            name => new ArrayColumn<byte[]>(name, "String", rows),
            name => new ArrayColumn<string>(name, "String", expected),
            settings: null);

    // As above, and a null row is a NULL rather than a rejected value, because the wrapper carries the nulls.
    private static InsertRoundTripCase NullableStringBytes(byte[][] rows, params string[] expected)
        => new(
            $"Nullable(String) from bytes [{rows.Length} rows]",
            "Nullable(String)",
            name => new ArrayColumn<byte[]>(name, "Nullable(String)", rows),
            name => new ArrayColumn<string>(name, "Nullable(String)", expected),
            settings: null);

    // JSON is inserted and read back as its compact text, so the column is an ArrayColumn<string> like String's.
    // Values must already be canonical for insert to equal read-back; see the normalization case.
    private static InsertRoundTripCase JsonTexts(params string[] values)
        => Same($"JSON [{values.Length} rows]", "JSON", name => new ArrayColumn<string>(name, "JSON", values), JsonSettings);

    private static InsertRoundTripCase NullableJsonTexts(params string[] values)
        => Same($"Nullable(JSON) [{values.Length} rows]", "Nullable(JSON)", name => new ArrayColumn<string>(name, "Nullable(JSON)", values), JsonSettings);

    private static InsertRoundTripCase NullableIps(string innerType, params string[] values)
    {
        string type = $"Nullable({innerType})";
        return Same($"{type} [{values.Length} rows]", type, name => new ArrayColumn<IPAddress>(
            name, type, values.Select(v => v is null ? null : IPAddress.Parse(v)).ToArray()));
    }

    private static InsertRoundTripCase Primitive<T>(string clickHouseType, T[] values)
        where T : unmanaged
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => PrimitiveColumn<T>.FromValues(name, clickHouseType, values));

    private static InsertRoundTripCase Strings(string clickHouseType, params string[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<string>(name, clickHouseType, values));

    // FixedString(N) inserts and reads back a per-row byte[]. Every value must be exactly N bytes: the write path
    // rejects any other width rather than padding or truncating, so a wrong-width case belongs in the codec's unit
    // tests (it never reaches the server), not here.
    private static InsertRoundTripCase FixedStrings(int size, params byte[][] values)
    {
        string type = $"FixedString({size})";
        return Same($"{type} [{values.Length} rows]", type, name => new ArrayColumn<byte[]>(name, type, values));
    }

    private static InsertRoundTripCase NullableFixedStrings(int size, params byte[][] values)
    {
        string type = $"Nullable(FixedString({size}))";
        return Same($"{type} [{values.Length} rows]", type, name => new ArrayColumn<byte[]>(name, type, values));
    }

    // BFloat16 widens to float; values are chosen to be exactly representable so the narrow-on-write is lossless.
    // NaN and the infinities qualify: truncating the low 16 bits keeps an all-ones exponent, and the quiet bit is
    // the mantissa's top bit, which stays.
    private static InsertRoundTripCase BFloat16s(string clickHouseType, IReadOnlyDictionary<string, string> settings, params float[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<float>(name, clickHouseType, values), settings);

    // Time inserts and reads back the raw Int32 seconds; the inserted seconds are returned verbatim.
    private static InsertRoundTripCase TimeSeconds(string clickHouseType, IReadOnlyDictionary<string, string> settings, params int[] seconds)
        => Same($"{clickHouseType} [{seconds.Length} rows]", clickHouseType, name => new ArrayColumn<int>(name, clickHouseType, seconds), settings);

    // Time64 inserts and reads back the raw Int64 counts at the column's scale; the inserted counts are returned verbatim.
    private static InsertRoundTripCase Time64Counts(string clickHouseType, IReadOnlyDictionary<string, string> settings, params long[] counts)
        => Same($"{clickHouseType} [{counts.Length} rows]", clickHouseType, name => new ArrayColumn<long>(name, clickHouseType, counts), settings);

    // DateTime inserts as DateTime (UTC) but reads back as the raw UInt32 epoch seconds, so the expected column
    // carries each instant's epoch seconds regardless of the timezone the server presents.
    private static InsertRoundTripCase DateTimes(string clickHouseType, params DateTime[] values)
        => new(
            $"{clickHouseType} [{values.Length} rows]",
            clickHouseType,
            name => new ArrayColumn<DateTime>(name, clickHouseType, values),
            name => new ArrayColumn<uint>(name, clickHouseType, Array.ConvertAll(values, v => (uint)new DateTimeOffset(v.ToUniversalTime()).ToUnixTimeSeconds())),
            settings: null);

    private static InsertRoundTripCase Dates(string clickHouseType, params DateOnly[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<DateOnly>(name, clickHouseType, values));

    // DateTime64 inserts and reads back the raw Int64 counts at the column's scale, so the inserted counts are
    // returned verbatim regardless of the timezone the server presents.
    private static InsertRoundTripCase DateTime64s(string clickHouseType, params long[] counts)
        => Same($"{clickHouseType} [{counts.Length} rows]", clickHouseType, name => new ArrayColumn<long>(name, clickHouseType, counts));

    // An enum written from its labels reads back as the ordinals they are declared with, so the two columns differ.
    private static InsertRoundTripCase EnumLabels<T>(string clickHouseType, T[] ordinals, params string[] labels)
        => new(
            $"{clickHouseType} from labels [{labels.Length} rows]",
            clickHouseType,
            name => new ArrayColumn<string>(name, clickHouseType, labels),
            name => new ArrayColumn<T>(name, clickHouseType, ordinals),
            settings: null);

    // As above, wrapped: a null label is a NULL row, whose hidden inner value is the codec's placeholder label.
    private static InsertRoundTripCase NullableEnumLabels<T>(string innerType, T?[] ordinals, params string[] labels)
        where T : struct
    {
        string type = $"Nullable({innerType})";
        return new InsertRoundTripCase(
            $"{type} from labels [{labels.Length} rows]",
            type,
            name => new ArrayColumn<string>(name, type, labels),
            name => new ArrayColumn<T?>(name, type, ordinals),
            settings: null);
    }

    // As above, one row per array of labels: the array path flattens them before the enum converts each one.
    private static InsertRoundTripCase ArrayEnumLabels<T>(string innerType, T[][] ordinals, params string[][] labels)
    {
        string type = $"Array({innerType})";
        return new InsertRoundTripCase(
            $"{type} from labels [{labels.Length} rows]",
            type,
            name => new ArrayColumn<string[]>(name, type, labels),
            name => new ArrayColumn<T[]>(name, type, ordinals),
            settings: null);
    }

    private static InsertRoundTripCase Uuids(string clickHouseType, params Guid[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<Guid>(name, clickHouseType, values));

    private static InsertRoundTripCase IpAddresses(string clickHouseType, params string[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<IPAddress>(name, clickHouseType, values.Select(IPAddress.Parse).ToArray()));

    private static InsertRoundTripCase Decimals(string clickHouseType, params decimal[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<decimal>(name, clickHouseType, values));

    private static InsertRoundTripCase WideDecimals(string clickHouseType, params string[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<ClickHouseTcpDecimal>(name, clickHouseType, Array.ConvertAll(values, ParseWide)));

    private static ClickHouseTcpDecimal ParseWide(string text)
    {
        bool negative = text.StartsWith('-');
        string digits = negative ? text.Substring(1) : text;
        int dot = digits.IndexOf('.');
        int scale = dot < 0 ? 0 : digits.Length - dot - 1;
        System.Numerics.BigInteger mantissa = System.Numerics.BigInteger.Parse(dot < 0 ? digits : digits.Remove(dot, 1), System.Globalization.CultureInfo.InvariantCulture);
        return new ClickHouseTcpDecimal(negative ? -mantissa : mantissa, scale);
    }

    /// <summary>A case that inserts and reads back the same column — the common shape.</summary>
    private static InsertRoundTripCase Same(string label, string clickHouseType, Func<string, IColumn> build, IReadOnlyDictionary<string, string> settings = null)
        => new(label, clickHouseType, build, build, settings);

    private static T[] Ramp<T>(int length, Func<int, T> value)
    {
        var values = new T[length];
        for (int i = 0; i < length; i++)
        {
            values[i] = value(i);
        }

        return values;
    }

    /// <summary>Enables Nullable over a Tuple, which the server refuses by default.</summary>
    private static readonly IReadOnlyDictionary<string, string> NullableTupleSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["enable_nullable_tuple_type"] = "1",
    };

    /// <summary>Enables the experimental BFloat16 type for the round-trip.</summary>
    private static readonly IReadOnlyDictionary<string, string> BFloat16Settings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["allow_experimental_bfloat16_type"] = "1",
    };

    /// <summary>Enables the Time/Time64 types for the round-trip (both the experimental and the graduation flag).</summary>
    private static readonly IReadOnlyDictionary<string, string> TimeSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["enable_time_time64_type"] = "1",
        ["allow_experimental_time_time64_type"] = "1",
    };

    /// <summary>
    /// Keeps a <c>Nested</c> column as a single wire column instead of flattening it into parallel dotted
    /// <c>Array</c> columns; must apply at CREATE for the column to be stored as one <c>Nested(...)</c> column.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> NestedSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["flatten_nested"] = "0",
    };

    /// <summary>Allows a <c>LowCardinality</c> over a numeric inner, which the server otherwise rejects as suspicious.</summary>
    private static readonly IReadOnlyDictionary<string, string> LowCardinalitySettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["allow_suspicious_low_cardinality_types"] = "1",
    };

    /// <summary>Enables the experimental <c>Variant</c> type (and allows the suspicious combinations the tests use).</summary>
    private static readonly IReadOnlyDictionary<string, string> VariantSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["allow_experimental_variant_type"] = "1",
        ["allow_suspicious_variant_types"] = "1",
    };

    /// <summary>
    /// Enables the experimental <c>Dynamic</c> type and selects the flattened serialization the client reads and
    /// writes (without it the server would emit the non-flat native default, which the client rejects).
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> DynamicSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["allow_experimental_dynamic_type"] = "1",
        ["output_format_native_use_flattened_dynamic_and_json_serialization"] = "1",
    };

    /// <summary>
    /// Enables the experimental <c>JSON</c> type and selects the String serialization the client reads (without it
    /// the server would emit one of the per-path encodings, which the client rejects). The insert direction needs no
    /// setting — the server reads whichever version the client's state prefix declares. <c>ClickHouseTcpClient</c>
    /// injects the output setting itself; these cases drive a connection directly, so they pass it explicitly.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> JsonSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["allow_experimental_json_type"] = "1",
        ["output_format_native_write_json_as_string"] = "1",
    };
}
