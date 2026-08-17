using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using ClickHouse.Driver.Tcp.Numerics;
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

    private InsertRoundTripCase(string label, string clickHouseType, Func<string, IColumn> buildInsert, Func<string, IColumn> buildExpected, IReadOnlyDictionary<string, string> settings)
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
        yield return Primitive("UInt256", new[] { UInt256.Zero, UInt256.FromBigInteger(1), UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)) });
        yield return Primitive("Int256", new[] { Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 200)), Int256.FromBigInteger(-1), Int256.Zero, Int256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)) });

        // Enum columns are inserted and read as their raw underlying ordinals; the ordinals must be declared members.
        yield return Primitive("Enum8('a' = -1, 'b' = 127)", new sbyte[] { -1, 127 });
        yield return Primitive("Enum16('x' = -32768, 'y' = 32767)", new short[] { -32768, 32767 });

        // Floats and Bool are direct blittable maps, so the primitive factory covers them.
        yield return Primitive("Float32", new[] { 0f, 1.5f, -1.5f, float.MinValue, float.MaxValue });
        yield return Primitive("Float64", new[] { 0d, 1.5, -1.5e100, double.MinValue, double.MaxValue });
        yield return Primitive("Bool", new[] { false, true, true, false });

        yield return Strings("String", string.Empty, "hello", "héllo✓", "a\0b", new string('x', 500));

        // FixedString(N): N contiguous bytes per row, surfaced as a per-row byte[] of exactly N bytes. The bytes
        // are byte-oriented, so embedded NULs and non-UTF-8 bytes ride along unchanged. A wider N crosses the
        // stride past a single row so a mis-strided blit could not pass unnoticed.
        yield return FixedStrings(4, new byte[] { 0, 0, 0, 0 }, new byte[] { 1, 2, 3, 4 }, new byte[] { 0xFF, 0x00, 0xFF, 0x00 });
        yield return FixedStrings(200, Enumerable.Range(0, 200).Select(i => (byte)i).ToArray(), new byte[200]);

        yield return Dates("Date", new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2149, 6, 6));
        yield return Dates("Date32", new DateOnly(1900, 1, 1), new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2299, 12, 31));

        // DateTime reads back as the raw UInt32 epoch seconds. Insert as DateTime (UTC) and expect the epoch
        // seconds of the same instants, regardless of the timezone the server presents.
        yield return DateTimes(
            "DateTime",
            new DateTime(1988, 8, 28, 11, 22, 33, DateTimeKind.Utc),
            new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            DateTime.UnixEpoch);

        // A DateTimeOffset with a non-UTC offset survives as the same instant (i.e. the same epoch seconds).
        var dateTimeOffsets = new[]
        {
            new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)),
            new DateTimeOffset(1988, 8, 28, 11, 22, 33, TimeSpan.FromHours(-8)),
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
        yield return DateTime64s("DateTime64(9)", 0L, 1_700_000_000_123_456_789L, -1_000_000_001L);

        // DateTime64 also accepts a DateTimeOffset on write, converting the instant to the column's scale; the
        // read-back is still the raw count, so at scale 3 the expected column carries epoch milliseconds. The
        // mirror of the "DateTime <- DateTimeOffset" case above.
        var dateTime64Offsets = new[]
        {
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123),
            new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)),
        };
        yield return new InsertRoundTripCase(
            "DateTime64(3) <- DateTimeOffset",
            "DateTime64(3)",
            name => new ArrayColumn<DateTimeOffset>(name, "DateTime64(3)", dateTime64Offsets),
            name => new ArrayColumn<long>(name, "DateTime64(3)", Array.ConvertAll(dateTime64Offsets, o => o.ToUnixTimeMilliseconds())),
            settings: null);

        yield return Uuids("UUID", Guid.Empty, new Guid("00112233-4455-6677-8899-aabbccddeeff"), new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"));

        yield return IpAddresses("IPv4", "0.0.0.0", "127.0.0.1", "192.168.1.1", "255.255.255.255");
        yield return IpAddresses("IPv6", "::", "::1", "2001:db8::1", "fe80::1");

        // Decimal32/64 surface as System.Decimal; Decimal128/256 as ClickHouseDecimal.
        yield return Decimals("Decimal(9, 2)", 0m, 1.23m, -1.23m, 9999999.99m);
        yield return Decimals("Decimal(18, 4)", 0m, 12345.6789m, -12345.6789m, 99999999999999.9999m);
        yield return WideDecimals("Decimal(38, 10)", "0", "12345.6789", "-98765.4321");
        yield return WideDecimals("Decimal(76, 20)", "0", "1.00000000000000000001", "-1.00000000000000000001");

        // The DecimalN(S) alias spellings resolve to the same codecs as Decimal(P, S); one case proves the server
        // round-trips the alias type name as declared.
        yield return Decimals("Decimal64(4)", 0m, 12345.6789m, -12345.6789m);

        // Interval<Unit> surfaces its underlying Int64 count; the unit is kept in the type name.
        yield return Primitive("IntervalSecond", new[] { 0L, 1L, -5L, long.MaxValue });
        yield return Primitive("IntervalDay", new[] { 0L, 7L, -30L });

        // Newer/experimental server types: enable their flag on the round-trip
        yield return BFloat16s("BFloat16", BFloat16Settings, 0f, 1f, -2f, 0.5f, 100f);
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
        yield return NullableValues<float>("Float32", 0f, null, -1.5f, float.MaxValue);
        yield return NullableValues<double>("Float64", 1.5, null, -1.5e100, null);
        yield return NullableValues<bool>("Bool", true, null, false);
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
            new DateTimeOffset(1988, 8, 28, 11, 22, 33, TimeSpan.Zero));
        yield return NullableDateTime64s(3, 0L, null, 1_700_000_000_123L, null);
        yield return NullableDateTime64s(9, 1_700_000_000_123_456_789L, null, -1_000_000_001L);

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
        yield return NullableValues<float>("BFloat16", BFloat16Settings, 0f, null, 1f, -2f);
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
        yield return Arrays("Float32", new[] { 0f, 1.5f, -1.5f, float.MaxValue }, Array.Empty<float>());
        yield return Arrays("Float64", new[] { 0d, -1.5e100, double.MaxValue });
        yield return Arrays("Bool", new[] { true, false, true }, Array.Empty<bool>());
        yield return Arrays("String", new[] { "a", "bb" }, Array.Empty<string>(), new[] { string.Empty, "héllo✓" });
        yield return Arrays<byte[]>("FixedString(4)", new[] { new byte[] { 1, 2, 3, 4 }, new byte[] { 0xFF, 0, 0xFF, 0 } }, Array.Empty<byte[]>());
        yield return Arrays("Date", new[] { new DateOnly(1970, 1, 1), new DateOnly(2149, 6, 6) }, Array.Empty<DateOnly>());
        yield return Arrays("Date32", new[] { new DateOnly(1900, 1, 1), new DateOnly(2299, 12, 31) });

        // Array(DateTime) reads back raw uint epoch seconds; Array(DateTime64) raw long counts at the column scale.
        yield return Arrays<uint>("DateTime", new uint[] { 1_700_000_000, 0 }, Array.Empty<uint>());
        yield return Arrays<long>("DateTime64(3)", new[] { 0L, 1_700_000_000_123L });
        yield return Arrays<long>("DateTime64(9)", new[] { 1_700_000_000_123_456_789L }, Array.Empty<long>());

        yield return Arrays("UUID", new[] { Guid.Empty }, new[] { new Guid("00112233-4455-6677-8899-aabbccddeeff"), new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff") });
        yield return Arrays<IPAddress>("IPv4", new[] { IPAddress.Parse("0.0.0.0"), IPAddress.Parse("255.255.255.255") }, Array.Empty<IPAddress>());
        yield return Arrays<IPAddress>("IPv6", new[] { IPAddress.Parse("::1"), IPAddress.Parse("2001:db8::1") });

        yield return Arrays("Decimal(9, 2)", new[] { 0m, 1.23m, -1.23m, 9999999.99m }, Array.Empty<decimal>());
        yield return Arrays("Decimal(18, 4)", new[] { 12345.6789m, -12345.6789m });
        yield return Arrays<ClickHouseDecimal>("Decimal(38, 10)", new[] { ParseWide("12345.6789"), ParseWide("-98765.4321") });
        yield return Arrays<ClickHouseDecimal>("Decimal(76, 20)", new[] { ParseWide("1.00000000000000000001"), ParseWide("-1.00000000000000000001") });

        yield return Arrays("IntervalSecond", new[] { 0L, 1L, -5L }, Array.Empty<long>());
        yield return Arrays("IntervalDay", new[] { 7L, -30L });

        // Experimental server types: enable their flag on the round-trip (same as their bare cases).
        yield return Arrays("BFloat16", BFloat16Settings, new[] { 0f, 1f, -2f, 0.5f }, Array.Empty<float>());
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

        // Decimal32/64 surface as System.Decimal, Decimal128/256 as ClickHouseDecimal — one tuple spans all four.
        yield return Same(
            "Tuple(Decimal(9, 2), Decimal(18, 4), Decimal(38, 10), Decimal(76, 20))",
            "Tuple(Decimal(9, 2), Decimal(18, 4), Decimal(38, 10), Decimal(76, 20))",
            name => new TupleColumn<decimal, decimal, ClickHouseDecimal, ClickHouseDecimal>(name, "Tuple(Decimal(9, 2), Decimal(18, 4), Decimal(38, 10), Decimal(76, 20))", new (decimal, decimal, ClickHouseDecimal, ClickHouseDecimal)[]
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

        // Array(LowCardinality(String)) flattens its jagged rows into one values stream handed to the
        // low-cardinality codec; empty rows and repeated values ride along.
        yield return Arrays("LowCardinality(String)", new[] { "a", "b" }, Array.Empty<string>(), new[] { "a", "a", "c" });

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
        // address family, and a ClickHouseDecimal carries its own scale. Inferring from the CLR type alone cannot
        // reach either, so this covers the Map slots the Array and Tuple cases above already cover.
        yield return Same(
            "Dynamic [map value, value-disambiguated key and value]",
            "Dynamic",
            name => new ArrayColumn<object>(name, "Dynamic", new object[]
            {
                Pairs<IPAddress, ClickHouseDecimal>((IPAddress.Parse("10.0.0.1"), ParseWide("12345.6789")), (IPAddress.Parse("10.0.0.2"), ParseWide("-1.0002"))),
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
        // canonical read-back CLR types (e.g. ClickHouseDecimal) so insert equals read-back; the
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
                new ClickHouseDecimal(System.Numerics.BigInteger.Parse("1234567890123456789012345"), 5),
                new ulong[] { 1, 2, 3 },
                Pairs<string, uint>(("k", 9), ("m", 10)),
                (1, "t"),
                null,
            }),
            DynamicSettings);

        // Inputs whose inferred ClickHouse type reads back as a different (canonical) CLR type: a DateTimeOffset
        // and a DateTime infer to DateTime64(9) (read back as the raw long nanosecond count), and a System.Decimal
        // infers to Decimal128 (read back as ClickHouseDecimal, equal by value).
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
                new ClickHouseDecimal(new System.Numerics.BigInteger(123456789), 4),
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

    // Nullable of a wide decimal (Decimal128/256) surfaces a ClickHouseDecimal?; a null string maps to a null row.
    private static InsertRoundTripCase NullableWideDecimals(string innerType, params string[] values)
    {
        string type = $"Nullable({innerType})";
        return Same($"{type} [{values.Length} rows]", type, name => new ArrayColumn<ClickHouseDecimal?>(
            name, type, values.Select(v => v is null ? (ClickHouseDecimal?)null : ParseWide(v)).ToArray()));
    }

    private static InsertRoundTripCase NullableStrings(params string[] values)
        => Same($"Nullable(String) [{values.Length} rows]", "Nullable(String)", name => new ArrayColumn<string>(name, "Nullable(String)", values));

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

    private static InsertRoundTripCase Uuids(string clickHouseType, params Guid[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<Guid>(name, clickHouseType, values));

    private static InsertRoundTripCase IpAddresses(string clickHouseType, params string[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<IPAddress>(name, clickHouseType, values.Select(IPAddress.Parse).ToArray()));

    private static InsertRoundTripCase Decimals(string clickHouseType, params decimal[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<decimal>(name, clickHouseType, values));

    private static InsertRoundTripCase WideDecimals(string clickHouseType, params string[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<ClickHouseDecimal>(name, clickHouseType, Array.ConvertAll(values, ParseWide)));

    private static ClickHouseDecimal ParseWide(string text)
    {
        bool negative = text.StartsWith('-');
        string digits = negative ? text.Substring(1) : text;
        int dot = digits.IndexOf('.');
        int scale = dot < 0 ? 0 : digits.Length - dot - 1;
        System.Numerics.BigInteger mantissa = System.Numerics.BigInteger.Parse(dot < 0 ? digits : digits.Remove(dot, 1), System.Globalization.CultureInfo.InvariantCulture);
        return new ClickHouseDecimal(negative ? -mantissa : mantissa, scale);
    }

    /// <summary>A case that inserts and reads back the same column — the common shape.</summary>
    private static InsertRoundTripCase Same(string label, string clickHouseType, Func<string, IColumn> build, IReadOnlyDictionary<string, string> settings = null)
        => new(label, clickHouseType, build, build, settings);

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
}
