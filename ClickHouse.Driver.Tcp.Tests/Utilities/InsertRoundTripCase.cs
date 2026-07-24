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
        // are byte-oriented, so embedded NULs and non-UTF-8 bytes ride along unchanged.
        yield return FixedStrings(4, new byte[] { 0, 0, 0, 0 }, new byte[] { 1, 2, 3, 4 }, new byte[] { 0xFF, 0x00, 0xFF, 0x00 });

        // A value shorter than N is right-padded to N zero bytes by the server, so the read-back differs from the
        // inserted bytes; the empty value becomes an all-zero row and a full-width value is unchanged.
        yield return new InsertRoundTripCase(
            "FixedString(6) [padding]",
            "FixedString(6)",
            name => new ArrayColumn<byte[]>(name, "FixedString(6)", new[] { Array.Empty<byte>(), new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3, 4, 5, 6 } }),
            name => new ArrayColumn<byte[]>(name, "FixedString(6)", new[] { new byte[6], new byte[] { 1, 2, 3, 0, 0, 0 }, new byte[] { 1, 2, 3, 4, 5, 6 } }),
            settings: null);

        yield return Dates("Date", new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2149, 6, 6));
        yield return Dates("Date32", new DateOnly(1900, 1, 1), new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2299, 12, 31));

        // DateTime reads back as a DateTimeOffset; equality compares the instant, so the offset the server
        // presents does not matter. Insert as DateTime (UTC) and expect the same instants.
        yield return DateTimes(
            "DateTime",
            new DateTime(1988, 8, 28, 11, 22, 33, DateTimeKind.Utc),
            new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            DateTime.UnixEpoch);

        // A DateTimeOffset with a non-UTC offset survives as the same instant.
        yield return Same("DateTime <- DateTimeOffset", "DateTime", name => new ArrayColumn<DateTimeOffset>(
            name,
            "DateTime",
            new[]
            {
                new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)),
                new DateTimeOffset(1988, 8, 28, 11, 22, 33, TimeSpan.FromHours(-8)),
            }));

        // DateTime64 surfaces as ClickHouseDateTime64, which retains the exact wire count at any scale. Scale 9
        // (nanoseconds) is finer than a .NET tick, so the round-trip proves precision no DateTimeOffset can hold.
        yield return DateTime64s("DateTime64(3)", 3, 0L, 1_700_000_000_123L, -6_000_000_000_000L);
        yield return DateTime64s("DateTime64(9)", 9, 0L, 1_700_000_000_123_456_789L, -1_000_000_001L);

        yield return Uuids("UUID", Guid.Empty, new Guid("00112233-4455-6677-8899-aabbccddeeff"), new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"));

        yield return IpAddresses("IPv4", "0.0.0.0", "127.0.0.1", "192.168.1.1", "255.255.255.255");
        yield return IpAddresses("IPv6", "::", "::1", "2001:db8::1", "fe80::1");

        // Decimal32/64 surface as System.Decimal; Decimal128/256 as ClickHouseDecimal.
        yield return Decimals("Decimal(9, 2)", 0m, 1.23m, -1.23m, 9999999.99m);
        yield return Decimals("Decimal(18, 4)", 0m, 12345.6789m, -12345.6789m, 99999999999999.9999m);
        yield return WideDecimals("Decimal(38, 10)", "0", "12345.6789", "-98765.4321");
        yield return WideDecimals("Decimal(76, 20)", "0", "1.00000000000000000001", "-1.00000000000000000001");

        // Interval<Unit> surfaces its underlying Int64 count; the unit is kept in the type name.
        yield return Primitive("IntervalSecond", new[] { 0L, 1L, -5L, long.MaxValue });
        yield return Primitive("IntervalDay", new[] { 0L, 7L, -30L });

        // Newer/experimental server types: enable their flag on the round-trip
        yield return BFloat16s("BFloat16", BFloat16Settings, 0f, 1f, -2f, 0.5f, 100f);
        yield return Times("Time", TimeSettings, TimeSpan.Zero, new TimeSpan(12, 34, 56), new TimeSpan(-1, -2, -3));
        yield return Times("Time64(3)", TimeSettings, TimeSpan.Zero, new TimeSpan(0, 1, 2, 3, 456), new TimeSpan(-0, -1, -2, -3, -456));

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

        // Experimental server types: enable their flag on the round-trip (same as their non-nullable cases).
        yield return NullableValues<float>("BFloat16", BFloat16Settings, 0f, null, 1f, -2f);
        yield return NullableValues<TimeSpan>("Time", TimeSettings, TimeSpan.Zero, null, new TimeSpan(12, 34, 56));
        yield return NullableValues<TimeSpan>("Time64(3)", TimeSettings, TimeSpan.Zero, null, new TimeSpan(0, 1, 2, 3, 456));

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

        // Array(DateTime)/Array(DateTime64) insert and read back a DateTimeOffset / ClickHouseDateTime64 element;
        // equality is by instant, so the offset the server presents does not matter.
        yield return Arrays<DateTimeOffset>("DateTime", new[] { new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero), DateTimeOffset.UnixEpoch }, Array.Empty<DateTimeOffset>());
        yield return Arrays<ClickHouseDateTime64>("DateTime64(3)", new[] { new ClickHouseDateTime64(0L, 3, TimeSpan.Zero), new ClickHouseDateTime64(1_700_000_000_123L, 3, TimeSpan.Zero) });
        yield return Arrays<ClickHouseDateTime64>("DateTime64(9)", new[] { new ClickHouseDateTime64(1_700_000_000_123_456_789L, 9, TimeSpan.Zero) }, Array.Empty<ClickHouseDateTime64>());

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
        yield return Arrays("Time", TimeSettings, new[] { TimeSpan.Zero, new TimeSpan(12, 34, 56) }, Array.Empty<TimeSpan>());
        yield return Arrays("Time64(3)", TimeSettings, new[] { TimeSpan.Zero, new TimeSpan(0, 1, 2, 3, 456) });

        // Array(Nullable(T)): nullability composed inside the array, for both a value inner and a reference inner.
        yield return Arrays<uint?>("Nullable(UInt32)", new uint?[] { 1, null, 3 }, Array.Empty<uint?>(), new uint?[] { null });
        yield return Arrays<int?>("Nullable(Int32)", new int?[] { int.MinValue, null, 0 });
        yield return Arrays<string>("Nullable(String)", new[] { "x", null, string.Empty }, new string[] { null });
        yield return Arrays<IPAddress>("Nullable(IPv4)", new[] { IPAddress.Parse("127.0.0.1"), null });

        // Nested arrays: the same offsets-plus-values shape recurses one level down.
        yield return Arrays<byte[]>("Array(UInt8)", new[] { new byte[] { 1, 2 } }, Array.Empty<byte[]>(), new[] { new byte[] { 3 }, new byte[] { 4, 5 } });
        yield return Arrays<string[]>("Array(String)", new[] { new[] { "a" }, new[] { "b", "c" } }, Array.Empty<string[]>());

        // Tuple(...): a heterogeneous fixed-arity composite serialized as N side-by-side element columns. The
        // cases below collectively touch every supported element type across various arities (1 through the
        // supported maximum of 7), then compose tuples with named elements, nesting, Nullable and Array elements,
        // and an Array(Tuple(...)) that flattens through the tuple codec's per-element write path. Element names
        // do not change the CLR value (a ValueTuple either way); they only ride along in the type string.
        yield return Same(
            "Tuple(Int32) [arity 1]",
            "Tuple(Int32)",
            name => new TupleColumn<int>(name, "Tuple(Int32)", new[] { new ValueTuple<int>(1), new ValueTuple<int>(int.MinValue), new ValueTuple<int>(int.MaxValue) }));

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

        // DateTime reads back a DateTimeOffset and DateTime64 a ClickHouseDateTime64; equality is by instant, so
        // insert-as-UTC matches whatever offset the server presents on read.
        yield return Same(
            "Tuple(Date, Date32, DateTime, DateTime64(3), UUID)",
            "Tuple(Date, Date32, DateTime, DateTime64(3), UUID)",
            name => new TupleColumn<DateOnly, DateOnly, DateTimeOffset, ClickHouseDateTime64, Guid>(name, "Tuple(Date, Date32, DateTime, DateTime64(3), UUID)", new (DateOnly, DateOnly, DateTimeOffset, ClickHouseDateTime64, Guid)[]
            {
                (new DateOnly(1970, 1, 1), new DateOnly(1900, 1, 1), DateTimeOffset.UnixEpoch, new ClickHouseDateTime64(0L, 3, TimeSpan.Zero), Guid.Empty),
                (new DateOnly(2149, 6, 6), new DateOnly(2299, 12, 31), new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero), new ClickHouseDateTime64(1_700_000_000_123L, 3, TimeSpan.Zero), new Guid("00112233-4455-6677-8899-aabbccddeeff")),
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
            name => new TupleColumn<TimeSpan, TimeSpan>(name, "Tuple(Time, Time64(3))", new (TimeSpan, TimeSpan)[]
            {
                (TimeSpan.Zero, TimeSpan.Zero),
                (new TimeSpan(12, 34, 56), new TimeSpan(0, 1, 2, 3, 456)),
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

        // A max-arity (7) tuple mixing fixed-width and variable-width elements.
        yield return Same(
            "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32, String) [arity 7]",
            "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32, String)",
            name => new TupleColumn<byte, sbyte, ushort, short, uint, int, string>(name, "Tuple(UInt8, Int8, UInt16, Int16, UInt32, Int32, String)", new (byte, sbyte, ushort, short, uint, int, string)[]
            {
                (0, -128, 0, short.MinValue, 0, int.MinValue, string.Empty),
                (255, 127, ushort.MaxValue, short.MaxValue, uint.MaxValue, int.MaxValue, "héllo✓"),
            }));

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

        // Composite fields recurse: a nullable field and an array field. Rows: [(1,['x','y']),(null,[])], [], [(-5,['z'])].
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
                    new ArrayColumn<string[]>(name, "Array(String)", new[] { new[] { "x", "y" }, Array.Empty<string>(), new[] { "z" } }),
                },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
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
        // LowCardinality(Nullable(T)), which is a separate (not-yet-supported) feature. A numeric inner is
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
    }

    // Map(K, V) inserts and reads back the ergonomic jagged column of KeyValuePair arrays, which doubles as expected.
    private static InsertRoundTripCase Maps<TKey, TValue>(string keyType, string valueType, params KeyValuePair<TKey, TValue>[][] rows)
    {
        string type = $"Map({keyType}, {valueType})";
        return Same($"{type} [{rows.Length} rows]", type, name => new ArrayColumn<KeyValuePair<TKey, TValue>[]>(name, type, rows));
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

    // Nullable(DateTime) inserts and reads back a DateTimeOffset?; DateTimeOffset equality compares the instant,
    // so the offset the server presents on read need not match the (UTC) one supplied here.
    private static InsertRoundTripCase NullableDateTimes(params DateTimeOffset?[] values)
        => Same($"Nullable(DateTime) [{values.Length} rows]", "Nullable(DateTime)", name => new ArrayColumn<DateTimeOffset?>(name, "Nullable(DateTime)", values));

    // Nullable(DateTime64(scale)) surfaces a ClickHouseDateTime64?; a null count maps to a null row.
    private static InsertRoundTripCase NullableDateTime64s(int scale, params long?[] counts)
    {
        string type = $"Nullable(DateTime64({scale}))";
        return Same($"{type} [{counts.Length} rows]", type, name => new ArrayColumn<ClickHouseDateTime64?>(
            name, type, Array.ConvertAll(counts, c => c is null ? (ClickHouseDateTime64?)null : new ClickHouseDateTime64(c.Value, scale, TimeSpan.Zero))));
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

    // FixedString(N) inserts and reads back a per-row byte[]; values must be exactly N bytes for the read-back to
    // equal the inserted column (a shorter value is server-padded — see the dedicated padding case).
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

    private static InsertRoundTripCase Times(string clickHouseType, IReadOnlyDictionary<string, string> settings, params TimeSpan[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<TimeSpan>(name, clickHouseType, values), settings);

    // DateTime inserts as DateTime (UTC) but reads back as a DateTimeOffset; DateTimeOffset equality compares
    // the instant, so the expected column carries the same instants regardless of the presented offset.
    private static InsertRoundTripCase DateTimes(string clickHouseType, params DateTime[] values)
        => new(
            $"{clickHouseType} [{values.Length} rows]",
            clickHouseType,
            name => new ArrayColumn<DateTime>(name, clickHouseType, values),
            name => new ArrayColumn<DateTimeOffset>(name, clickHouseType, Array.ConvertAll(values, v => new DateTimeOffset(v.ToUniversalTime()))),
            settings: null);

    private static InsertRoundTripCase Dates(string clickHouseType, params DateOnly[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<DateOnly>(name, clickHouseType, values));

    // DateTime64 inserts and reads back a ClickHouseDateTime64. Equality is by instant, so the offset each value
    // carries here (UTC) need not match the offset the server presents on read.
    private static InsertRoundTripCase DateTime64s(string clickHouseType, int scale, params long[] counts)
        => Same(
            $"{clickHouseType} [{counts.Length} rows]",
            clickHouseType,
            name => new ArrayColumn<ClickHouseDateTime64>(name, clickHouseType, Array.ConvertAll(counts, c => new ClickHouseDateTime64(c, scale, TimeSpan.Zero))));

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
}
