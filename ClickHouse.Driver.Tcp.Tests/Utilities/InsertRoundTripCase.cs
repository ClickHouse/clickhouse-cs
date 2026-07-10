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
    }

    private static InsertRoundTripCase Primitive<T>(string clickHouseType, T[] values)
        where T : unmanaged
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => PrimitiveColumn<T>.FromValues(name, clickHouseType, values));

    private static InsertRoundTripCase Strings(string clickHouseType, params string[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<string>(name, clickHouseType, values));

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
}
