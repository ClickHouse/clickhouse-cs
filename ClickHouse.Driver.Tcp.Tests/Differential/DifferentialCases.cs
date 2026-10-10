using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using ClickHouse.Driver.Tcp.Tests.Types;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The case list of the differential tests: every case of <c>InsertRoundTripCase</c> and of
/// <c>CompositeLiftMatrixTests</c>, a table of column types with read targets, and a list of value and error scenarios
/// of <c>ReadAs</c>.
/// </summary>
public static class DifferentialCases
{
    /// <summary>The number of cases from <c>InsertRoundTripCase.CasesFor(TcpFeature.All)</c>.</summary>
    public const int InsertRoundTripCount = 286;

    /// <summary>The number of cases from <c>CompositeLiftMatrixTests.Cases()</c>.</summary>
    public const int CompositeLiftMatrixCount = 22;

    /// <summary>The number of column types of the read targets table (<see cref="CaseSource.ColumnReadProjection"/>).</summary>
    public const int ColumnReadProjectionCount = 56;

    /// <summary>The number of value and error scenarios (<see cref="CaseSource.ColumnReadScenario"/>).</summary>
    public const int ColumnReadScenarioCount = 39;

    /// <summary>
    /// The column types that the read tests of the client read, each with the CLR types that a case reads it as: the
    /// canonical type first, then the types that the column type converts to, then types that it is refused as or that
    /// a read rule of D6 gives. Each type is a case of <see cref="CaseSource.ColumnReadProjection"/>, with sample values.
    /// </summary>
    private static readonly (string Type, Type[] Targets)[] ColumnReadProjectionTargets =
    {
        ("UInt8", new[] { typeof(byte) }),
        ("Int32", new[] { typeof(int) }),
        ("UInt64", new[] { typeof(ulong), typeof(DateTime), typeof(ulong?) }),
        ("Int128", new[] { typeof(Int128) }),
        ("Float32", new[] { typeof(float) }),
        ("Float64", new[] { typeof(double) }),
        ("Bool", new[] { typeof(bool) }),
        ("String", new[] { typeof(string), typeof(byte[]) }),
        ("FixedString(4)", new[] { typeof(byte[]), typeof(string) }),
        ("Date", new[] { typeof(DateOnly), typeof(DateOnly?) }),
        ("Date32", new[] { typeof(DateOnly) }),
        ("DateTime", new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) }),
        ("DateTime('Europe/Berlin')", new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) }),
        ("DateTime64(3)", new[] { typeof(long), typeof(DateTimeOffset), typeof(DateTime) }),
        ("DateTime64(9, 'UTC')", new[] { typeof(long), typeof(DateTimeOffset), typeof(DateTime) }),
        ("Time", new[] { typeof(int), typeof(TimeSpan), typeof(TimeOnly), typeof(DateTime), typeof(TimeSpan?) }),
        ("Time64(3)", new[] { typeof(long), typeof(TimeSpan), typeof(TimeOnly), typeof(DateTime), typeof(TimeSpan?) }),
        ("UUID", new[] { typeof(Guid), typeof(Guid?) }),
        ("IPv4", new[] { typeof(IPAddress) }),
        ("IPv6", new[] { typeof(IPAddress) }),
        ("Decimal(9, 2)", new[] { typeof(decimal) }),
        ("Decimal(38, 10)", new[] { typeof(ClickHouseTcpDecimal) }),
        ("Enum8('a' = 1)", new[] { typeof(sbyte), typeof(string), typeof(int) }),
        ("Nullable(Int32)", new[] { typeof(int?), typeof(long?) }),
        ("Nullable(String)", new[] { typeof(string), typeof(byte[]), typeof(byte[][]) }),
        ("Nullable(DateTime)", new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) }),
        ("Nullable(Time64(3))", new[] { typeof(long?), typeof(TimeSpan?), typeof(TimeOnly?), typeof(TimeSpan) }),
        ("LowCardinality(String)", new[] { typeof(string), typeof(byte[]), typeof(Guid) }),
        ("LowCardinality(UInt32)", new[] { typeof(uint) }),
        ("LowCardinality(Nullable(DateTime))", new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) }),
        ("Array(Int32)", new[] { typeof(int[]) }),
        ("Map(String, Int32)", new[] { typeof(KeyValuePair<string, int>[]) }),
        ("Tuple(Int32, String)", new[] { typeof((int, string)) }),
        ("Variant(Int32, String)", new[] { typeof(object) }),
        ("Dynamic", new[] { typeof(object) }),
        ("Nullable(DateTime('UTC'))", new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?), typeof(DateTime), typeof(TimeSpan?), typeof(uint) }),
        ("Nullable(DateTime64(3, 'UTC'))", new[] { typeof(long?), typeof(DateTimeOffset?), typeof(DateTime?) }),
        ("Nullable(Time)", new[] { typeof(int?), typeof(TimeSpan?), typeof(TimeOnly?) }),
        ("Nullable(UUID)", new[] { typeof(Guid?) }),
        ("LowCardinality(DateTime('UTC'))", new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) }),
        ("LowCardinality(Nullable(String))", new[] { typeof(string), typeof(byte[]) }),
        ("LowCardinality(Nullable(DateTime('UTC')))", new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?), typeof(DateTime), typeof(DateTimeOffset), typeof(TimeSpan?) }),
        ("Enum8('a' = 1, 'b' = 2)", new[] { typeof(sbyte), typeof(string) }),
        ("Array(DateTime('UTC'))", new[] { typeof(uint[]), typeof(DateTime[]) }),
        ("Tuple(DateTime('UTC'), Time)", new[] { typeof((uint, int)), typeof((DateTime, TimeSpan)) }),
        ("Map(String, DateTime('UTC'))", new[] { typeof(KeyValuePair<string, uint>[]), typeof(KeyValuePair<string, DateTime>[]) }),
        ("Array(String)", new[] { typeof(string[]), typeof(Guid[]), typeof(byte[]), typeof(byte[][]), typeof(string) }),
        ("Array(Array(String))", new[] { typeof(string[][]), typeof(byte[][][]) }),
        ("Map(String, String)", new[] { typeof(KeyValuePair<string, string>[]), typeof(KeyValuePair<byte[], byte[]>[]), typeof(string) }),
        ("Map(UInt8, String)", new[] { typeof(KeyValuePair<byte, string>[]), typeof(KeyValuePair<byte, byte[]>[]), typeof(KeyValuePair<long, byte[]>[]) }),
        ("Tuple(String)", new[] { typeof(ValueTuple<string>), typeof(ValueTuple<byte[]>) }),
        ("Tuple(UInt8, String)", new[] { typeof((byte, string)), typeof((byte, byte[])), typeof((long, byte[])), typeof(string) }),
        ("LowCardinality(FixedString(4))", new[] { typeof(byte[]), typeof(string) }),
        ("JSON", new[] { typeof(string), typeof(byte[]) }),
        ("DateTime('UTC')", new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime), typeof(DateTime?), typeof(DateTimeOffset?), typeof(TimeSpan) }),
        ("DateTime64(3, 'UTC')", new[] { typeof(long), typeof(DateTimeOffset), typeof(DateTime), typeof(DateTime?), typeof(TimeSpan) }),
    };

    private static readonly Lazy<IReadOnlyList<DifferentialCase>> Cases = new(Build);

    /// <summary>Every case, for an NUnit <c>TestCaseSource</c>.</summary>
    /// <returns>The cases, in a fixed order.</returns>
    public static IEnumerable<DifferentialCase> All() => Cases.Value;

    private static IReadOnlyList<DifferentialCase> Build()
    {
        var cases = new List<DifferentialCase>();
        cases.AddRange(UniqueIds(CaseSource.InsertRoundTrip, FromInsertRoundTrip()));
        cases.AddRange(UniqueIds(CaseSource.CompositeLiftMatrix, FromCompositeLiftMatrix()));
        cases.AddRange(UniqueIds(CaseSource.ColumnReadProjection, FromColumnReadProjection()));
        cases.AddRange(UniqueIds(CaseSource.ColumnReadScenario, FromColumnReadScenarios()));
        return cases;
    }

    // Two cases of one source can have the same label, so the second and later ones get " #2", " #3" and so on.
    private static IEnumerable<DifferentialCase> UniqueIds(CaseSource source, IEnumerable<(string Label, Func<string, DifferentialCase> Make)> cases)
    {
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach ((string label, Func<string, DifferentialCase> make) in cases)
        {
            seen[label] = seen.TryGetValue(label, out int count) ? count + 1 : 1;
            yield return make($"{source}: {label}{(seen[label] > 1 ? $" #{seen[label]}" : string.Empty)}");
        }
    }

    // The read targets are the element type of the expected column and of the inserted column. The write inputs
    // are the inserted column and the decoded column.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromInsertRoundTrip()
    {
        foreach (InsertRoundTripCase roundTrip in InsertRoundTripCase.CasesFor(TcpFeature.All))
        {
            Type expectedType;
            Type insertType;
            int rowCount;
            using (IColumn expected = roundTrip.BuildExpectedColumn("value"))
            using (IColumn insert = roundTrip.BuildInsertColumn("value"))
            {
                expectedType = expected.ElementType;
                insertType = insert.ElementType;
                rowCount = insert.RowCount;
            }

            Type decodedType = Codec(roundTrip.ClickHouseType).ElementType;
            yield return (roundTrip.ToString(), id => new DifferentialCase(
                id,
                CaseSource.InsertRoundTrip,
                roundTrip.ClickHouseType,
                rowCount,
                new[] { expectedType, insertType }.Distinct().ToList(),
                new[] { WriteInput.Built("insert", insertType, roundTrip.BuildInsertColumn), WriteInput.Decoded(decodedType) }));
        }
    }

    // The read targets are the canonical and the lifted type. The write inputs are sample values of the canonical
    // type, and the values that the baseline reads as the lifted type.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromCompositeLiftMatrix()
    {
        foreach (CompositeLiftMatrixTests.Case lift in CompositeLiftMatrixTests.Cases())
        {
            yield return (lift.ColumnType, id => TypeOnlyCase(id, CaseSource.CompositeLiftMatrix, lift.ColumnType, new[] { lift.Canonical, lift.Lifted }));
        }
    }

    // One case for each column type of ColumnReadProjectionTargets, with its read targets.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromColumnReadProjection()
    {
        foreach ((string type, Type[] targets) in ColumnReadProjectionTargets)
        {
            yield return (type, id => TypeOnlyCase(id, CaseSource.ColumnReadProjection, type, targets));
        }
    }

    // One case for each scenario: its own values, read as its canonical type and as its target. The scenario states
    // the outcome of the ReadAs facet of its target.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromColumnReadScenarios()
    {
        foreach (ColumnReadScenario scenario in Scenarios())
        {
            Type canonical = Codec(scenario.ColumnType).ElementType;
            if (scenario.Values.GetType().GetElementType() != canonical)
            {
                throw new InvalidOperationException($"Scenario '{scenario.Name}' has values of {scenario.Values.GetType().GetElementType()}, not of the canonical type {canonical}.");
            }

            Expectation expected = scenario.ExceptionType is null
                ? Expectation.Values(scenario.Expected.Cast<object>().ToArray())
                : Expectation.Fails(scenario.ExceptionType, scenario.MessageParts.ToArray());

            var inputs = new List<WriteInput>
            {
                WriteInput.Built("canonical", canonical, name => (IColumn)Activator.CreateInstance(
                    typeof(ArrayColumn<>).MakeGenericType(canonical), name, scenario.ColumnType, (Array)scenario.Values.Clone())),
            };

            if (scenario.Target != canonical)
            {
                inputs.Add(WriteInput.ReadBack(scenario.Target));
            }

            yield return (scenario.Name, id => new DifferentialCase(
                id,
                CaseSource.ColumnReadScenario,
                scenario.ColumnType,
                scenario.Values.Length,
                new[] { canonical, scenario.Target }.Distinct().ToList(),
                inputs,
                new[] { new StatedOutcome(Tier.ReadAs, scenario.Target, expected) }));
        }
    }

    // The value and error scenarios of ReadAs that a server round trip cannot observe: the DateTimeKind of a calendar
    // reading, the scale of a count, and the values that have no reading and fail on their row.
    private static IEnumerable<ColumnReadScenario> Scenarios() => new[]
    {
        // The instant in the timezone of the column. 1700000000 = 2023-11-14T22:13:20Z, which is 23:13:20 +01:00 in
        // Berlin (winter, no DST).
        ColumnReadScenario.Reads("DateTime('Europe/Berlin') as DateTimeOffset", "DateTime('Europe/Berlin')", new uint[] { 1_700_000_000 }, new DateTimeOffset(2023, 11, 14, 23, 13, 20, TimeSpan.FromHours(1))),

        // A timezone that TimeZoneInfo cannot hold fails on the row, and not when the reading is built.
        ColumnReadScenario.Throws<uint, DateTimeOffset, FormatException>("DateTime('Fixed/UTC+19:00:00') as DateTimeOffset", "DateTime('Fixed/UTC+19:00:00')", 1_700_000_000, "+19:00:00"),
        ColumnReadScenario.Throws<uint, DateTimeOffset, FormatException>("DateTime('Fixed/UTC+05:30:15') as DateTimeOffset", "DateTime('Fixed/UTC+05:30:15')", 1_700_000_000, "+05:30:15"),
        ColumnReadScenario.Throws<long, DateTimeOffset, FormatException>("DateTime64(3, 'Fixed/UTC+19:00:00') as DateTimeOffset", "DateTime64(3, 'Fixed/UTC+19:00:00')", 1_700_000_000_000, "+19:00:00"),
        ColumnReadScenario.Throws<long, DateTimeOffset, FormatException>("DateTime64(9, 'Fixed/UTC+05:30:15') as DateTimeOffset", "DateTime64(9, 'Fixed/UTC+05:30:15')", 1_700_000_000_000, "+05:30:15"),

        // A UTC column gives DateTimeKind.Utc.
        ColumnReadScenario.Reads("DateTime('UTC') as DateTime", "DateTime('UTC')", new uint[] { 1_700_000_000 }, new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)),

        // A column with a non-zero offset gives the wall clock in its timezone as DateTimeKind.Unspecified. This is
        // the rule of ToDateTime in the HTTP driver, so a POCO that reads the same column through either client gets
        // the same value.
        ColumnReadScenario.Reads("DateTime('Europe/Berlin') as DateTime", "DateTime('Europe/Berlin')", new uint[] { 1_700_000_000 }, new DateTime(2023, 11, 14, 23, 13, 20, DateTimeKind.Unspecified)),

        // The scale of the column applies. Scale 9 is finer than a .NET tick, so the sub-100 ns digits truncate toward
        // zero.
        ColumnReadScenario.Reads("DateTime64(3, 'UTC') as DateTimeOffset", "DateTime64(3, 'UTC')", new[] { 1_700_000_000_123L }, Instant("2023-11-14T22:13:20.1230000Z")),
        ColumnReadScenario.Reads("DateTime64(9, 'UTC') as DateTimeOffset", "DateTime64(9, 'UTC')", new[] { 1_700_000_000_123_456_789L }, Instant("2023-11-14T22:13:20.1234567Z")),
        ColumnReadScenario.Reads("DateTime64(0, 'UTC') as DateTimeOffset", "DateTime64(0, 'UTC')", new[] { 1_700_000_000L }, Instant("2023-11-14T22:13:20.0000000Z")),

        // DateTime64 has the DateTimeKind rule of DateTime.
        ColumnReadScenario.Reads("DateTime64(3, 'UTC') as DateTime", "DateTime64(3, 'UTC')", new[] { 1_700_000_000_123L }, new DateTime(2023, 11, 14, 22, 13, 20, 123, DateTimeKind.Utc)),
        ColumnReadScenario.Reads("DateTime64(3, 'Europe/Berlin') as DateTime", "DateTime64(3, 'Europe/Berlin')", new[] { 1_700_000_000_123L }, new DateTime(2023, 11, 14, 23, 13, 20, 123, DateTimeKind.Unspecified)),

        // A count that decodes but names an instant outside the .NET calendar fails with an OverflowException that
        // names the raw values. The canonical read gives the exact count.
        ColumnReadScenario.Throws<long, DateTimeOffset, OverflowException>("DateTime64(0, 'UTC') as DateTimeOffset: beyond the calendar range", "DateTime64(0, 'UTC')", long.MaxValue, "Values"),

        // Time reads as exact whole seconds, and Time64 with the scale of the column.
        ColumnReadScenario.Reads("Time as TimeSpan", "Time", new[] { 3661, -3661, 0 }, new TimeSpan(1, 1, 1), new TimeSpan(1, 1, 1).Negate(), TimeSpan.Zero),
        ColumnReadScenario.Reads("Time64(3) as TimeSpan", "Time64(3)", new[] { 3_661_500L }, TimeSpan.Parse("01:01:01.5000000", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(9) as TimeSpan", "Time64(9)", new[] { -1_000_000_001L }, TimeSpan.Parse("-00:00:01.0000000", CultureInfo.InvariantCulture)),

        // The time of day, with the scale of the column.
        ColumnReadScenario.Reads("Time as TimeOnly", "Time", new[] { 3661, 0, (23 * 3600) + (59 * 60) + 59 }, new TimeOnly(1, 1, 1), TimeOnly.MinValue, new TimeOnly(23, 59, 59)),
        ColumnReadScenario.Reads("Time64(3) as TimeOnly", "Time64(3)", new[] { 3_661_500L }, TimeOnly.Parse("01:01:01.5000000", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(9) as TimeOnly", "Time64(9)", new[] { 3_661_000_000_000L }, TimeOnly.Parse("01:01:01", CultureInfo.InvariantCulture)),

        // TimeOnly cannot hold a negative value or a duration of one day or more, and the reading does not wrap it. The
        // message names the reading that works, TimeSpan. The Time64 cases check raw counts, because a negative count
        // smaller than one tick truncates to TimeSpan.Zero.
        NoTimeOfDay("Time as TimeOnly: a negative duration", "Time", -1),
        NoTimeOfDay("Time as TimeOnly: exactly 24 hours", "Time", 24 * 3600),
        NoTimeOfDay("Time as TimeOnly: a duration of 100 hours", "Time", 100 * 3600),
        NoTimeOfDay("Time64(9) as TimeOnly: a nanosecond before midnight", "Time64(9)", -1L),
        NoTimeOfDay("Time64(9) as TimeOnly: the last count that truncates to zero", "Time64(9)", -99L),
        NoTimeOfDay("Time64(9) as TimeOnly: one tick before midnight", "Time64(9)", -100L),
        NoTimeOfDay("Time64(8) as TimeOnly: ten nanoseconds before midnight", "Time64(8)", -1L),
        NoTimeOfDay("Time64(8) as TimeOnly: the last count that truncates to zero", "Time64(8)", -9L),
        NoTimeOfDay("Time64(3) as TimeOnly: a millisecond before midnight", "Time64(3)", -1L),
        NoTimeOfDay("Time64(3) as TimeOnly: exactly 24 hours", "Time64(3)", 86_400_000L),
        NoTimeOfDay("Time64(0) as TimeOnly: exactly 24 hours", "Time64(0)", 86_400L),
        NoTimeOfDay("Time64(0) as TimeOnly: a duration of 100 hours", "Time64(0)", 100 * 3600L),

        // The two accepted ends of a day.
        ColumnReadScenario.Reads("Time64(9) as TimeOnly: midnight", "Time64(9)", new[] { 0L }, TimeOnly.Parse("00:00:00", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(9) as TimeOnly: the last count of the day", "Time64(9)", new[] { 86_399_999_999_999L }, TimeOnly.Parse("23:59:59.9999999", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(3) as TimeOnly: the last count of the day", "Time64(3)", new[] { 86_399_999L }, TimeOnly.Parse("23:59:59.999", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(0) as TimeOnly: the last count of the day", "Time64(0)", new[] { 86_399L }, TimeOnly.Parse("23:59:59", CultureInfo.InvariantCulture)),

        // A NULL stays NULL.
        ColumnReadScenario.Reads<uint?, DateTime?>("Nullable(DateTime('UTC')) as DateTime?", "Nullable(DateTime('UTC'))", new uint?[] { 1_700_000_000, null }, new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc), null),
        ColumnReadScenario.Reads<uint?, DateTimeOffset?>("LowCardinality(Nullable(DateTime('UTC'))) as DateTimeOffset?", "LowCardinality(Nullable(DateTime('UTC')))", new uint?[] { 1_700_000_000, null }, new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero), null),

        // A non-nullable LowCardinality reads its dictionary with the reading of the inner type, with no lifting.
        ColumnReadScenario.Reads("LowCardinality(DateTime('Europe/Berlin')) as DateTimeOffset", "LowCardinality(DateTime('Europe/Berlin'))", new uint[] { 1_700_000_000 }, new DateTimeOffset(2023, 11, 14, 23, 13, 20, TimeSpan.FromHours(1))),

        // An ordinal with no declared member fails and names the type. A column that the server sends has only declared
        // ordinals, but a clear failure is better than a wrong label.
        ColumnReadScenario.Throws<sbyte, string, KeyNotFoundException>("Enum8('a' = -1, 'b' = 127) as string: ordinal 0", "Enum8('a' = -1, 'b' = 127)", 0, "Enum8('a' = -1, 'b' = 127)", "ordinal 0"),
    };

    private static ColumnReadScenario NoTimeOfDay<TSource>(string name, string columnType, TSource value)
        => ColumnReadScenario.Throws<TSource, TimeOnly, InvalidOperationException>(name, columnType, value, "is not a time of day", "TimeSpan");

    private static DateTimeOffset Instant(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);

    private static DifferentialCase TypeOnlyCase(string id, CaseSource source, string columnType, IReadOnlyList<Type> targets)
    {
        Type canonical = Codec(columnType).ElementType;
        var inputs = new List<WriteInput> { WriteInput.Built("canonical", canonical, name => SampleColumns.Build(name, columnType)) };
        inputs.AddRange(targets.Where(t => t != canonical).Select(WriteInput.ReadBack));
        return new DifferentialCase(id, source, columnType, SampleColumns.RowCount, targets, inputs);
    }

    private static IColumnCodec Codec(string columnType) => ColumnCodecRegistry.Default.Resolve(columnType, DifferentialEngine.Context);
}
