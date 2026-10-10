using System;
using System.Collections.Generic;
using System.Globalization;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers the conversions of <see cref="Block.ReadAs{T}(int)"/> that a server round-trip cannot observe: which
/// <see cref="DateTimeKind"/> a calendar reading carries, the scale of a count, the values that have no reading and
/// fail on their row, and the converting view itself. The value and error tests take their data from
/// <see cref="ColumnReadScenario"/> sources, which the differential tests read as cases too.
/// </summary>
[TestFixture]
public class ColumnReadProjectionTests
{
    private static IColumnCodec Codec(string type)
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = "UTC" });

    [TestCaseSource(nameof(DateTimeToOffsetScenarios))]
    public void ReadAs_DateTimeToOffset_PresentsTheInstantInTheColumnTimezone(ColumnReadScenario scenario)
        => AssertReads(scenario);

    /// <summary>
    /// Verifies that an unrepresentable timezone fails when a row is projected, not when projection is built.
    /// </summary>
    [TestCaseSource(nameof(ZoneTimeZoneInfoCannotHoldScenarios))]
    public void ReadAs_ACalendarTargetOfAZoneTimeZoneInfoCannotHold_BuildsAndThrowsOnTheRow(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(DateTime64ZoneTimeZoneInfoCannotHoldScenarios))]
    public void ReadAs_ADateTime64CalendarTargetOfAZoneTimeZoneInfoCannotHold_BuildsAndThrowsOnTheRow(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(DateTimeToUtcDateTimeScenarios))]
    public void ReadAs_DateTimeToDateTime_UtcColumnYieldsUtcKind(ColumnReadScenario scenario)
        => AssertReads(scenario);

    /// <summary>
    /// The <see cref="DateTimeKind"/> rule is chosen to match the HTTP driver's <c>ToDateTime</c>: a non-zero
    /// offset yields the wall clock in the column's timezone as <see cref="DateTimeKind.Unspecified"/>, so a POCO
    /// reading the same column through either client sees the same value.
    /// </summary>
    [TestCaseSource(nameof(DateTimeToWallClockScenarios))]
    public void ReadAs_DateTimeToDateTime_OffsetColumnYieldsUnspecifiedWallClock(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(DateTime64ToOffsetScenarios))]
    public void ReadAs_DateTime64ToOffset_HonorsTheColumnScale(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(DateTime64ToDateTimeScenarios))]
    public void ReadAs_DateTime64ToDateTime_AppliesTheSameKindRuleAsDateTime(ColumnReadScenario scenario)
        => AssertReads(scenario);

    /// <summary>
    /// A raw count can be decodable yet name an instant outside the .NET calendar. The projection reports that as an
    /// <see cref="OverflowException"/> pointing at the raw values, rather than letting a bare arithmetic exception
    /// escape — the canonical read still returns the exact count.
    /// </summary>
    [TestCaseSource(nameof(BeyondTheCalendarRangeScenarios))]
    public void ReadAs_DateTime64BeyondTheCalendarRange_ThrowsOverflowPointingAtTheRawValues(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(TimeToTimeSpanScenarios))]
    public void ReadAs_TimeToTimeSpan_IsExactWholeSeconds(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(Time64ToTimeSpanScenarios))]
    public void ReadAs_Time64ToTimeSpan_HonorsTheColumnScale(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(TimeToTimeOnlyScenarios))]
    public void ReadAs_TimeToTimeOnly_IsTheTimeOfDay(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(Time64ToTimeOnlyScenarios))]
    public void ReadAs_Time64ToTimeOnly_HonorsTheColumnScale(ColumnReadScenario scenario)
        => AssertReads(scenario);

    // TimeOnly cannot represent negative values or durations of at least one day; do not wrap them.
    [TestCaseSource(nameof(TimeOfNoTimeOfDayScenarios))]
    public void ReadAs_TimeToTimeOnlyOfAValueThatIsNoTimeOfDay_Throws(ColumnReadScenario scenario)
        => AssertReads(scenario);

    // Check raw counts because sub-tick negative values truncate to TimeSpan.Zero.
    [TestCaseSource(nameof(Time64OfNoTimeOfDayScenarios))]
    public void ReadAs_Time64ToTimeOnlyOfAValueThatIsNoTimeOfDay_Throws(ColumnReadScenario scenario)
        => AssertReads(scenario);

    // Pin both accepted bounds of a day.
    [TestCaseSource(nameof(Time64AtTheEndsOfTheDayScenarios))]
    public void ReadAs_Time64ToTimeOnlyAtTheEndsOfTheDay_IsAccepted(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(NullableOfDateTimeScenarios))]
    public void ReadAs_NullableOfDateTime_ProjectsValueAndPreservesNull(ColumnReadScenario scenario)
        => AssertReads(scenario);

    [TestCaseSource(nameof(LowCardinalityOfNullableDateTimeScenarios))]
    public void ReadAs_LowCardinalityOfNullableDateTime_ProjectsValueAndPreservesNull(ColumnReadScenario scenario)
        => AssertReads(scenario);

    /// <summary>
    /// A non-nullable <c>LowCardinality</c> reads its dictionary with the inner type's own reading, with no lifting.
    /// </summary>
    [TestCaseSource(nameof(LowCardinalityOfProjectingInnerScenarios))]
    public void ReadAs_LowCardinalityOfProjectingInner_ReadsTheDictionaryWithTheInnerReading(ColumnReadScenario scenario)
        => AssertReads(scenario);

    /// <summary>
    /// Every row of a column read from the server is a declared ordinal, so the projection cannot meet this on a
    /// real read. Pinned anyway: it is the difference between a clear failure and a wrong label.
    /// </summary>
    [TestCaseSource(nameof(EnumOrdinalWithNoDeclaredMemberScenarios))]
    public void ReadAs_EnumOrdinalWithNoDeclaredMember_ThrowsNamingTheType(ColumnReadScenario scenario)
        => AssertReads(scenario);

    /// <summary>
    /// The converting view converts the whole column on its first access, through the indexer or through
    /// <c>Values</c>, so the two access paths have to agree, and a row past the end has to fail either way round.
    /// </summary>
    [Test]
    public void ReadAs_ConvertingView_AgreesBetweenTheIndexerAndValuesAndBoundsBothWays()
    {
        var ordinals = new ArrayColumn<sbyte>("state", "Enum8('a' = 1, 'b' = 2)", new sbyte[] { 1, 2 });

        IColumn<string> beforeValues = ReadAs<string>(ordinals);
        IColumn<string> afterValues = ReadAs<string>(ordinals);
        _ = afterValues.Values;

        Assert.Multiple(() =>
        {
            Assert.That(beforeValues[1], Is.EqualTo("b"), "first access through the indexer");
            Assert.That(afterValues[1], Is.EqualTo("b"), "first access through Values");
            Assert.That(beforeValues.Values.ToArray(), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(beforeValues.RowCount, Is.EqualTo(2));
            Assert.Throws<IndexOutOfRangeException>(() => _ = beforeValues[2]);
            Assert.Throws<IndexOutOfRangeException>(() => _ = afterValues[2]);
        });
    }

    /// <summary>
    /// A column built by a caller for an insert carries no type string, so there is nothing to resolve a reading
    /// from. Not reachable through a <see cref="Block"/>, whose columns all come off a header.
    /// </summary>
    [Test]
    public void ReadAs_ColumnWithNoTypeString_SaysSoRatherThanFailingToParseIt()
    {
        IColumn<int> built = ClickHouseTcpColumn.Create("v", new[] { 1, 2 });

        Assert.Multiple(() =>
        {
            Assert.That(ReadAs<int>(built), Is.SameAs(built), "the requested type is the column's own, so nothing is resolved");

            var thrown = Assert.Throws<InvalidCastException>(() => ReadAs<long>(built));
            Assert.That(thrown.Message, Does.Contain("carries no ClickHouse type").And.Contain("System.Int32"));
        });
    }

    /// <summary>
    /// Verifies that a converting view preserves source identity and does not dispose the source.
    /// </summary>
    [Test]
    public void ReadAs_ConvertingView_CarriesTheSourcesIdentityAndDisposesNothing()
    {
        var ordinals = new ArrayColumn<sbyte>("state", "Enum8('a' = 1)", new sbyte[] { 1 });

        IColumn<string> projected = ReadAs<string>(ordinals);
        projected.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(projected.Name, Is.EqualTo("state"));
            Assert.That(projected.TypeName, Is.EqualTo("Enum8('a' = 1)"));
            Assert.That(projected.GetValue(0), Is.EqualTo("a"), "the boxed reading is the converted one");
            Assert.That(ordinals.RowCount, Is.EqualTo(1), "the source column is untouched");
            Assert.That(ordinals.Values.ToArray(), Is.EqualTo(new sbyte[] { 1 }));
        });
    }

    /// <summary>
    /// Verifies the error when a caller-built composite lacks its decoded columnar surface.
    /// </summary>
    [Test]
    public void ReadAs_CompositeColumnWithoutItsColumnarSurface_SaysWhichSurfaceItLacks()
    {
        Assert.Multiple(() =>
        {
            AssertLacksSurface<byte[]>("Nullable(String)", "INullableColumn");
            AssertLacksSurface<byte[][]>("Array(String)", "IArrayColumn");
            AssertLacksSurface<KeyValuePair<string, byte[]>[]>("Map(String, String)", "IMapColumn");
            AssertLacksSurface<ValueTuple<byte[]>>("Tuple(String)", "ITupleColumn");
            AssertLacksSurface<byte[]>("LowCardinality(String)", "ILowCardinalityColumn");
        });
    }

    [Test]
    [TestCase("Array(DateTime('UTC'))", typeof(DateTime[]))]
    [TestCase("Tuple(DateTime('UTC'), Time)", typeof((DateTime, TimeSpan)))]
    [TestCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, DateTime>[]))]
    [TestCase("Nullable(DateTime('UTC'))", typeof(DateTime?))]
    public void CanRead_CompositeOfConvertingChildren_IsTrue(string type, Type target)
        => Assert.That(ClickHouseTcpTypes.CanRead(type, target), Is.True);

    /// <summary>
    /// Verifies that JSON remains text-only despite using String serialization.
    /// </summary>
    [Test]
    public void CanRead_JsonAsBytes_IsFalse()
        => Assert.That(ClickHouseTcpTypes.CanRead("JSON", typeof(byte[])), Is.False);

    // 1700000000 = 2023-11-14T22:13:20Z, which is 23:13:20 +01:00 in Berlin (winter, no DST).
    internal static IEnumerable<ColumnReadScenario> DateTimeToOffsetScenarios() => new[]
    {
        ColumnReadScenario.Reads("DateTime('Europe/Berlin') as DateTimeOffset", "DateTime('Europe/Berlin')", new uint[] { 1_700_000_000 }, new DateTimeOffset(2023, 11, 14, 23, 13, 20, TimeSpan.FromHours(1))),
    };

    internal static IEnumerable<ColumnReadScenario> ZoneTimeZoneInfoCannotHoldScenarios() => new[]
    {
        ColumnReadScenario.Throws<uint, DateTimeOffset, FormatException>("DateTime('Fixed/UTC+19:00:00') as DateTimeOffset", "DateTime('Fixed/UTC+19:00:00')", 1_700_000_000, "+19:00:00"),
        ColumnReadScenario.Throws<uint, DateTimeOffset, FormatException>("DateTime('Fixed/UTC+05:30:15') as DateTimeOffset", "DateTime('Fixed/UTC+05:30:15')", 1_700_000_000, "+05:30:15"),
    };

    internal static IEnumerable<ColumnReadScenario> DateTime64ZoneTimeZoneInfoCannotHoldScenarios() => new[]
    {
        ColumnReadScenario.Throws<long, DateTimeOffset, FormatException>("DateTime64(3, 'Fixed/UTC+19:00:00') as DateTimeOffset", "DateTime64(3, 'Fixed/UTC+19:00:00')", 1_700_000_000_000, "+19:00:00"),
        ColumnReadScenario.Throws<long, DateTimeOffset, FormatException>("DateTime64(9, 'Fixed/UTC+05:30:15') as DateTimeOffset", "DateTime64(9, 'Fixed/UTC+05:30:15')", 1_700_000_000_000, "+05:30:15"),
    };

    internal static IEnumerable<ColumnReadScenario> DateTimeToUtcDateTimeScenarios() => new[]
    {
        ColumnReadScenario.Reads("DateTime('UTC') as DateTime", "DateTime('UTC')", new uint[] { 1_700_000_000 }, new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)),
    };

    internal static IEnumerable<ColumnReadScenario> DateTimeToWallClockScenarios() => new[]
    {
        ColumnReadScenario.Reads("DateTime('Europe/Berlin') as DateTime", "DateTime('Europe/Berlin')", new uint[] { 1_700_000_000 }, new DateTime(2023, 11, 14, 23, 13, 20, DateTimeKind.Unspecified)),
    };

    // Scale 9 is finer than a .NET tick, so the sub-100 ns digits truncate toward zero.
    internal static IEnumerable<ColumnReadScenario> DateTime64ToOffsetScenarios() => new[]
    {
        ColumnReadScenario.Reads("DateTime64(3, 'UTC') as DateTimeOffset", "DateTime64(3, 'UTC')", new[] { 1_700_000_000_123L }, Instant("2023-11-14T22:13:20.1230000Z")),
        ColumnReadScenario.Reads("DateTime64(9, 'UTC') as DateTimeOffset", "DateTime64(9, 'UTC')", new[] { 1_700_000_000_123_456_789L }, Instant("2023-11-14T22:13:20.1234567Z")),
        ColumnReadScenario.Reads("DateTime64(0, 'UTC') as DateTimeOffset", "DateTime64(0, 'UTC')", new[] { 1_700_000_000L }, Instant("2023-11-14T22:13:20.0000000Z")),
    };

    internal static IEnumerable<ColumnReadScenario> DateTime64ToDateTimeScenarios() => new[]
    {
        ColumnReadScenario.Reads("DateTime64(3, 'UTC') as DateTime", "DateTime64(3, 'UTC')", new[] { 1_700_000_000_123L }, new DateTime(2023, 11, 14, 22, 13, 20, 123, DateTimeKind.Utc)),
        ColumnReadScenario.Reads("DateTime64(3, 'Europe/Berlin') as DateTime", "DateTime64(3, 'Europe/Berlin')", new[] { 1_700_000_000_123L }, new DateTime(2023, 11, 14, 23, 13, 20, 123, DateTimeKind.Unspecified)),
    };

    internal static IEnumerable<ColumnReadScenario> BeyondTheCalendarRangeScenarios() => new[]
    {
        ColumnReadScenario.Throws<long, DateTimeOffset, OverflowException>("DateTime64(0, 'UTC') as DateTimeOffset: beyond the calendar range", "DateTime64(0, 'UTC')", long.MaxValue, "Values"),
    };

    internal static IEnumerable<ColumnReadScenario> TimeToTimeSpanScenarios() => new[]
    {
        ColumnReadScenario.Reads("Time as TimeSpan", "Time", new[] { 3661, -3661, 0 }, new TimeSpan(1, 1, 1), new TimeSpan(1, 1, 1).Negate(), TimeSpan.Zero),
    };

    internal static IEnumerable<ColumnReadScenario> Time64ToTimeSpanScenarios() => new[]
    {
        ColumnReadScenario.Reads("Time64(3) as TimeSpan", "Time64(3)", new[] { 3_661_500L }, TimeSpan.Parse("01:01:01.5000000", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(9) as TimeSpan", "Time64(9)", new[] { -1_000_000_001L }, TimeSpan.Parse("-00:00:01.0000000", CultureInfo.InvariantCulture)),
    };

    internal static IEnumerable<ColumnReadScenario> TimeToTimeOnlyScenarios() => new[]
    {
        ColumnReadScenario.Reads("Time as TimeOnly", "Time", new[] { 3661, 0, (23 * 3600) + (59 * 60) + 59 }, new TimeOnly(1, 1, 1), TimeOnly.MinValue, new TimeOnly(23, 59, 59)),
    };

    internal static IEnumerable<ColumnReadScenario> Time64ToTimeOnlyScenarios() => new[]
    {
        ColumnReadScenario.Reads("Time64(3) as TimeOnly", "Time64(3)", new[] { 3_661_500L }, TimeOnly.Parse("01:01:01.5000000", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(9) as TimeOnly", "Time64(9)", new[] { 3_661_000_000_000L }, TimeOnly.Parse("01:01:01", CultureInfo.InvariantCulture)),
    };

    // The message has to name the reading that does work, TimeSpan.
    internal static IEnumerable<ColumnReadScenario> TimeOfNoTimeOfDayScenarios() => new[]
    {
        NoTimeOfDay("Time as TimeOnly: a negative duration", "Time", -1),
        NoTimeOfDay("Time as TimeOnly: exactly 24 hours", "Time", 24 * 3600),
        NoTimeOfDay("Time as TimeOnly: a duration of 100 hours", "Time", 100 * 3600),
    };

    internal static IEnumerable<ColumnReadScenario> Time64OfNoTimeOfDayScenarios() => new[]
    {
        NoTimeOfDay("Time64(9) as TimeOnly: a nanosecond before midnight", "Time64(9)", -1L),
        NoTimeOfDay("Time64(9) as TimeOnly: the last count that truncates to zero", "Time64(9)", -99L),
        NoTimeOfDay("Time64(9) as TimeOnly: one tick before midnight", "Time64(9)", -100L),
        NoTimeOfDay("Time64(8) as TimeOnly: ten nanoseconds before midnight", "Time64(8)", -1L),
        NoTimeOfDay("Time64(8) as TimeOnly: the last count that truncates to zero", "Time64(8)", -9L),
        NoTimeOfDay("Time64(3) as TimeOnly: a millisecond before midnight", "Time64(3)", -1L),
        NoTimeOfDay("Time64(3) as TimeOnly: exactly 24 hours", "Time64(3)", 86_400_000L),
        NoTimeOfDay("Time64(0) as TimeOnly: exactly 24 hours", "Time64(0)", 86_400L),
        NoTimeOfDay("Time64(0) as TimeOnly: a duration of 100 hours", "Time64(0)", 100 * 3600L),
    };

    internal static IEnumerable<ColumnReadScenario> Time64AtTheEndsOfTheDayScenarios() => new[]
    {
        ColumnReadScenario.Reads("Time64(9) as TimeOnly: midnight", "Time64(9)", new[] { 0L }, TimeOnly.Parse("00:00:00", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(9) as TimeOnly: the last count of the day", "Time64(9)", new[] { 86_399_999_999_999L }, TimeOnly.Parse("23:59:59.9999999", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(3) as TimeOnly: the last count of the day", "Time64(3)", new[] { 86_399_999L }, TimeOnly.Parse("23:59:59.999", CultureInfo.InvariantCulture)),
        ColumnReadScenario.Reads("Time64(0) as TimeOnly: the last count of the day", "Time64(0)", new[] { 86_399L }, TimeOnly.Parse("23:59:59", CultureInfo.InvariantCulture)),
    };

    internal static IEnumerable<ColumnReadScenario> NullableOfDateTimeScenarios() => new[]
    {
        ColumnReadScenario.Reads<uint?, DateTime?>("Nullable(DateTime('UTC')) as DateTime?", "Nullable(DateTime('UTC'))", new uint?[] { 1_700_000_000, null }, new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc), null),
    };

    internal static IEnumerable<ColumnReadScenario> LowCardinalityOfNullableDateTimeScenarios() => new[]
    {
        ColumnReadScenario.Reads<uint?, DateTimeOffset?>("LowCardinality(Nullable(DateTime('UTC'))) as DateTimeOffset?", "LowCardinality(Nullable(DateTime('UTC')))", new uint?[] { 1_700_000_000, null }, new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero), null),
    };

    internal static IEnumerable<ColumnReadScenario> LowCardinalityOfProjectingInnerScenarios() => new[]
    {
        ColumnReadScenario.Reads("LowCardinality(DateTime('Europe/Berlin')) as DateTimeOffset", "LowCardinality(DateTime('Europe/Berlin'))", new uint[] { 1_700_000_000 }, new DateTimeOffset(2023, 11, 14, 23, 13, 20, TimeSpan.FromHours(1))),
    };

    internal static IEnumerable<ColumnReadScenario> EnumOrdinalWithNoDeclaredMemberScenarios() => new[]
    {
        ColumnReadScenario.Throws<sbyte, string, KeyNotFoundException>("Enum8('a' = -1, 'b' = 127) as string: ordinal 0", "Enum8('a' = -1, 'b' = 127)", 0, "Enum8('a' = -1, 'b' = 127)", "ordinal 0"),
    };

    private static ColumnReadScenario NoTimeOfDay<TSource>(string name, string columnType, TSource value)
        => ColumnReadScenario.Throws<TSource, TimeOnly, InvalidOperationException>(name, columnType, value, "is not a time of day", "TimeSpan");

    private static DateTimeOffset Instant(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);

    /// <summary>
    /// Asserts that <see cref="Block.ReadAs{T}(int)"/> of a decoded column of each value alone gives the expected value,
    /// or throws the expected exception. Values compare strictly: the same type, the same floating-point bits, the same
    /// <see cref="DateTime.Kind"/> and the same offset.
    /// </summary>
    private static void AssertReads(ColumnReadScenario scenario)
    {
        Type canonical = Codec(scenario.ColumnType).ElementType;
        Assert.That(scenario.Values.GetType().GetElementType(), Is.EqualTo(canonical), "the scenario's values must be of the canonical type of the column type");

        Assert.Multiple(() =>
        {
            for (int i = 0; i < scenario.Values.Length; i++)
            {
                Array one = Array.CreateInstance(canonical, 1);
                one.SetValue(scenario.Values.GetValue(i), 0);
                var source = (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(canonical), "v", scenario.ColumnType, one);
                using Block block = ReadRulesTests.Decode(scenario.ColumnType, source);
                object Read() => ConverterHarness.InvokeGeneric(typeof(ColumnReadProjectionTests), nameof(ReadFirst), new[] { scenario.Target }, block);
                if (scenario.ExceptionType is null)
                {
                    Assert.That(ValueComparer.Difference(scenario.Expected.GetValue(i), Read()), Is.Null, $"value {i}");
                    continue;
                }

                Exception thrown = Assert.Throws(scenario.ExceptionType, () => Read());
                foreach (string part in scenario.MessageParts)
                {
                    Assert.That(thrown?.Message, Does.Contain(part), $"value {i}");
                }
            }
        });
    }

    private static T ReadFirst<T>(Block block) => block.ReadAs<T>(0)[0];

    private static IColumn<T> ReadAs<T>(IColumn column)
        => ColumnCodecRegistry.Default.Projections.ReadAs<T>(column, new ResolveContext { ServerTimezone = "UTC" });

    private static void AssertLacksSurface<T>(string type, string surface)
    {
        using var mislabelled = new ArrayColumn<string>("c", type, new[] { "a" });

        var thrown = Assert.Throws<InvalidOperationException>(() => ReadAs<T>(mislabelled));

        Assert.That(thrown.Message, Does.Contain($"Column 'c' ({type})").And.Contain(surface));
    }
}
