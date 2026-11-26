using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers <see cref="IColumnCodec.TryProjectRead"/> — the authority on which readings a codec offers — and the
/// diagnostics-only <see cref="IColumnCodec.ReadableElementTypes"/>. These are API-surface and expression-shape
/// concerns a server round-trip cannot observe: which <see cref="DateTimeKind"/> a projection carries, whether the
/// emitted expression evaluates its source exactly once, and which targets are refused.
/// </summary>
[TestFixture]
public class ColumnReadProjectionTests
{
    private static IColumnCodec Codec(string type, string serverTimezone = "UTC")
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = serverTimezone });

    /// <summary>Compiles a codec's projection into a delegate so its runtime result can be asserted.</summary>
    private static Func<TSource, TTarget> Project<TSource, TTarget>(IColumnCodec codec)
    {
        ParameterExpression source = Expression.Parameter(typeof(TSource), "v");

        Assert.That(codec.TryProjectRead(source, typeof(TTarget), out Expression body), Is.True,
            $"{codec.TypeName} does not project to {typeof(TTarget)}");
        Assert.That(body.Type, Is.EqualTo(typeof(TTarget)), "the projection must yield the requested type");

        return Expression.Lambda<Func<TSource, TTarget>>(body, source).Compile();
    }

    [Test]
    public void ReadableElementTypes_CodecWithoutAlternates_IsJustTheElementType()
    {
        IColumnCodec codec = Codec("UInt64");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(ulong) }));
            Assert.That(codec.ElementType, Is.EqualTo(typeof(ulong)));
        });
    }

    [TestCase("UInt64", typeof(ulong))]
    public void TryProjectRead_CanonicalElementType_IsTheIdentity(string type, Type canonical)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression source = Expression.Parameter(canonical, "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, canonical, out Expression projected), Is.True);
            Assert.That(projected, Is.SameAs(source));
        });
    }

    /// <summary>
    /// A target the codec does not offer is answered, not thrown: the caller decides what an unmappable member means,
    /// and only it knows the column and property names worth naming. A refusal must also leave no projection behind,
    /// so a caller that ignores the bool cannot use a stale one.
    /// </summary>
    [TestCase("UInt64", typeof(DateTime))]
    public void TryProjectRead_TypeNotOffered_ReturnsFalseAndNoProjection(string type, Type unoffered)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression source = Expression.Parameter(codec.ElementType, "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, unoffered, out Expression projected), Is.False);
            Assert.That(projected, Is.Null);
        });
    }

    /// <summary>
    /// A codec with no absence concept refuses a <see cref="Nullable{T}"/> target, even for a reading it offers bare.
    /// This is a gap, not a rule: widening a non-nullable column into a nullable member loses nothing. Closing it
    /// belongs in the caller — one unwrap-and-widen step where <see cref="IColumnCodec.TryProjectRead"/> is consumed,
    /// not a nullable arm in every codec. Pinned so that closing it is a visible change.
    /// </summary>
    [TestCase("UInt64", typeof(ulong?))]
    [TestCase("Date", typeof(DateOnly?))]
    [TestCase("UUID", typeof(Guid?))]
    [TestCase("DateTime('UTC')", typeof(DateTime?))]
    [TestCase("DateTime('UTC')", typeof(DateTimeOffset?))]
    [TestCase("DateTime64(3, 'UTC')", typeof(DateTime?))]
    [TestCase("Time", typeof(TimeSpan?))]
    [TestCase("Time64(3)", typeof(TimeSpan?))]
    public void TryProjectRead_NullableTargetOnCodecWithoutNulls_ReturnsFalse(string type, Type nullableTarget)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression source = Expression.Parameter(codec.ElementType, "v");

        Assert.Multiple(() =>
        {
            Assert.That(
                codec.TryProjectRead(source, Nullable.GetUnderlyingType(nullableTarget), out Expression _), Is.True,
                "the test case must name a reading the codec actually offers bare");

            Assert.That(codec.TryProjectRead(source, nullableTarget, out Expression projected), Is.False);
            Assert.That(projected, Is.Null);
        });
    }

    /// <summary>
    /// A source expression of the wrong type is a caller mistake, distinct from a target the codec does not offer, so
    /// it still throws rather than being reported as "no such projection".
    /// </summary>
    [Test]
    public void TryProjectRead_SourceOfWrongType_ThrowsArgumentException()
    {
        IColumnCodec codec = Codec("DateTime('UTC')");
        ParameterExpression wrong = Expression.Parameter(typeof(long), "v");

        var ex = Assert.Throws<ArgumentException>(
            () => codec.TryProjectRead(wrong, typeof(DateTimeOffset), out Expression _));
        Assert.That(ex.Message, Does.Contain("System.UInt32").And.Contain("System.Int64"));
    }

    /// <summary>
    /// One type of each registered kind. The differential tests read these types as each of their readable
    /// element types.
    /// </summary>
    internal static readonly string[] RegisteredTypes =
    {
        "UInt8", "Int32", "UInt64", "Int128", "Float32", "Float64", "Bool", "String", "FixedString(4)",
        "Date", "Date32", "DateTime", "DateTime('Europe/Berlin')", "DateTime64(3)", "DateTime64(9, 'UTC')",
        "Time", "Time64(3)", "UUID", "IPv4", "IPv6", "Decimal(9, 2)", "Decimal(38, 10)", "Enum8('a' = 1)",
        "Nullable(Int32)", "Nullable(String)", "Nullable(DateTime)", "Nullable(Time64(3))",
        "LowCardinality(String)", "LowCardinality(UInt32)", "LowCardinality(Nullable(DateTime))",
        "Array(Int32)", "Map(String, Int32)", "Tuple(Int32, String)", "Variant(Int32, String)", "Dynamic",
    };

    /// <summary>
    /// <c>Nullable</c> and <c>LowCardinality</c> types over projecting and non-projecting inners. The differential
    /// tests read these types as each of their readable element types.
    /// </summary>
    internal static readonly string[] WrappedTypes =
    {
        "Nullable(DateTime('UTC'))", "Nullable(DateTime64(3, 'UTC'))", "Nullable(Time)", "Nullable(Time64(3))",
        "Nullable(String)", "Nullable(Int32)", "Nullable(UUID)",
        "LowCardinality(String)", "LowCardinality(UInt32)", "LowCardinality(DateTime('UTC'))",
        "LowCardinality(Nullable(String))", "LowCardinality(Nullable(DateTime('UTC')))",
    };

    /// <summary>
    /// Keeps the diagnostic list honest in the one direction that stays true: every type a codec advertises must
    /// actually be projectable, so the failure message a caller is shown never names a reading that does not exist.
    /// The converse is deliberately not asserted — <see cref="IColumnCodec.TryProjectRead"/> is the authority, and a
    /// composite that lifts its children will answer for shapes too numerous to enumerate.
    /// </summary>
    [Test]
    public void ReadableElementTypes_EveryRegisteredType_LeadsWithElementTypeAndIsProjectable()
    {
        Assert.Multiple(() =>
        {
            foreach (string type in RegisteredTypes)
            {
                IColumnCodec codec = Codec(type);
                IReadOnlyList<Type> readable = codec.ReadableElementTypes;

                Assert.That(readable, Is.Not.Empty, $"{type} advertises no readable types");
                Assert.That(readable[0], Is.EqualTo(codec.ElementType), $"{type} must lead with its element type");
                Assert.That(readable.Distinct().Count(), Is.EqualTo(readable.Count), $"{type} lists a duplicate");

                foreach (Type target in readable)
                {
                    try
                    {
                        AssertOffers(codec, target, type);
                    }
                    catch (Exception ex) when (ex is not AssertionException)
                    {
                        Assert.Fail($"{type} advertises {target} but threw projecting it: {ex.Message}");
                    }
                }
            }
        });
    }

    /// <summary>
    /// Pins each projecting codec's readable list to literal types, so dropping a projection fails here rather than
    /// silently agreeing with a matching change to the writable list.
    /// </summary>
    [Test]
    public void ReadableElementTypes_ProjectingCodecs_AreTheExpectedLiteralTypes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Codec("DateTime('UTC')").ReadableElementTypes,
                Is.EqualTo(new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) }));
            Assert.That(
                Codec("DateTime64(3, 'UTC')").ReadableElementTypes,
                Is.EqualTo(new[] { typeof(long), typeof(DateTimeOffset), typeof(DateTime) }));
            Assert.That(Codec("Time").ReadableElementTypes, Is.EqualTo(new[] { typeof(int), typeof(TimeSpan), typeof(TimeOnly) }));
            Assert.That(Codec("Time64(3)").ReadableElementTypes, Is.EqualTo(new[] { typeof(long), typeof(TimeSpan), typeof(TimeOnly) }));
        });
    }

    [Test]
    public void ReadableElementTypes_DateTimeFamily_MirrorsTheWritableList()
    {
        Assert.Multiple(() =>
        {
            foreach (string type in new[] { "DateTime", "DateTime64(3)", "Time", "Time64(3)" })
            {
                IColumnCodec codec = Codec(type);
                Assert.That(
                    codec.ReadableElementTypes,
                    Is.EqualTo(codec.WritableElementTypes),
                    $"{type} should read back every CLR spelling it accepts on write");
            }
        });
    }

    [TestCaseSource(nameof(DateTimeToOffsetScenarios))]
    public void TryProjectRead_DateTimeToOffset_PresentsTheInstantInTheColumnTimezone(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    /// <summary>
    /// Verifies that an unrepresentable timezone fails when a row is projected, not when projection is built.
    /// </summary>
    [TestCaseSource(nameof(ZoneTimeZoneInfoCannotHoldScenarios))]
    public void TryProjectRead_ACalendarTargetOfAZoneTimeZoneInfoCannotHold_BuildsAndThrowsOnTheRow(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [TestCaseSource(nameof(DateTime64ZoneTimeZoneInfoCannotHoldScenarios))]
    public void TryProjectRead_ADateTime64CalendarTargetOfAZoneTimeZoneInfoCannotHold_BuildsAndThrowsOnTheRow(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [TestCaseSource(nameof(DateTimeToUtcDateTimeScenarios))]
    public void TryProjectRead_DateTimeToDateTime_UtcColumnYieldsUtcKind(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    /// <summary>
    /// The <see cref="DateTimeKind"/> rule is chosen to match the HTTP driver's <c>ToDateTime</c>: a non-zero
    /// offset yields the wall clock in the column's timezone as <see cref="DateTimeKind.Unspecified"/>, so a POCO
    /// reading the same column through either client sees the same value.
    /// </summary>
    [TestCaseSource(nameof(DateTimeToWallClockScenarios))]
    public void TryProjectRead_DateTimeToDateTime_OffsetColumnYieldsUnspecifiedWallClock(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [TestCaseSource(nameof(DateTime64ToOffsetScenarios))]
    public void TryProjectRead_DateTime64ToOffset_HonorsTheColumnScale(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [TestCaseSource(nameof(DateTime64ToDateTimeScenarios))]
    public void TryProjectRead_DateTime64ToDateTime_AppliesTheSameKindRuleAsDateTime(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    /// <summary>
    /// A raw count can be decodable yet name an instant outside the .NET calendar. The projection reports that as an
    /// <see cref="OverflowException"/> pointing at the raw values, rather than letting a bare arithmetic exception
    /// escape — the canonical read still returns the exact count.
    /// </summary>
    [TestCaseSource(nameof(BeyondTheCalendarRangeScenarios))]
    public void TryProjectRead_DateTime64BeyondTheCalendarRange_ThrowsOverflowPointingAtTheRawValues(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [Test]
    [TestCase("DateTime('UTC')", typeof(TimeSpan))]
    [TestCase("DateTime64(3, 'UTC')", typeof(TimeSpan))]
    [TestCase("Time", typeof(DateTime))]
    [TestCase("Time64(3)", typeof(DateTime))]
    [TestCase("Nullable(DateTime('UTC'))", typeof(TimeSpan?))]
    [TestCase("LowCardinality(Nullable(DateTime('UTC')))", typeof(TimeSpan?))]
    [TestCase("Enum8('a' = 1)", typeof(int))]
    public void TryProjectRead_ProjectingCodecAskedForAnUnofferedType_ReturnsFalse(string type, Type unoffered)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression source = Expression.Parameter(codec.ElementType, "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ReadableElementTypes, Does.Not.Contain(unoffered));
            Assert.That(codec.TryProjectRead(source, unoffered, out Expression _), Is.False);
        });
    }

    /// <summary>
    /// A nullable surface refuses a bare value-typed target, because it has nowhere to put a null row. Asked for a
    /// plain <c>uint</c>, <c>Nullable(DateTime)</c> must decline rather than silently hand back the inner's
    /// non-nullable reading and drop the nulls.
    /// </summary>
    [Test]
    [TestCase("Nullable(DateTime('UTC'))", typeof(uint))]
    [TestCase("Nullable(DateTime('UTC'))", typeof(DateTime))]
    [TestCase("Nullable(Time64(3))", typeof(TimeSpan))]
    [TestCase("LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTimeOffset))]
    public void TryProjectRead_NullableSurfaceAskedForABareValueType_ReturnsFalse(string type, Type bare)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression source = Expression.Parameter(codec.ElementType, "v");

        Assert.That(codec.TryProjectRead(source, bare, out Expression _), Is.False);
    }

    [TestCaseSource(nameof(TimeToTimeSpanScenarios))]
    public void TryProjectRead_TimeToTimeSpan_IsExactWholeSeconds(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [TestCaseSource(nameof(Time64ToTimeSpanScenarios))]
    public void TryProjectRead_Time64ToTimeSpan_HonorsTheColumnScale(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [TestCaseSource(nameof(TimeToTimeOnlyScenarios))]
    public void TryProjectRead_TimeToTimeOnly_IsTheTimeOfDay(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [TestCaseSource(nameof(Time64ToTimeOnlyScenarios))]
    public void TryProjectRead_Time64ToTimeOnly_HonorsTheColumnScale(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    // TimeOnly cannot represent negative values or durations of at least one day; do not wrap them.
    [TestCaseSource(nameof(TimeOfNoTimeOfDayScenarios))]
    public void TryProjectRead_TimeToTimeOnlyOfAValueThatIsNoTimeOfDay_Throws(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    // Check raw counts because sub-tick negative values truncate to TimeSpan.Zero.
    [TestCaseSource(nameof(Time64OfNoTimeOfDayScenarios))]
    public void TryProjectRead_Time64ToTimeOnlyOfAValueThatIsNoTimeOfDay_Throws(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    // Pin both accepted bounds of a day.
    [TestCaseSource(nameof(Time64AtTheEndsOfTheDayScenarios))]
    public void TryProjectRead_Time64ToTimeOnlyAtTheEndsOfTheDay_IsAccepted(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [Test]
    public void ReadableElementTypes_NullableOfProjectingInner_LiftsEveryInnerType()
    {
        IColumnCodec codec = Codec("Nullable(DateTime('UTC'))");

        Assert.That(
            codec.ReadableElementTypes,
            Is.EqualTo(new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) }));
    }

    [TestCaseSource(nameof(NullableOfDateTimeScenarios))]
    public void TryProjectRead_NullableOfDateTime_ProjectsValueAndPreservesNull(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [Test]
    [TestCase("String")]
    [TestCase("Nullable(String)")]
    [TestCase("LowCardinality(String)")]
    public void ReadableElementTypes_StringShape_OffersTextAndBytes(string type)
    {
        IColumnCodec codec = Codec(type);

        Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(string), typeof(byte[]) }));
    }

    [Test]
    public void ReadableElementTypes_LowCardinalityOfNullableDateTime_LiftsThroughBothWrappers()
    {
        IColumnCodec codec = Codec("LowCardinality(Nullable(DateTime('UTC')))");

        Assert.That(
            codec.ReadableElementTypes,
            Is.EqualTo(new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) }));
    }

    [TestCaseSource(nameof(LowCardinalityOfNullableDateTimeScenarios))]
    public void TryProjectRead_LowCardinalityOfNullableDateTime_ProjectsValueAndPreservesNull(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    [Test]
    public void ReadableElementTypes_LowCardinalityOfNonProjectingInner_IsJustTheInnerType()
    {
        IColumnCodec codec = Codec("LowCardinality(UInt32)");

        Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(uint) }));
    }

    /// <summary>
    /// A non-nullable <c>LowCardinality</c> surfaces the inner type unchanged, so its projection is the inner's own,
    /// applied with no lifting. Exercises the delegation arm that the nullable cases skip.
    /// </summary>
    [TestCaseSource(nameof(LowCardinalityOfProjectingInnerScenarios))]
    public void TryProjectRead_LowCardinalityOfProjectingInner_DelegatesToTheInnerUnlifted(ColumnReadScenario scenario)
    {
        Assert.That(
            Codec(scenario.ColumnType).ReadableElementTypes,
            Is.EqualTo(new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) }));

        AssertProjects(scenario);
    }

    /// <summary>
    /// Every advertised type must project to exactly itself through both wrappers. The wrappers recover the inner
    /// spelling by undoing their own wrap on the target, so this pins that the wrap really is invertible for every
    /// shape the registry produces.
    /// </summary>
    [Test]
    public void TryProjectRead_WrappedCodecs_ProjectEveryAdvertisedTypeToExactlyThatType()
    {
        Assert.Multiple(() =>
        {
            foreach (string type in WrappedTypes)
            {
                IColumnCodec codec = Codec(type);
                foreach (Type target in codec.ReadableElementTypes)
                {
                    AssertOffers(codec, target, type);
                }
            }
        });
    }

    /// <summary>
    /// The lifted projection splices its source in twice (once for the presence test, once for the value), so it must
    /// bind it to a local first. A caller's source is typically a span access or a method call, and evaluating it
    /// twice per row would be a silent correctness and throughput bug that no value-level assertion catches.
    /// </summary>
    [Test]
    public void TryProjectRead_NullableLifting_EvaluatesItsSourceExactlyOnce()
    {
        var counter = new EvaluationCounter();
        IColumnCodec codec = Codec("Nullable(DateTime('UTC'))");

        // A source expression with an observable side effect, standing in for a span access.
        Expression source = Expression.Call(
            Expression.Constant(counter),
            typeof(EvaluationCounter).GetMethod(nameof(EvaluationCounter.Next)));

        Assert.That(codec.TryProjectRead(source, typeof(DateTime?), out Expression projected), Is.True);
        DateTime? result = Expression.Lambda<Func<DateTime?>>(projected).Compile()();

        Assert.Multiple(() =>
        {
            Assert.That(counter.Count, Is.EqualTo(1), "the source expression was evaluated more than once");
            Assert.That(result, Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
        });
    }

    [TestCase("Nullable(Int32)", typeof(long?), typeof(int?))]
    public void TryProjectRead_NullableOfNonProjectingInner_OffersOnlyTheCanonicalType(string type, Type unoffered, Type canonical)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression source = Expression.Parameter(canonical, "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { canonical }));
            Assert.That(codec.TryProjectRead(source, unoffered, out Expression _), Is.False);
        });
    }

    /// <summary>
    /// An enum reads as its raw ordinal or as its label, and writes from either, so the two lists match. The
    /// members come from the type string the column carries, so neither direction needs anything of the server.
    /// </summary>
    [TestCase("Enum8('a' = 1, 'b' = 2)")]
    public void ReadableElementTypes_Enum_OffersTheOrdinalAndTheLabel(string type)
    {
        IColumnCodec codec = Codec(type);

        Assert.Multiple(() =>
        {
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(sbyte), typeof(string) }));
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(codec.WritableElementTypes));
        });
    }

    /// <summary>
    /// Every row of a column read from the server is a declared ordinal, so the projection cannot meet this on a
    /// real read. Pinned anyway: it is the difference between a clear failure and a wrong label.
    /// </summary>
    [TestCaseSource(nameof(EnumOrdinalWithNoDeclaredMemberScenarios))]
    public void TryProjectRead_EnumOrdinalWithNoDeclaredMember_ThrowsNamingTheType(ColumnReadScenario scenario)
        => AssertProjects(scenario);

    /// <summary>
    /// The lifting rule reads source and target shapes independently. No registered pair distinguishes that from
    /// inferring one out of the other, so a stand-in codec supplies the pair that does: a reference-typed element
    /// with a value-typed reading, as <c>FixedString(16)</c> read as a <see cref="Guid"/> would be. Testing such a
    /// source for <c>HasValue</c>, or handing it to the inner unlifted, both answer wrongly here.
    /// </summary>
    [Test]
    public void TryLiftOverAbsent_ReferenceSourceWithValueTypedReading_LiftsAndKeepsAbsentAbsent()
    {
        var inner = new LengthOfStringCodec();
        ParameterExpression source = Expression.Parameter(typeof(string), "v");

        Assert.That(
            ColumnValueProjections.TryLiftOverAbsent(source, inner, typeof(int), typeof(int?), out Expression projected),
            Is.True);
        Assert.That(projected.Type, Is.EqualTo(typeof(int?)));

        Func<string, int?> project = Expression.Lambda<Func<string, int?>>(projected, source).Compile();

        Assert.Multiple(() =>
        {
            Assert.That(project("abcd"), Is.EqualTo(4));
            Assert.That(project(string.Empty), Is.EqualTo(0));
            Assert.That(project(null), Is.Null, "an absent reference row must stay absent, not become default(int)");
        });
    }

    /// <summary>
    /// An absent row short-circuits: the inner projection never runs for it. Worth pinning separately from the value
    /// assertion, because a lift that evaluated the projection eagerly and then discarded it would still return the
    /// right answer while running per-row work — and, for a projection that throws on a null, would not.
    /// </summary>
    [Test]
    public void TryLiftOverAbsent_AbsentRow_DoesNotRunTheInnerProjection()
    {
        var inner = new LengthOfStringCodec();
        ParameterExpression source = Expression.Parameter(typeof(string), "v");

        Assert.That(
            ColumnValueProjections.TryLiftOverAbsent(source, inner, typeof(int), typeof(int?), out Expression projected),
            Is.True);

        Func<string, int?> project = Expression.Lambda<Func<string, int?>>(projected, source).Compile();

        Assert.Multiple(() =>
        {
            Assert.That(project(null), Is.Null);
            Assert.That(inner.Invocations, Is.Zero, "the inner projection must not run for an absent row");

            Assert.That(project("abc"), Is.EqualTo(3));
            Assert.That(inner.Invocations, Is.EqualTo(1), "a present row must run it exactly once");
        });
    }

    /// <summary>A lift over an inner that does not offer the reading is declined, not built.</summary>
    [Test]
    public void TryLiftOverAbsent_InnerDoesNotOfferTheReading_ReturnsFalseAndNoProjection()
    {
        var inner = new LengthOfStringCodec();
        ParameterExpression source = Expression.Parameter(typeof(string), "v");

        Assert.Multiple(() =>
        {
            Assert.That(
                ColumnValueProjections.TryLiftOverAbsent(source, inner, typeof(Guid), typeof(Guid?), out Expression projected),
                Is.False);
            Assert.That(projected, Is.Null);
        });
    }

    /// <summary>
    /// The arm where the surfaced target already is the inner's spelling, so no <c>Convert</c> is spliced in. Reached
    /// only by a reference-typed reading, which every registered type resolves through the wrapper's identity check
    /// instead.
    /// </summary>
    [Test]
    public void TryLiftOverAbsent_TargetAlreadyTheInnerSpelling_SplicesNoConversion()
    {
        var inner = new LengthOfStringCodec();
        ParameterExpression source = Expression.Parameter(typeof(string), "v");

        Assert.That(
            ColumnValueProjections.TryLiftOverAbsent(source, inner, typeof(string), typeof(string), out Expression projected),
            Is.True);
        Assert.That(projected.Type, Is.EqualTo(typeof(string)));

        Func<string, string> project = Expression.Lambda<Func<string, string>>(projected, source).Compile();

        Assert.Multiple(() =>
        {
            Assert.That(project("abc"), Is.EqualTo("abc"));
            Assert.That(project(null), Is.Null);
        });
    }

    /// <summary>
    /// The wrapper's own half of the rule, which no registered type reaches: asked for a value-typed reading over a
    /// <b>reference-typed</b> inner element, <c>Nullable</c> must still lift. Deciding that from the inner's canonical
    /// type — reference-typed here, so "no lift" — returns a bare <c>int</c> where an <c>int?</c> was asked for, and a
    /// null row becomes <c>default(int)</c>. Built over a stand-in via <see cref="NullableColumnCodec.Over"/>.
    /// </summary>
    [Test]
    public void TryProjectRead_NullableOverReferenceInnerWithValueTypedReading_StillLifts()
    {
        IColumnCodec codec = NullableColumnCodec.Over(new LengthOfStringCodec());

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(string)), "a reference inner surfaces unwrapped");
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(string), typeof(int?) }));
        });

        Func<string, int?> project = Project<string, int?>(codec);

        Assert.Multiple(() =>
        {
            Assert.That(project("abcd"), Is.EqualTo(4));
            Assert.That(project(null), Is.Null, "a null row must stay null, not become default(int)");
        });
    }

    /// <summary>
    /// The same shape through <c>LowCardinality(Nullable(T))</c>, whose surface rule is the same wrap and so needs the
    /// same unwrap.
    /// </summary>
    [Test]
    public void TryProjectRead_LowCardinalityNullableOverReferenceInnerWithValueTypedReading_StillLifts()
    {
        IColumnCodec codec = LowCardinalityColumnCodec.Over(new LengthOfStringCodec(), nullable: true);

        Func<string, int?> project = Project<string, int?>(codec);

        Assert.Multiple(() =>
        {
            Assert.That(project("abcd"), Is.EqualTo(4));
            Assert.That(project(null), Is.Null);
        });
    }

    /// <summary>
    /// The reference-typed-target arm of the wrapper's unwrap: a reading that is neither the canonical element type nor
    /// a <see cref="Nullable{T}"/> is passed through as the inner's own spelling. Also unreachable through the
    /// registry, since no registered codec offers a second reference reading.
    /// </summary>
    [Test]
    public void TryProjectRead_NullableAskedForANonCanonicalReferenceReading_PassesTheTargetThrough()
    {
        IColumnCodec codec = NullableColumnCodec.Over(new ReversedStringCodec());

        Func<string, char[]> project = Project<string, char[]>(codec);

        Assert.Multiple(() =>
        {
            Assert.That(project("abc"), Is.EqualTo(new[] { 'c', 'b', 'a' }));
            Assert.That(project(null), Is.Null, "the inner projection must not run on a null reference");
        });
    }

    /// <summary>The same reference-target arm through <c>LowCardinality(Nullable(T))</c>.</summary>
    [Test]
    public void TryProjectRead_LowCardinalityNullableAskedForANonCanonicalReferenceReading_PassesTheTargetThrough()
    {
        IColumnCodec codec = LowCardinalityColumnCodec.Over(new ReversedStringCodec(), nullable: true);

        Func<string, char[]> project = Project<string, char[]>(codec);

        Assert.Multiple(() =>
        {
            Assert.That(project("abc"), Is.EqualTo(new[] { 'c', 'b', 'a' }));
            Assert.That(project(null), Is.Null);
        });
    }

    /// <summary>
    /// The projected view materializes its values into an array of its own on the first <c>Values</c>, so the two
    /// access paths have to agree, and a row past the end has to fail either way round.
    /// </summary>
    [Test]
    public void ReadAs_ProjectedView_AgreesBetweenTheIndexerAndValuesAndBoundsBothWays()
    {
        var ordinals = new ArrayColumn<sbyte>("state", "Enum8('a' = 1, 'b' = 2)", new sbyte[] { 1, 2 });

        IColumn<string> beforeValues = ReadAs<string>(ordinals);
        IColumn<string> afterValues = ReadAs<string>(ordinals);
        _ = afterValues.Values;

        Assert.Multiple(() =>
        {
            Assert.That(beforeValues[1], Is.EqualTo("b"), "read per row, nothing materialized");
            Assert.That(afterValues[1], Is.EqualTo("b"), "read out of the materialized array");
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
    /// Verifies that a projected view preserves source identity and does not dispose the source.
    /// </summary>
    [Test]
    public void ReadAs_ProjectedView_CarriesTheSourcesIdentityAndDisposesNothing()
    {
        var ordinals = new ArrayColumn<sbyte>("state", "Enum8('a' = 1)", new sbyte[] { 1 });

        IColumn<string> projected = ReadAs<string>(ordinals);
        projected.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(projected.Name, Is.EqualTo("state"));
            Assert.That(projected.TypeName, Is.EqualTo("Enum8('a' = 1)"));
            Assert.That(projected.GetValue(0), Is.EqualTo("a"), "the boxed reading is the projected one");
            Assert.That(ordinals.RowCount, Is.EqualTo(1), "the source column is untouched");
            Assert.That(ordinals.Values.ToArray(), Is.EqualTo(new sbyte[] { 1 }));
        });
    }

    [TestCaseSource(nameof(ColumnReadCandidates))]
    public void TryProjectColumnRead_Candidate_ReturnsExpected(string type, Type target, bool expected)
        => Assert.That(OffersColumnRead(Codec(type), target), Is.EqualTo(expected));

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
    public void CanRead_CompositeOfElementwiseChildren_UsesTheValueProjection(string type, Type target)
        => Assert.That(ClickHouseTcpTypes.CanRead(type, target), Is.True);

    /// <summary>
    /// Verifies that JSON remains text-only despite using String serialization.
    /// </summary>
    [Test]
    public void ReadableElementTypes_Json_OffersTextButNotBytes()
    {
        IColumnCodec json = Codec("JSON");

        Assert.Multiple(() =>
        {
            Assert.That(json.ReadableElementTypes, Is.EqualTo(new[] { typeof(string) }));
            Assert.That(ClickHouseTcpTypes.CanRead("JSON", typeof(byte[])), Is.False);
        });
    }

    /// <summary>
    /// Verifies the projected-view cache's single-entry, source-identity behavior.
    /// </summary>
    [Test]
    public void ProjectedViewCache_SameColumnThenAnother_ReusesTheViewThenRebuildsIt()
    {
        int built = 0;
        var cache = new ProjectedViewCache(source =>
        {
            built++;
            return new ProjectedReadColumn<string>(source, static (column, row) => column.Name);
        });

        using var first = new ArrayColumn<string>("a", "String", new[] { "1" });
        using var second = new ArrayColumn<string>("b", "String", new[] { "2" });

        IColumn firstView = cache.For(first);
        IColumn firstAgain = cache.For(first);
        IColumn secondView = cache.For(second);
        IColumn firstAfterEviction = cache.For(first);

        Assert.Multiple(() =>
        {
            Assert.That(firstAgain, Is.SameAs(firstView), "the same column reads through the same view");
            Assert.That(secondView, Is.Not.SameAs(firstView));
            Assert.That(firstAfterEviction, Is.Not.SameAs(firstView), "one entry, so the other column replaced it");
            Assert.That(built, Is.EqualTo(3));
        });
    }

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
    /// Asserts that the codec offers the scenario's reading, and that the compiled projection gives each expected
    /// value or throws the expected exception on each value. Values compare strictly: the same type, the same
    /// floating-point bits, the same <see cref="DateTime.Kind"/> and the same offset.
    /// </summary>
    private static void AssertProjects(ColumnReadScenario scenario)
    {
        IColumnCodec codec = Codec(scenario.ColumnType);
        Assert.That(scenario.Values.GetType().GetElementType(), Is.EqualTo(codec.ElementType), "the scenario's values must be of the codec's element type");

        ParameterExpression source = Expression.Parameter(codec.ElementType, "v");
        Assert.That(codec.TryProjectRead(source, scenario.Target, out Expression body), Is.True, $"{codec.TypeName} does not project to {scenario.Target}");
        Assert.That(body.Type, Is.EqualTo(scenario.Target), "the projection must yield the requested type");

        ParameterExpression boxed = Expression.Parameter(typeof(object), "boxed");
        Func<object, object> project = Expression.Lambda<Func<object, object>>(
            Expression.Block(
                new[] { source },
                Expression.Assign(source, Expression.Convert(boxed, codec.ElementType)),
                Expression.Convert(body, typeof(object))),
            boxed).Compile();

        Assert.Multiple(() =>
        {
            for (int i = 0; i < scenario.Values.Length; i++)
            {
                object value = scenario.Values.GetValue(i);
                if (scenario.ExceptionType is null)
                {
                    Assert.That(ValueComparer.Difference(scenario.Expected.GetValue(i), project(value)), Is.Null, $"value {i}");
                    continue;
                }

                Exception thrown = Assert.Throws(scenario.ExceptionType, () => project(value));
                foreach (string part in scenario.MessageParts)
                {
                    Assert.That(thrown?.Message, Does.Contain(part), $"value {i}");
                }
            }
        });
    }

    internal static IEnumerable<TestCaseData> ColumnReadCandidates()
    {
        yield return ColumnReadCase("String", typeof(byte[]), true);
        yield return ColumnReadCase("Nullable(String)", typeof(byte[]), true);
        yield return ColumnReadCase("LowCardinality(String)", typeof(byte[]), true);
        yield return ColumnReadCase("LowCardinality(Nullable(String))", typeof(byte[]), true);
        yield return ColumnReadCase("Array(String)", typeof(byte[][]), true);
        yield return ColumnReadCase("Array(Array(String))", typeof(byte[][][]), true);
        yield return ColumnReadCase("Map(String, String)", typeof(KeyValuePair<byte[], byte[]>[]), true);
        yield return ColumnReadCase("Map(UInt8, String)", typeof(KeyValuePair<byte, byte[]>[]), true);
        yield return ColumnReadCase("Tuple(String)", typeof(ValueTuple<byte[]>), true);
        yield return ColumnReadCase("Tuple(UInt8, String)", typeof((byte, byte[])), true);
        yield return ColumnReadCase("String", typeof(string), false);
        yield return ColumnReadCase("Nullable(String)", typeof(byte[][]), false);
        yield return ColumnReadCase("Array(String)", typeof(byte[]), false);
        yield return ColumnReadCase("Array(String)", typeof(string), false);
        yield return ColumnReadCase("Array(String)", typeof(string[]), false);
        yield return ColumnReadCase("Map(String, String)", typeof(string), false);
        yield return ColumnReadCase("Map(String, String)", typeof(KeyValuePair<string, string>[]), false);
        yield return ColumnReadCase("Tuple(UInt8, String)", typeof(string), false);
        yield return ColumnReadCase("Tuple(UInt8, String)", typeof((byte, string)), false);
        yield return ColumnReadCase("Nullable(String)", typeof(string), false);
        yield return ColumnReadCase("LowCardinality(String)", typeof(string), false);
        yield return ColumnReadCase("Tuple(UInt8, String)", typeof((long, byte[])), false);
        yield return ColumnReadCase("Map(UInt8, String)", typeof(KeyValuePair<long, byte[]>[]), false);
        yield return ColumnReadCase("Array(String)", typeof(Guid[]), false);
        yield return ColumnReadCase("LowCardinality(String)", typeof(Guid), false);
        yield return ColumnReadCase("Array(DateTime('UTC'))", typeof(DateTime[]), false);
        yield return ColumnReadCase("Tuple(DateTime('UTC'), Time)", typeof((DateTime, TimeSpan)), false);
        yield return ColumnReadCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, DateTime>[]), false);
        yield return ColumnReadCase("Nullable(DateTime('UTC'))", typeof(DateTime?), false);
        yield return ColumnReadCase("LowCardinality(DateTime('UTC'))", typeof(DateTime), true);
        yield return ColumnReadCase("LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTime?), true);
        yield return ColumnReadCase("LowCardinality(FixedString(4))", typeof(string), true);
        yield return ColumnReadCase("LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTime), false);
        yield return ColumnReadCase("JSON", typeof(byte[]), false);
    }

    private static TestCaseData ColumnReadCase(string type, Type target, bool expected)
        => new TestCaseData(type, target, expected).SetArgDisplayNames(type, target.Name, expected.ToString());

    /// <summary>Asks a codec for the reading it takes over the whole column rather than over one decoded value.</summary>
    private static bool OffersColumnRead(IColumnCodec codec, Type targetType)
        => codec.TryProjectColumnRead(targetType, out _);

    /// <summary>
    /// Asserts that a codec offers the advertised target and types elementwise projections correctly.
    /// </summary>
    private static void AssertOffers(IColumnCodec codec, Type target, string type)
    {
        Assert.That(ColumnProjection.Offers(codec, target), Is.True, $"{type} advertises {target} but does not project it");

        if (codec.TryProjectRead(Expression.Parameter(codec.ElementType, "v"), target, out Expression projected))
        {
            Assert.That(projected.Type, Is.EqualTo(target), $"{type} projected {target} as {projected.Type}");
        }
    }

    private static IColumn<T> ReadAs<T>(IColumn column)
        => ColumnCodecRegistry.Default.Projections.ReadAs<T>(column, new ResolveContext { ServerTimezone = "UTC" });

    private static void AssertLacksSurface<T>(string type, string surface)
    {
        using var mislabelled = new ArrayColumn<string>("c", type, new[] { "a" });

        var thrown = Assert.Throws<InvalidOperationException>(() => ReadAs<T>(mislabelled));

        Assert.That(thrown.Message, Does.Contain($"Column 'c' ({type})").And.Contain(surface));
    }

    private sealed class EvaluationCounter
    {
        public int Count { get; private set; }

        public uint? Next()
        {
            Count++;
            return 1_700_000_000;
        }
    }

    /// <summary>
    /// A stand-in codec whose element type is a reference type but which offers a value-typed reading — the shape no
    /// registered codec has yet, and the one the independent source/target lifting rule exists for. Only the read
    /// projection is implemented; nothing else is reached by these tests.
    /// </summary>
    private sealed class LengthOfStringCodec : IColumnCodec
    {
        private static readonly MethodInfo LengthOf =
            typeof(LengthOfStringCodec).GetMethod(nameof(Length), BindingFlags.Public | BindingFlags.Static);

        private static readonly MethodInfo CountOne =
            typeof(LengthOfStringCodec).GetMethod(nameof(Count), BindingFlags.Public | BindingFlags.Instance);

        public int Invocations { get; private set; }

        public string TypeName => "LengthOfString";

        public Type ElementType => typeof(string);

        public IReadOnlyList<Type> ReadableElementTypes => new[] { typeof(string), typeof(int) };

        public object NullPlaceholder => string.Empty;

        public static int Length(string value) => value.Length;

        public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
        {
            ColumnValueProjections.RequireSourceType(value, typeof(string), TypeName);

            if (targetType == typeof(string))
            {
                projected = value;
                return true;
            }

            if (targetType == typeof(int))
            {
                // Routed through an instance counter so a test can assert the projection never runs for an absent row.
                projected = Expression.Block(
                    Expression.Call(Expression.Constant(this), CountOne),
                    Expression.Call(LengthOf, value));
                return true;
            }

            projected = null;
            return false;
        }

        public void Count() => Invocations++;

        public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public bool CanWrite(IColumn column) => false;

        public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
            => throw new NotSupportedException();
    }

    /// <summary>
    /// A second stand-in, offering a <b>reference</b>-typed reading other than its canonical one — the shape that
    /// reaches the wrappers' reference-target arm. Its projection dereferences its argument, so a lift that failed to
    /// guard the null would throw rather than return null.
    /// </summary>
    private sealed class ReversedStringCodec : IColumnCodec
    {
        private static readonly MethodInfo ReverseOf =
            typeof(ReversedStringCodec).GetMethod(nameof(Reverse), BindingFlags.Public | BindingFlags.Static);

        public string TypeName => "ReversedString";

        public Type ElementType => typeof(string);

        public IReadOnlyList<Type> ReadableElementTypes => new[] { typeof(string), typeof(char[]) };

        public object NullPlaceholder => string.Empty;

        public static char[] Reverse(string value)
        {
            char[] chars = value.ToCharArray();
            Array.Reverse(chars);
            return chars;
        }

        public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
        {
            ColumnValueProjections.RequireSourceType(value, typeof(string), TypeName);

            if (targetType == typeof(string))
            {
                projected = value;
                return true;
            }

            if (targetType == typeof(char[]))
            {
                projected = Expression.Call(ReverseOf, value);
                return true;
            }

            projected = null;
            return false;
        }

        public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public bool CanWrite(IColumn column) => false;

        public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
            => throw new NotSupportedException();
    }
}
