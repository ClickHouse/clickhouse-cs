using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
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

    [Test]
    public void TryProjectRead_CanonicalElementType_IsTheIdentity()
    {
        IColumnCodec codec = Codec("UInt64");
        ParameterExpression source = Expression.Parameter(typeof(ulong), "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, typeof(ulong), out Expression projected), Is.True);
            Assert.That(projected, Is.SameAs(source));
        });
    }

    /// <summary>
    /// A target the codec does not offer is answered, not thrown: the caller decides what an unmappable member means,
    /// and only it knows the column and property names worth naming. A refusal must also leave no projection behind,
    /// so a caller that ignores the bool cannot use a stale one.
    /// </summary>
    [Test]
    public void TryProjectRead_TypeNotOffered_ReturnsFalseAndNoProjection()
    {
        IColumnCodec codec = Codec("UInt64");
        ParameterExpression source = Expression.Parameter(typeof(ulong), "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, typeof(DateTime), out Expression projected), Is.False);
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
    /// Keeps the diagnostic list honest in the one direction that stays true: every type a codec advertises must
    /// actually be projectable, so the failure message a caller is shown never names a reading that does not exist.
    /// The converse is deliberately not asserted — <see cref="IColumnCodec.TryProjectRead"/> is the authority, and a
    /// composite that lifts its children will answer for shapes too numerous to enumerate.
    /// </summary>
    [Test]
    public void ReadableElementTypes_EveryRegisteredType_LeadsWithElementTypeAndIsProjectable()
    {
        string[] types =
        {
            "UInt8", "Int32", "UInt64", "Int128", "Float32", "Float64", "Bool", "String", "FixedString(4)",
            "Date", "Date32", "DateTime", "DateTime('Europe/Berlin')", "DateTime64(3)", "DateTime64(9, 'UTC')",
            "Time", "Time64(3)", "UUID", "IPv4", "IPv6", "Decimal(9, 2)", "Decimal(38, 10)", "Enum8('a' = 1)",
            "Nullable(Int32)", "Nullable(String)", "Nullable(DateTime)", "Nullable(Time64(3))",
            "LowCardinality(String)", "LowCardinality(UInt32)", "LowCardinality(Nullable(DateTime))",
            "Array(Int32)", "Map(String, Int32)", "Tuple(Int32, String)", "Variant(Int32, String)", "Dynamic",
        };

        Assert.Multiple(() =>
        {
            foreach (string type in types)
            {
                IColumnCodec codec = Codec(type);
                IReadOnlyList<Type> readable = codec.ReadableElementTypes;

                Assert.That(readable, Is.Not.Empty, $"{type} advertises no readable types");
                Assert.That(readable[0], Is.EqualTo(codec.ElementType), $"{type} must lead with its element type");
                Assert.That(readable.Distinct().Count(), Is.EqualTo(readable.Count), $"{type} lists a duplicate");

                foreach (Type target in readable)
                {
                    ParameterExpression source = Expression.Parameter(codec.ElementType, "v");
                    Expression projected = null;
                    bool offered = false;
                    try
                    {
                        offered = codec.TryProjectRead(source, target, out projected);
                    }
                    catch (Exception ex)
                    {
                        Assert.Fail($"{type} advertises {target} but threw projecting it: {ex.Message}");
                    }

                    Assert.That(offered, Is.True, $"{type} advertises {target} but does not project it");
                    Assert.That(projected?.Type, Is.EqualTo(target), $"{type} projected {target} as {projected?.Type}");
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
            Assert.That(Codec("Time").ReadableElementTypes, Is.EqualTo(new[] { typeof(int), typeof(TimeSpan) }));
            Assert.That(Codec("Time64(3)").ReadableElementTypes, Is.EqualTo(new[] { typeof(long), typeof(TimeSpan) }));
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

    [Test]
    public void TryProjectRead_DateTimeToOffset_PresentsTheInstantInTheColumnTimezone()
    {
        // 1700000000 = 2023-11-14T22:13:20Z, which is 23:13:20 +01:00 in Berlin (winter, no DST).
        Func<uint, DateTimeOffset> project = Project<uint, DateTimeOffset>(Codec("DateTime('Europe/Berlin')"));

        DateTimeOffset result = project(1_700_000_000);

        Assert.Multiple(() =>
        {
            Assert.That(result.UtcDateTime, Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
            Assert.That(result.Offset, Is.EqualTo(TimeSpan.FromHours(1)));
        });
    }

    [Test]
    public void TryProjectRead_DateTimeToDateTime_UtcColumnYieldsUtcKind()
    {
        Func<uint, DateTime> project = Project<uint, DateTime>(Codec("DateTime('UTC')"));

        DateTime result = project(1_700_000_000);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20)));
            Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Utc));
        });
    }

    /// <summary>
    /// The <see cref="DateTimeKind"/> rule is chosen to match the HTTP driver's <c>ToDateTime</c>: a non-zero
    /// offset yields the wall clock in the column's timezone as <see cref="DateTimeKind.Unspecified"/>, so a POCO
    /// reading the same column through either client sees the same value.
    /// </summary>
    [Test]
    public void TryProjectRead_DateTimeToDateTime_OffsetColumnYieldsUnspecifiedWallClock()
    {
        Func<uint, DateTime> project = Project<uint, DateTime>(Codec("DateTime('Europe/Berlin')"));

        DateTime result = project(1_700_000_000);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(new DateTime(2023, 11, 14, 23, 13, 20)));
            Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Unspecified));
        });
    }

    [Test]
    [TestCase(3, 1_700_000_000_123L, "2023-11-14T22:13:20.1230000Z")]
    [TestCase(9, 1_700_000_000_123_456_789L, "2023-11-14T22:13:20.1234567Z")]
    [TestCase(0, 1_700_000_000L, "2023-11-14T22:13:20.0000000Z")]
    public void TryProjectRead_DateTime64ToOffset_HonorsTheColumnScale(int scale, long count, string expected)
    {
        Func<long, DateTimeOffset> project = Project<long, DateTimeOffset>(Codec($"DateTime64({scale}, 'UTC')"));

        // Scale 9 is finer than a .NET tick, so the sub-100 ns digits truncate toward zero.
        Assert.That(project(count).UtcDateTime, Is.EqualTo(DateTimeOffset.Parse(expected).UtcDateTime));
    }

    [Test]
    public void TryProjectRead_DateTime64ToDateTime_AppliesTheSameKindRuleAsDateTime()
    {
        Func<long, DateTime> utc = Project<long, DateTime>(Codec("DateTime64(3, 'UTC')"));
        Func<long, DateTime> berlin = Project<long, DateTime>(Codec("DateTime64(3, 'Europe/Berlin')"));

        Assert.Multiple(() =>
        {
            Assert.That(utc(1_700_000_000_123L), Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, 123)));
            Assert.That(utc(1_700_000_000_123L).Kind, Is.EqualTo(DateTimeKind.Utc));

            Assert.That(berlin(1_700_000_000_123L), Is.EqualTo(new DateTime(2023, 11, 14, 23, 13, 20, 123)));
            Assert.That(berlin(1_700_000_000_123L).Kind, Is.EqualTo(DateTimeKind.Unspecified));
        });
    }

    /// <summary>
    /// A raw count can be decodable yet name an instant outside the .NET calendar. The projection reports that as an
    /// <see cref="OverflowException"/> pointing at the raw values, rather than letting a bare arithmetic exception
    /// escape — the canonical read still returns the exact count.
    /// </summary>
    [Test]
    public void TryProjectRead_DateTime64BeyondTheCalendarRange_ThrowsOverflowPointingAtTheRawValues()
    {
        Func<long, DateTimeOffset> project = Project<long, DateTimeOffset>(Codec("DateTime64(0, 'UTC')"));

        var ex = Assert.Throws<OverflowException>(() => project(long.MaxValue));
        Assert.That(ex.Message, Does.Contain("Values"));
    }

    [Test]
    [TestCase("DateTime('UTC')", typeof(TimeSpan))]
    [TestCase("DateTime64(3, 'UTC')", typeof(TimeSpan))]
    [TestCase("Time", typeof(DateTime))]
    [TestCase("Time64(3)", typeof(DateTime))]
    [TestCase("Nullable(DateTime('UTC'))", typeof(TimeSpan?))]
    [TestCase("LowCardinality(Nullable(DateTime('UTC')))", typeof(TimeSpan?))]
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

    [Test]
    public void TryProjectRead_TimeToTimeSpan_IsExactWholeSeconds()
    {
        Func<int, TimeSpan> project = Project<int, TimeSpan>(Codec("Time"));

        Assert.Multiple(() =>
        {
            Assert.That(project(3661), Is.EqualTo(new TimeSpan(1, 1, 1)));
            Assert.That(project(-3661), Is.EqualTo(new TimeSpan(1, 1, 1).Negate()));
            Assert.That(project(0), Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    [TestCase(3, 3_661_500L, "01:01:01.5000000")]
    [TestCase(9, -1_000_000_001L, "-00:00:01.0000000")]
    public void TryProjectRead_Time64ToTimeSpan_HonorsTheColumnScale(int scale, long count, string expected)
    {
        Func<long, TimeSpan> project = Project<long, TimeSpan>(Codec($"Time64({scale})"));

        Assert.That(project(count), Is.EqualTo(TimeSpan.Parse(expected)));
    }

    [Test]
    public void ReadableElementTypes_NullableOfProjectingInner_LiftsEveryInnerType()
    {
        IColumnCodec codec = Codec("Nullable(DateTime('UTC'))");

        Assert.That(
            codec.ReadableElementTypes,
            Is.EqualTo(new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) }));
    }

    [Test]
    public void TryProjectRead_NullableOfDateTime_ProjectsValueAndPreservesNull()
    {
        Func<uint?, DateTime?> project = Project<uint?, DateTime?>(Codec("Nullable(DateTime('UTC'))"));

        Assert.Multiple(() =>
        {
            Assert.That(project(1_700_000_000), Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
            Assert.That(project(null), Is.Null);
        });
    }

    [Test]
    public void ReadableElementTypes_NullableOfReferenceInner_StaysUnwrapped()
    {
        IColumnCodec codec = Codec("Nullable(String)");

        // A reference inner's nulls are already CLR nulls, so the surface type is the bare inner type.
        Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(string) }));
    }

    [Test]
    public void ReadableElementTypes_LowCardinalityOfNullableDateTime_LiftsThroughBothWrappers()
    {
        IColumnCodec codec = Codec("LowCardinality(Nullable(DateTime('UTC')))");

        Assert.That(
            codec.ReadableElementTypes,
            Is.EqualTo(new[] { typeof(uint?), typeof(DateTimeOffset?), typeof(DateTime?) }));
    }

    [Test]
    public void TryProjectRead_LowCardinalityOfNullableDateTime_ProjectsValueAndPreservesNull()
    {
        Func<uint?, DateTimeOffset?> project =
            Project<uint?, DateTimeOffset?>(Codec("LowCardinality(Nullable(DateTime('UTC')))"));

        Assert.Multiple(() =>
        {
            Assert.That(project(1_700_000_000)?.UtcDateTime, Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
            Assert.That(project(null), Is.Null);
        });
    }

    [Test]
    public void ReadableElementTypes_LowCardinalityOfNonProjectingInner_IsJustTheInnerType()
    {
        IColumnCodec codec = Codec("LowCardinality(String)");

        Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(string) }));
    }

    /// <summary>
    /// A non-nullable <c>LowCardinality</c> surfaces the inner type unchanged, so its projection is the inner's own,
    /// applied with no lifting. Exercises the delegation arm that the nullable cases skip.
    /// </summary>
    [Test]
    public void TryProjectRead_LowCardinalityOfProjectingInner_DelegatesToTheInnerUnlifted()
    {
        IColumnCodec codec = Codec("LowCardinality(DateTime('Europe/Berlin'))");

        Assert.Multiple(() =>
        {
            Assert.That(
                codec.ReadableElementTypes,
                Is.EqualTo(new[] { typeof(uint), typeof(DateTimeOffset), typeof(DateTime) }));

            Func<uint, DateTimeOffset> project = Project<uint, DateTimeOffset>(codec);
            Assert.That(project(1_700_000_000).Offset, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(
                project(1_700_000_000).UtcDateTime,
                Is.EqualTo(new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc)));
        });
    }

    /// <summary>
    /// Every advertised type must project to exactly itself through both wrappers. The wrappers recover the inner
    /// spelling by undoing their own wrap on the target, so this pins that the wrap really is invertible for every
    /// shape the registry produces.
    /// </summary>
    [Test]
    public void TryProjectRead_WrappedCodecs_ProjectEveryAdvertisedTypeToExactlyThatType()
    {
        string[] wrapped =
        {
            "Nullable(DateTime('UTC'))", "Nullable(DateTime64(3, 'UTC'))", "Nullable(Time)", "Nullable(Time64(3))",
            "Nullable(String)", "Nullable(Int32)", "Nullable(UUID)",
            "LowCardinality(String)", "LowCardinality(UInt32)", "LowCardinality(DateTime('UTC'))",
            "LowCardinality(Nullable(String))", "LowCardinality(Nullable(DateTime('UTC')))",
        };

        Assert.Multiple(() =>
        {
            foreach (string type in wrapped)
            {
                IColumnCodec codec = Codec(type);
                foreach (Type target in codec.ReadableElementTypes)
                {
                    ParameterExpression source = Expression.Parameter(codec.ElementType, "v");
                    Assert.That(codec.TryProjectRead(source, target, out Expression projected), Is.True,
                        $"{type} advertises {target} but does not project it");
                    Assert.That(projected.Type, Is.EqualTo(target), $"{type} projected {target} as {projected.Type}");
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

    [Test]
    public void TryProjectRead_NullableOfNonProjectingInner_OffersOnlyTheCanonicalType()
    {
        IColumnCodec codec = Codec("Nullable(Int32)");
        ParameterExpression source = Expression.Parameter(typeof(int?), "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(int?) }));
            Assert.That(codec.TryProjectRead(source, typeof(long?), out Expression _), Is.False);
        });
    }

    /// <summary>
    /// An enum reads as its raw ordinal or as its label, and writes from either, so the two lists match. The
    /// members come from the type string the column carries, so neither direction needs anything of the server.
    /// </summary>
    [Test]
    public void ReadableElementTypes_Enum_OffersTheOrdinalAndTheLabel()
    {
        IColumnCodec codec = Codec("Enum8('a' = 1, 'b' = 2)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { typeof(sbyte), typeof(string) }));
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(codec.WritableElementTypes));
        });
    }

    [Test]
    public void TryProjectRead_EnumAskedForAString_YieldsTheDeclaredLabel()
    {
        Func<sbyte, string> project = Project<sbyte, string>(Codec("Enum8('a' = -1, 'b' = 127)"));

        Assert.Multiple(() =>
        {
            Assert.That(project(-1), Is.EqualTo("a"));
            Assert.That(project(127), Is.EqualTo("b"));
        });
    }

    /// <summary>
    /// Every row of a column read from the server is a declared ordinal, so the projection cannot meet this on a
    /// real read. Pinned anyway: it is the difference between a clear failure and a wrong label.
    /// </summary>
    [Test]
    public void TryProjectRead_EnumOrdinalWithNoDeclaredMember_ThrowsNamingTheType()
    {
        Func<sbyte, string> project = Project<sbyte, string>(Codec("Enum8('a' = -1, 'b' = 127)"));

        var thrown = Assert.Throws<KeyNotFoundException>(() => project(0));
        Assert.That(thrown.Message, Does.Contain("Enum8('a' = -1, 'b' = 127)").And.Contain("ordinal 0"));
    }

    [Test]
    public void TryProjectRead_EnumAskedForAnUnrelatedType_ReturnsFalse()
    {
        IColumnCodec codec = Codec("Enum8('a' = 1)");
        ParameterExpression source = Expression.Parameter(typeof(sbyte), "v");

        Assert.That(codec.TryProjectRead(source, typeof(int), out Expression _), Is.False);
    }

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
    /// The projected view stands in for the column it reads, so it reports that column's name and type — and
    /// disposing it must leave the column alone: the block owns that storage and every other reader of the block
    /// shares it.
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

    private static IColumn<T> ReadAs<T>(IColumn column)
        => ColumnCodecRegistry.Default.Projections.ReadAs<T>(column, new ResolveContext { ServerTimezone = "UTC" });

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
