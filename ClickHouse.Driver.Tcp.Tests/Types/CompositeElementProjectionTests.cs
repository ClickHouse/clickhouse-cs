using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers the container codecs lifting their children's readings through <see cref="IColumnCodec.TryProjectRead"/> —
/// <c>Array</c>, <c>Map</c> and <c>Tuple</c> recursing into their element, key/value and field codecs.
///
/// <para>
/// Everything here is expression-shape and API-surface: which targets are offered and which refused, that a lifted
/// row is rebuilt element-wise with the right values, and that the emitted tree evaluates its source exactly once. A
/// server round-trip cannot observe any of it — it sees only the canonical reading — so this is the layer that owns
/// the projections themselves.
/// </para>
/// </summary>
[TestFixture]
public class CompositeElementProjectionTests
{
    private static int sourceEvaluations;

    private static IColumnCodec Codec(string type, string serverTimezone = "UTC")
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = serverTimezone });

    /// <summary>Counts how often a projection evaluates the expression it was handed.</summary>
    public static T Counted<T>(T value)
    {
        sourceEvaluations++;
        return value;
    }

    /// <summary>Compiles a codec's projection into a delegate so its runtime result can be asserted.</summary>
    private static Func<TSource, TTarget> Project<TSource, TTarget>(IColumnCodec codec)
    {
        ParameterExpression source = Expression.Parameter(typeof(TSource), "v");

        Assert.That(codec.TryProjectRead(source, typeof(TTarget), out Expression body), Is.True,
            $"{codec.TypeName} does not project to {typeof(TTarget)}");
        Assert.That(body.Type, Is.EqualTo(typeof(TTarget)), "the projection must yield the requested type");

        return Expression.Lambda<Func<TSource, TTarget>>(body, source).Compile();
    }

    private static void AssertNotOffered<TTarget>(IColumnCodec codec, Type sourceType)
    {
        ParameterExpression source = Expression.Parameter(sourceType, "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, typeof(TTarget), out Expression projected), Is.False);
            Assert.That(projected, Is.Null, "a refusal must leave no projection behind");
        });
    }

    private static DateTime Utc(long epochSeconds) => DateTimeOffset.FromUnixTimeSeconds(epochSeconds).UtcDateTime;

    [Test]
    public void TryProjectRead_ArrayOfDateTimeAskedForCalendarElements_LiftsEachElement()
    {
        Func<uint[], DateTime[]> project = Project<uint[], DateTime[]>(Codec("Array(DateTime('UTC'))"));

        Assert.That(project(new uint[] { 0, 1_000_000, 2_000_000_000 }),
            Is.EqualTo(new[] { Utc(0), Utc(1_000_000), Utc(2_000_000_000) }));
    }

    /// <summary>
    /// The canonical reading is the identity, and must stay the identity: an <c>Array</c> that rebuilt its row for its
    /// own element type would allocate per row for no conversion at all.
    /// </summary>
    [Test]
    public void TryProjectRead_ArrayAskedForItsOwnElementType_IsTheIdentity()
    {
        IColumnCodec codec = Codec("Array(UInt64)");
        ParameterExpression source = Expression.Parameter(typeof(ulong[]), "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, typeof(ulong[]), out Expression projected), Is.True);
            Assert.That(projected, Is.SameAs(source));
        });
    }

    [Test]
    public void TryProjectRead_ArrayOfEmptyRow_YieldsAnEmptyTargetArray()
    {
        Func<uint[], DateTime[]> project = Project<uint[], DateTime[]>(Codec("Array(DateTime('UTC'))"));

        Assert.That(project(Array.Empty<uint>()), Is.Empty);
    }

    /// <summary>
    /// The row expression reaches the projection once however many elements it has. A tree that spliced it per
    /// element would re-run whatever produced the row — a span access or an accessor call — for every element.
    /// </summary>
    [Test]
    public void TryProjectRead_ArrayLifted_EvaluatesTheRowExpressionOnce()
    {
        IColumnCodec codec = Codec("Array(DateTime('UTC'))");
        ParameterExpression parameter = Expression.Parameter(typeof(uint[]), "v");
        Expression counted = Expression.Call(
            typeof(CompositeElementProjectionTests).GetMethod(nameof(Counted)).MakeGenericMethod(typeof(uint[])),
            parameter);

        Assert.That(codec.TryProjectRead(counted, typeof(DateTime[]), out Expression body), Is.True);
        Func<uint[], DateTime[]> project = Expression.Lambda<Func<uint[], DateTime[]>>(body, parameter).Compile();

        sourceEvaluations = 0;
        project(new uint[] { 1, 2, 3, 4, 5 });

        Assert.That(sourceEvaluations, Is.EqualTo(1));
    }

    /// <summary>
    /// A container recurses through another container, so the lift is not limited to one level. This is the shape that
    /// makes enumerating readable sets hopeless and interrogation necessary.
    /// </summary>
    [Test]
    public void TryProjectRead_NestedArrayOfDateTime_LiftsThroughBothLevels()
    {
        Func<uint[][], DateTime[][]> project = Project<uint[][], DateTime[][]>(Codec("Array(Array(DateTime('UTC')))"));

        DateTime[][] result = project(new[] { new uint[] { 0, 60 }, Array.Empty<uint>() });

        Assert.Multiple(() =>
        {
            Assert.That(result[0], Is.EqualTo(new[] { Utc(0), Utc(60) }));
            Assert.That(result[1], Is.Empty);
        });
    }

    [Test]
    public void TryProjectRead_ArrayOfNullableDateTime_LiftsAndKeepsAbsentElements()
    {
        Func<uint?[], DateTime?[]> project = Project<uint?[], DateTime?[]>(Codec("Array(Nullable(DateTime('UTC')))"));

        Assert.That(project(new uint?[] { 0, null, 60 }), Is.EqualTo(new DateTime?[] { Utc(0), null, Utc(60) }));
    }

    [Test]
    public void TryProjectRead_ArrayAskedForAnElementTheInnerRefuses_ReturnsFalse()
        => AssertNotOffered<string[]>(Codec("Array(DateTime('UTC'))"), typeof(uint[]));

    /// <summary>
    /// An <c>Array</c> row is a single-dimension array, so a rank-2 target is refused rather than being treated as if
    /// its element type were the row's.
    /// </summary>
    [Test]
    public void TryProjectRead_ArrayAskedForAMultiDimensionTarget_ReturnsFalse()
    {
        IColumnCodec codec = Codec("Array(DateTime('UTC'))");
        ParameterExpression source = Expression.Parameter(typeof(uint[]), "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, typeof(DateTime[,]), out Expression projected), Is.False);
            Assert.That(projected, Is.Null);
        });
    }

    [Test]
    public void TryProjectRead_ArrayAskedForANonArrayTarget_ReturnsFalse()
        => AssertNotOffered<DateTime>(Codec("Array(DateTime('UTC'))"), typeof(uint[]));

    [Test]
    public void TryProjectRead_MapAskedForCalendarValues_LiftsTheValueAndKeepsTheKey()
    {
        Func<KeyValuePair<string, uint>[], KeyValuePair<string, DateTime>[]> project =
            Project<KeyValuePair<string, uint>[], KeyValuePair<string, DateTime>[]>(Codec("Map(String, DateTime('UTC'))"));

        Assert.That(
            project(new[] { new KeyValuePair<string, uint>("a", 0), new KeyValuePair<string, uint>("b", 60) }),
            Is.EqualTo(new[] { new KeyValuePair<string, DateTime>("a", Utc(0)), new KeyValuePair<string, DateTime>("b", Utc(60)) }));
    }

    /// <summary>The key and the value are asked independently, so lifting the key alone is offered.</summary>
    [Test]
    public void TryProjectRead_MapAskedForCalendarKeys_LiftsTheKeyAndKeepsTheValue()
    {
        Func<KeyValuePair<uint, string>[], KeyValuePair<DateTime, string>[]> project =
            Project<KeyValuePair<uint, string>[], KeyValuePair<DateTime, string>[]>(Codec("Map(DateTime('UTC'), String)"));

        Assert.That(
            project(new[] { new KeyValuePair<uint, string>(60, "a") }),
            Is.EqualTo(new[] { new KeyValuePair<DateTime, string>(Utc(60), "a") }));
    }

    [Test]
    public void TryProjectRead_MapAskedForItsOwnElementType_IsTheIdentity()
    {
        IColumnCodec codec = Codec("Map(String, UInt64)");
        ParameterExpression source = Expression.Parameter(typeof(KeyValuePair<string, ulong>[]), "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, typeof(KeyValuePair<string, ulong>[]), out Expression projected), Is.True);
            Assert.That(projected, Is.SameAs(source));
        });
    }

    /// <summary>
    /// A map row is an array of pairs specifically. An array of something else cannot hold it, whatever that
    /// something else's own type arguments are.
    /// </summary>
    [Test]
    public void TryProjectRead_MapAskedForAnArrayOfNonPairs_ReturnsFalse()
        => AssertNotOffered<Tuple<string, DateTime>[]>(Codec("Map(String, DateTime('UTC'))"), typeof(KeyValuePair<string, uint>[]));

    [Test]
    public void TryProjectRead_MapAskedForAValueTheChildRefuses_ReturnsFalse()
        => AssertNotOffered<KeyValuePair<string, Guid>[]>(Codec("Map(String, DateTime('UTC'))"), typeof(KeyValuePair<string, uint>[]));

    [Test]
    public void TryProjectRead_TupleAskedForCalendarFields_LiftsThePerFieldReadings()
    {
        Func<ValueTuple<uint, string>, ValueTuple<DateTime, string>> project =
            Project<ValueTuple<uint, string>, ValueTuple<DateTime, string>>(Codec("Tuple(DateTime('UTC'), String)"));

        Assert.That(project(new ValueTuple<uint, string>(60, "a")), Is.EqualTo(new ValueTuple<DateTime, string>(Utc(60), "a")));
    }

    /// <summary>One field may lift while the rest stay canonical — each child is asked for its own field's target.</summary>
    [Test]
    public void TryProjectRead_TupleWithTwoLiftableFieldsAskedForOne_LiftsOnlyThatField()
    {
        Func<ValueTuple<uint, uint>, ValueTuple<DateTime, uint>> project =
            Project<ValueTuple<uint, uint>, ValueTuple<DateTime, uint>>(Codec("Tuple(DateTime('UTC'), DateTime('UTC'))"));

        Assert.That(project(new ValueTuple<uint, uint>(60, 120)), Is.EqualTo(new ValueTuple<DateTime, uint>(Utc(60), 120)));
    }

    [Test]
    public void TryProjectRead_TupleAskedForItsOwnElementType_IsTheIdentity()
    {
        IColumnCodec codec = Codec("Tuple(UInt64, String)");
        ParameterExpression source = Expression.Parameter(typeof(ValueTuple<ulong, string>), "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, typeof(ValueTuple<ulong, string>), out Expression projected), Is.True);
            Assert.That(projected, Is.SameAs(source));
        });
    }

    /// <summary>
    /// Arity is part of the tuple's shape, not something a child could absorb, so a narrower target is refused rather
    /// than being filled from the fields that do line up.
    /// </summary>
    [Test]
    public void TryProjectRead_TupleAskedForADifferentArity_ReturnsFalse()
        => AssertNotOffered<ValueTuple<DateTime>>(Codec("Tuple(DateTime('UTC'), String)"), typeof(ValueTuple<uint, string>));

    [Test]
    public void TryProjectRead_TupleAskedForAReferenceTuple_ReturnsFalse()
        => AssertNotOffered<Tuple<DateTime, string>>(Codec("Tuple(DateTime('UTC'), String)"), typeof(ValueTuple<uint, string>));

    [Test]
    public void TryProjectRead_TupleAskedForAFieldTheChildRefuses_ReturnsFalse()
        => AssertNotOffered<ValueTuple<Guid, string>>(Codec("Tuple(DateTime('UTC'), String)"), typeof(ValueTuple<uint, string>));

    /// <summary>
    /// A container inside a container lifts, so a tuple field that is itself an array reaches the element codec.
    /// </summary>
    [Test]
    public void TryProjectRead_TupleWithAnArrayField_LiftsThroughTheArray()
    {
        Func<ValueTuple<uint[], string>, ValueTuple<DateTime[], string>> project =
            Project<ValueTuple<uint[], string>, ValueTuple<DateTime[], string>>(Codec("Tuple(Array(DateTime('UTC')), String)"));

        ValueTuple<DateTime[], string> result = project(new ValueTuple<uint[], string>(new uint[] { 0, 60 }, "a"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Item1, Is.EqualTo(new[] { Utc(0), Utc(60) }));
            Assert.That(result.Item2, Is.EqualTo("a"));
        });
    }

    /// <summary>
    /// <c>Nested</c>, <c>Variant</c> and <c>Dynamic</c> erase their children's CLR types into <c>object[][]</c> or
    /// <c>object</c>, so there is no per-child type at the surface to lift into. They keep the identity-only default,
    /// and that is a property of the surface rather than a gap to close later.
    /// </summary>
    [TestCase("Variant(String, UInt64)", typeof(object))]
    [TestCase("Dynamic", typeof(object))]
    [TestCase("Nested(a DateTime('UTC'))", typeof(object[][]))]
    public void TryProjectRead_ChildErasingComposite_OffersNothingBeyondItsElementType(string type, Type elementType)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression source = Expression.Parameter(elementType, "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(elementType));
            Assert.That(codec.TryProjectRead(source, elementType, out Expression identity), Is.True);
            Assert.That(identity, Is.SameAs(source));
            Assert.That(codec.TryProjectRead(source, typeof(DateTime[]), out Expression projected), Is.False);
            Assert.That(projected, Is.Null);
        });
    }

    /// <summary>
    /// The diagnostics lists stay canonical-only for a container: the honest answer is its children's cartesian
    /// product, which would cost a materialized <see cref="Type"/> per combination on a failure path.
    /// <see cref="IColumnCodec.TryProjectRead"/> is the authority, and it answers targets this list omits.
    /// </summary>
    [TestCase("Array(DateTime('UTC'))", typeof(uint[]))]
    [TestCase("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, uint>[]))]
    [TestCase("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<uint, string>))]
    public void ReadableElementTypes_ContainerThatLifts_StillReportsOnlyItsElementType(string type, Type elementType)
    {
        IColumnCodec codec = Codec(type);

        Assert.Multiple(() =>
        {
            Assert.That(codec.ReadableElementTypes, Is.EqualTo(new[] { elementType }));
            Assert.That(codec.ElementType, Is.EqualTo(elementType));
        });
    }

    /// <summary>
    /// A source expression of the wrong type is a caller mistake, distinct from a target the codec does not offer, so
    /// a container throws rather than reporting "no such projection".
    /// </summary>
    [TestCase("Array(DateTime('UTC'))")]
    [TestCase("Map(String, DateTime('UTC'))")]
    [TestCase("Tuple(DateTime('UTC'), String)")]
    public void TryProjectRead_ContainerGivenASourceOfWrongType_ThrowsArgumentException(string type)
    {
        IColumnCodec codec = Codec(type);
        ParameterExpression wrong = Expression.Parameter(typeof(long), "v");

        Assert.That(
            () => codec.TryProjectRead(wrong, codec.ElementType, out _),
            Throws.ArgumentException.With.Message.Contains("projects from its element type"));
    }
}
