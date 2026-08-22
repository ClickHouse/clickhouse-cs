using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// A breadth sweep over deeply nested composite types, checking that lifting composes in both directions and that the
/// two directions agree.
///
/// <para>
/// The per-shape rules live in <see cref="CompositeElementProjectionTests"/> and
/// <c>ColumnWriteAcceptanceTests</c>; what this file adds is depth and combination —
/// <c>Array(Array(Tuple(...)))</c>, a map whose key is a tuple and whose value is an array, a seven-field tuple of
/// mixed calendar types. Those are where a recursion that works one level down stops working, and where the read and
/// write sides can drift apart.
/// </para>
/// </summary>
[TestFixture]
public class CompositeLiftMatrixTests
{
    /// <summary>One nested column type, the CLR type it decodes as, and the lifted CLR type a caller would rather use.</summary>
    /// <param name="ColumnType">The ClickHouse type string.</param>
    /// <param name="Canonical">The CLR element type the column decodes as.</param>
    /// <param name="Lifted">The CLR element type lifting should offer in both directions.</param>
    public sealed record Case(string ColumnType, Type Canonical, Type Lifted)
    {
        public override string ToString() => ColumnType;
    }

    public static IEnumerable<Case> Cases()
    {
        // One level, as the baseline the deeper cases are compared against.
        yield return new Case("Array(DateTime('UTC'))", typeof(uint[]), typeof(DateTime[]));
        yield return new Case("Array(Time64(3))", typeof(long[]), typeof(TimeSpan[]));
        yield return new Case("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<uint, string>), typeof(ValueTuple<DateTime, string>));
        yield return new Case("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, uint>[]), typeof(KeyValuePair<string, DateTime>[]));

        // Containers through containers.
        yield return new Case("Array(Array(DateTime('UTC')))", typeof(uint[][]), typeof(DateTime[][]));
        yield return new Case("Array(Array(Array(DateTime64(3, 'UTC'))))", typeof(long[][][]), typeof(DateTime[][][]));
        yield return new Case("Array(Tuple(DateTime('UTC'), String))", typeof(ValueTuple<uint, string>[]), typeof(ValueTuple<DateTime, string>[]));
        yield return new Case("Array(Array(Tuple(DateTime('UTC'), String)))", typeof(ValueTuple<uint, string>[][]), typeof(ValueTuple<DateTime, string>[][]));
        yield return new Case("Tuple(Array(DateTime('UTC')), String)", typeof(ValueTuple<uint[], string>), typeof(ValueTuple<DateTime[], string>));
        yield return new Case("Tuple(Array(Array(Time)), Time64(6))", typeof(ValueTuple<int[][], long>), typeof(ValueTuple<TimeSpan[][], TimeSpan>));

        // Maps, whose two children lift independently.
        yield return new Case("Map(String, Array(DateTime('UTC')))", typeof(KeyValuePair<string, uint[]>[]), typeof(KeyValuePair<string, DateTime[]>[]));
        yield return new Case("Array(Map(String, DateTime('UTC')))", typeof(KeyValuePair<string, uint>[][]), typeof(KeyValuePair<string, DateTime>[][]));
        yield return new Case("Map(DateTime('UTC'), Time64(3))", typeof(KeyValuePair<uint, long>[]), typeof(KeyValuePair<DateTime, TimeSpan>[]));
        yield return new Case(
            "Map(Tuple(DateTime('UTC'), String), Array(Time))",
            typeof(KeyValuePair<ValueTuple<uint, string>, int[]>[]),
            typeof(KeyValuePair<ValueTuple<DateTime, string>, TimeSpan[]>[]));
        yield return new Case(
            "Map(String, Map(String, DateTime('UTC')))",
            typeof(KeyValuePair<string, KeyValuePair<string, uint>[]>[]),
            typeof(KeyValuePair<string, KeyValuePair<string, DateTime>[]>[]));

        // Nullable and LowCardinality inside containers.
        yield return new Case("Array(Nullable(DateTime('UTC')))", typeof(uint?[]), typeof(DateTime?[]));
        yield return new Case("Array(Array(Nullable(Time64(3))))", typeof(long?[][]), typeof(TimeSpan?[][]));
        yield return new Case("Tuple(Nullable(DateTime('UTC')), String)", typeof(ValueTuple<uint?, string>), typeof(ValueTuple<DateTime?, string>));
        yield return new Case("Array(LowCardinality(DateTime('UTC')))", typeof(uint[]), typeof(DateTime[]));
        yield return new Case("Map(String, LowCardinality(Nullable(DateTime('UTC'))))", typeof(KeyValuePair<string, uint?>[]), typeof(KeyValuePair<string, DateTime?>[]));

        // A full-arity tuple of mixed calendar types: the shape whose accepted set is 3^7, and the reason neither side
        // enumerates.
        yield return new Case(
            "Tuple(DateTime('UTC'), DateTime64(3, 'UTC'), Time, Time64(3), Int32, String, Nullable(DateTime('UTC')))",
            typeof(ValueTuple<uint, long, int, long, int, string, uint?>),
            typeof(ValueTuple<DateTime, DateTime, TimeSpan, TimeSpan, int, string, DateTime?>));

        // Only some fields lifted, the rest left canonical.
        yield return new Case(
            "Tuple(DateTime('UTC'), DateTime64(3, 'UTC'), Time)",
            typeof(ValueTuple<uint, long, int>),
            typeof(ValueTuple<DateTime, long, TimeSpan>));
    }

    private static IColumnCodec Codec(string type)
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = "UTC" });

    /// <summary>
    /// A minimal non-null value of <paramref name="type"/>, so a projection can actually be invoked. Containers come out
    /// empty, which is enough to prove the tree runs — the per-element values are asserted by the focused tests and by
    /// the server round-trips.
    /// </summary>
    private static object Sample(Type type)
    {
        if (type == typeof(string))
        {
            return string.Empty;
        }

        if (type.IsArray)
        {
            return Array.CreateInstance(type.GetElementType(), 0);
        }

        if (Nullable.GetUnderlyingType(type) is not null)
        {
            return null;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition().FullName!.StartsWith("System.ValueTuple", StringComparison.Ordinal))
        {
            Type[] arguments = type.GetGenericArguments();
            var fields = new object[arguments.Length];
            for (int i = 0; i < arguments.Length; i++)
            {
                fields[i] = Sample(arguments[i]);
            }

            return Activator.CreateInstance(type, fields);
        }

        return Activator.CreateInstance(type);
    }

    [TestCaseSource(nameof(Cases))]
    public void ElementType_NestedComposite_IsTheCanonicalShapeTheCaseDeclares(Case testCase)
        => Assert.That(Codec(testCase.ColumnType).ElementType, Is.EqualTo(testCase.Canonical));

    /// <summary>
    /// The lifted reading is offered however deep it sits, and the tree it builds runs rather than merely type-checking:
    /// a malformed expression only fails when compiled and invoked.
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void TryProjectRead_NestedComposite_OffersTheLiftedReadingAndRuns(Case testCase)
    {
        IColumnCodec codec = Codec(testCase.ColumnType);
        ParameterExpression source = Expression.Parameter(testCase.Canonical, "v");

        Assert.That(codec.TryProjectRead(source, testCase.Lifted, out Expression body), Is.True,
            $"{testCase.ColumnType} does not offer {testCase.Lifted}");
        Assert.That(body.Type, Is.EqualTo(testCase.Lifted));

        Delegate compiled = Expression.Lambda(body, source).Compile();

        Assert.That(() => compiled.DynamicInvoke(Sample(testCase.Canonical)), Throws.Nothing);
    }

    /// <summary>The canonical reading stays the identity at every depth — a lifting container must not rebuild for nothing.</summary>
    [TestCaseSource(nameof(Cases))]
    public void TryProjectRead_NestedCompositeAskedForItsOwnElementType_IsTheIdentity(Case testCase)
    {
        IColumnCodec codec = Codec(testCase.ColumnType);
        ParameterExpression source = Expression.Parameter(testCase.Canonical, "v");

        Assert.Multiple(() =>
        {
            Assert.That(codec.TryProjectRead(source, testCase.Canonical, out Expression projected), Is.True);
            Assert.That(projected, Is.SameAs(source));
        });
    }

    [TestCaseSource(nameof(Cases))]
    public void CanWriteElementType_NestedComposite_AcceptsBothTheCanonicalAndTheLiftedShape(Case testCase)
    {
        IColumnCodec codec = Codec(testCase.ColumnType);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWriteElementType(testCase.Canonical), Is.True, "the canonical shape must always be writable");
            Assert.That(codec.CanWriteElementType(testCase.Lifted), Is.True, $"{testCase.ColumnType} cannot be written from {testCase.Lifted}");
        });
    }

    /// <summary>
    /// The invariant that keeps a POCO round-tripping: anything the write side accepts, the read side must offer back.
    /// Checked over a pool of candidate CLR types rather than only the designed pair, since the failure mode is a write
    /// path that accepts a shape nobody can read — which inserts happily and then cannot be selected into the same
    /// property.
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void CanWriteElementType_WhateverItAccepts_TheReadSideAlsoOffers(Case testCase)
    {
        IColumnCodec codec = Codec(testCase.ColumnType);
        ParameterExpression source = Expression.Parameter(codec.ElementType, "v");

        Type[] candidates =
        {
            testCase.Canonical,
            testCase.Lifted,
            typeof(uint[]), typeof(int[]), typeof(long[]), typeof(DateTime[]), typeof(TimeSpan[]), typeof(string[]),
            typeof(DateTime?[]), typeof(TimeSpan?[]), typeof(uint?[]), typeof(int?[]),
            typeof(DateTime[][]), typeof(TimeSpan[][]), typeof(uint[][]),
            typeof(ValueTuple<DateTime, string>), typeof(ValueTuple<uint, string>), typeof(ValueTuple<DateTime>),
            typeof(KeyValuePair<string, DateTime>[]), typeof(KeyValuePair<string, uint>[]),
            typeof(object), typeof(object[]), typeof(DateTime), typeof(string),
        };

        Assert.Multiple(() =>
        {
            foreach (Type candidate in candidates)
            {
                if (!codec.CanWriteElementType(candidate))
                {
                    continue;
                }

                Assert.That(codec.TryProjectRead(source, candidate, out _), Is.True,
                    $"{testCase.ColumnType} can be written from {candidate} but cannot be read into it");
            }
        });
    }

    /// <summary>
    /// A container refuses a shape its children refuse, at depth. Without this the sweep above could pass by a codec
    /// accepting everything.
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void CanWriteElementType_NestedCompositeOfferedAnUnrelatedShape_ReturnsFalse(Case testCase)
    {
        IColumnCodec codec = Codec(testCase.ColumnType);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWriteElementType(typeof(Guid[])), Is.False);
            Assert.That(codec.CanWriteElementType(typeof(Guid)), Is.False);
            Assert.That(codec.CanWriteElementType(typeof(ValueTuple<Guid, Guid, Guid, Guid, Guid, Guid, Guid>)), Is.False);
        });
    }

    /// <summary>
    /// A composite over a child that cannot be written at all stays unwritable however it is wrapped, so a row-oriented
    /// insert reports it at plan build instead of failing once the INSERT is open.
    /// </summary>
    [TestCase("Array(Nested(a UInt8))")]
    [TestCase("Tuple(Nested(a UInt8), String)")]
    [TestCase("Map(String, Nested(a UInt8))")]
    [TestCase("Array(Array(Nested(a UInt8)))")]
    [TestCase("Array(Nothing)")]
    [TestCase("Tuple(Nothing, String)")]

    // A wrapper hides an unwritable child behind an element type of its own, so the refusal has to come from the
    // wrapper's own gate rather than from the shape of the type. Without that, the insert gate passes and the codec
    // faults part-way through writing the block, having already put bytes on the wire.
    [TestCase("Nullable(Nothing)")]
    [TestCase("Array(Nullable(Nothing))")]
    [TestCase("Array(Array(Nullable(Nothing)))")]
    [TestCase("Map(String, Nullable(Nothing))")]
    [TestCase("Tuple(Nullable(Nothing), String)")]
    [TestCase("Variant(String, Nested(a UInt8))")]
    [TestCase("Array(Variant(String, Nested(a UInt8)))")]
    [TestCase("Map(String, Variant(String, Nested(a UInt8)))")]
    [TestCase("Tuple(Variant(String, Nested(a UInt8)), String)")]
    public void CanWriteElementType_CompositeOverAnUnwritableChild_RefusesItsOwnElementType(string type)
    {
        IColumnCodec codec = Codec(type);

        Assert.That(codec.CanWriteElementType(codec.ElementType), Is.False);
    }

    /// <summary>
    /// The contract <see cref="IColumnCodec.CanWriteElementType"/> states: the two must agree wherever both can answer.
    /// A codec that gates <see cref="IColumnCodec.CanWrite"/> on something beyond the element type has to gate this on
    /// it too, or every composite that asks the interrogative question skips that gate.
    /// </summary>
    [TestCase("Nullable(Nothing)", typeof(object))]
    [TestCase("Variant(String, Nested(a UInt8))", typeof(object))]
    [TestCase("Array(Nullable(Nothing))", typeof(object[]))]
    [TestCase("Map(String, Nullable(Nothing))", typeof(object))]
    [TestCase("Nullable(Int32)", typeof(int?))]
    [TestCase("Variant(String, UInt64)", typeof(object))]
    [TestCase("Array(UInt32)", typeof(uint[]))]
    public void CanWriteElementType_AndCanWrite_AgreeOnAnArrayBackedColumn(string type, Type elementType)
    {
        IColumnCodec codec = Codec(type);
        var probe = (IColumn)Activator.CreateInstance(
            typeof(ArrayColumn<>).MakeGenericType(elementType),
            string.Empty,
            codec.TypeName,
            Array.CreateInstance(elementType, 0));

        Assert.That(codec.CanWriteElementType(elementType), Is.EqualTo(codec.CanWrite(probe)));
    }
}
