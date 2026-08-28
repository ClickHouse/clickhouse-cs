using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>Tests read and write type lifting through nested composite codecs.</summary>
[TestFixture]
public class CompositeLiftMatrixTests
{
    /// <summary>Pairs a ClickHouse type with its canonical and lifted CLR types.</summary>
    public sealed record Case(string ColumnType, Type Canonical, Type Lifted)
    {
        public override string ToString() => ColumnType;
    }

    public static IEnumerable<Case> Cases()
    {
        // Single-level baselines.
        yield return new Case("Array(DateTime('UTC'))", typeof(uint[]), typeof(DateTime[]));
        yield return new Case("Array(Time64(3))", typeof(long[]), typeof(TimeSpan[]));
        yield return new Case("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<uint, string>), typeof(ValueTuple<DateTime, string>));
        yield return new Case("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, uint>[]), typeof(KeyValuePair<string, DateTime>[]));

        // Nested containers.
        yield return new Case("Array(Array(DateTime('UTC')))", typeof(uint[][]), typeof(DateTime[][]));
        yield return new Case("Array(Array(Array(DateTime64(3, 'UTC'))))", typeof(long[][][]), typeof(DateTime[][][]));
        yield return new Case("Array(Tuple(DateTime('UTC'), String))", typeof(ValueTuple<uint, string>[]), typeof(ValueTuple<DateTime, string>[]));
        yield return new Case("Array(Array(Tuple(DateTime('UTC'), String)))", typeof(ValueTuple<uint, string>[][]), typeof(ValueTuple<DateTime, string>[][]));
        yield return new Case("Tuple(Array(DateTime('UTC')), String)", typeof(ValueTuple<uint[], string>), typeof(ValueTuple<DateTime[], string>));
        yield return new Case("Tuple(Array(Array(Time)), Time64(6))", typeof(ValueTuple<int[][], long>), typeof(ValueTuple<TimeSpan[][], TimeSpan>));

        // Independently lifted map children.
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

        // Nullable and LowCardinality children.
        yield return new Case("Array(Nullable(DateTime('UTC')))", typeof(uint?[]), typeof(DateTime?[]));
        yield return new Case("Array(Array(Nullable(Time64(3))))", typeof(long?[][]), typeof(TimeSpan?[][]));
        yield return new Case("Tuple(Nullable(DateTime('UTC')), String)", typeof(ValueTuple<uint?, string>), typeof(ValueTuple<DateTime?, string>));
        yield return new Case("Array(LowCardinality(DateTime('UTC')))", typeof(uint[]), typeof(DateTime[]));
        yield return new Case("Map(String, LowCardinality(Nullable(DateTime('UTC'))))", typeof(KeyValuePair<string, uint?>[]), typeof(KeyValuePair<string, DateTime?>[]));

        // Full-arity tuple with mixed calendar types.
        yield return new Case(
            "Tuple(DateTime('UTC'), DateTime64(3, 'UTC'), Time, Time64(3), Int32, String, Nullable(DateTime('UTC')))",
            typeof(ValueTuple<uint, long, int, long, int, string, uint?>),
            typeof(ValueTuple<DateTime, DateTime, TimeSpan, TimeSpan, int, string, DateTime?>));

        // Partially lifted tuple.
        yield return new Case(
            "Tuple(DateTime('UTC'), DateTime64(3, 'UTC'), Time)",
            typeof(ValueTuple<uint, long, int>),
            typeof(ValueTuple<DateTime, long, TimeSpan>));
    }

    private static IColumnCodec Codec(string type)
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = "UTC" });

    /// <summary>Creates a minimal value used to compile and invoke a projection.</summary>
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

    [TestCase("Array(Nested(a UInt8))")]
    [TestCase("Tuple(Nested(a UInt8), String)")]
    [TestCase("Map(String, Nested(a UInt8))")]
    [TestCase("Array(Array(Nested(a UInt8)))")]
    [TestCase("Array(Nothing)")]
    [TestCase("Tuple(Nothing, String)")]

    // Wrappers must preserve an inner codec's write refusal.
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
