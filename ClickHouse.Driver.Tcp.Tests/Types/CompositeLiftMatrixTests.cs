using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Read and write type lifting through nested composite types. The differential tests (<c>DifferentialCases</c>) read
/// each case as its canonical and its lifted type, and write it from both.
/// </summary>
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

        // A nullable tuple whose fields lift.
        yield return new Case("Nullable(Tuple(DateTime('UTC'), String))", typeof(ValueTuple<uint, string>?), typeof(ValueTuple<DateTime, string>?));
    }

    private static IColumnCodec Codec(string type)
        => ColumnCodecRegistry.Default.Resolve(type, new ResolveContext { ServerTimezone = "UTC" });

    [TestCaseSource(nameof(Cases))]
    public void ElementType_NestedComposite_IsTheCanonicalShapeTheCaseDeclares(Case testCase)
        => Assert.That(Codec(testCase.ColumnType).ElementType, Is.EqualTo(testCase.Canonical));

    // The differential tests read the values of each case as its lifted type.
    [TestCaseSource(nameof(Cases))]
    public void CanRead_NestedComposite_IsTrueForTheLiftedType(Case testCase)
        => Assert.That(ClickHouseTcpTypes.CanRead(testCase.ColumnType, testCase.Lifted), Is.True, $"{testCase.ColumnType} does not read as {testCase.Lifted}");

    [TestCase("Array(Nested(a UInt8))")]
    [TestCase("Tuple(Nested(a UInt8), String)")]
    [TestCase("Map(String, Nested(a UInt8))")]
    [TestCase("Array(Array(Nested(a UInt8)))")]
    [TestCase("Array(Nothing)")]
    [TestCase("Tuple(Nothing, String)")]

    // A composite keeps the write refusal of its child.
    [TestCase("Nullable(Nothing)")]
    [TestCase("Array(Nullable(Nothing))")]
    [TestCase("Array(Array(Nullable(Nothing)))")]
    [TestCase("Map(String, Nullable(Nothing))")]
    [TestCase("Tuple(Nullable(Nothing), String)")]
    [TestCase("Variant(String, Nested(a UInt8))")]
    [TestCase("Array(Variant(String, Nested(a UInt8)))")]
    [TestCase("Map(String, Variant(String, Nested(a UInt8)))")]
    [TestCase("Tuple(Variant(String, Nested(a UInt8)), String)")]
    public void CanWrite_CompositeOverAChildThatNoValueCanFill_IsFalseForItsElementType(string type)
        => Assert.That(ClickHouseTcpTypes.CanWrite(type, Codec(type).ElementType), Is.False);
}
