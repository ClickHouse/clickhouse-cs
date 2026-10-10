using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using ClickHouse.Driver.Tcp.Tests.Types;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The case list of the differential tests: every case of <c>InsertRoundTripCase</c> and of
/// <c>CompositeLiftMatrixTests</c>, a table of column types with read targets, and each value and error scenario of
/// <c>ColumnReadProjectionTests</c>.
/// </summary>
public static class DifferentialCases
{
    /// <summary>The number of cases from <c>InsertRoundTripCase.CasesFor(TcpFeature.All)</c>.</summary>
    public const int InsertRoundTripCount = 286;

    /// <summary>The number of cases from <c>CompositeLiftMatrixTests.Cases()</c>.</summary>
    public const int CompositeLiftMatrixCount = 22;

    /// <summary>The number of column types of the read targets table (<see cref="CaseSource.ColumnReadProjection"/>).</summary>
    public const int ColumnReadProjectionCount = 56;

    /// <summary>The number of <c>ColumnReadScenario</c> values of the tests of <c>ColumnReadProjectionTests</c>.</summary>
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

    // The scenarios of the tests of ColumnReadProjectionTests that take one ColumnReadScenario, from their
    // TestCaseSource, in the order of the test names.
    private static IEnumerable<ColumnReadScenario> Scenarios()
    {
        const BindingFlags Static = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;
        IEnumerable<MethodInfo> methods = typeof(ColumnReadProjectionTests)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.GetParameters() is { Length: 1 } parameters && parameters[0].ParameterType == typeof(ColumnReadScenario))
            .OrderBy(m => m.Name, StringComparer.Ordinal);

        foreach (MethodInfo method in methods)
        {
            foreach (TestCaseSourceAttribute source in method.GetCustomAttributes<TestCaseSourceAttribute>())
            {
                MethodInfo sourceMethod = typeof(ColumnReadProjectionTests).GetMethod(source.SourceName, Static)
                    ?? throw new InvalidOperationException($"ColumnReadProjectionTests has no static method '{source.SourceName}'.");
                foreach (ColumnReadScenario scenario in (IEnumerable<ColumnReadScenario>)sourceMethod.Invoke(null, null))
                {
                    yield return scenario;
                }
            }
        }
    }

    private static DifferentialCase TypeOnlyCase(string id, CaseSource source, string columnType, IReadOnlyList<Type> targets)
    {
        Type canonical = Codec(columnType).ElementType;
        var inputs = new List<WriteInput> { WriteInput.Built("canonical", canonical, name => SampleColumns.Build(name, columnType)) };
        inputs.AddRange(targets.Where(t => t != canonical).Select(WriteInput.ReadBack));
        return new DifferentialCase(id, source, columnType, SampleColumns.RowCount, targets, inputs);
    }

    private static IColumnCodec Codec(string columnType) => ColumnCodecRegistry.Default.Resolve(columnType, DifferentialEngine.Context);
}
