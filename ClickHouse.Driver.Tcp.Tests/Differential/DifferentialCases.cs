using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ClickHouse.Driver.Tcp.Tests.Types;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The case list of the differential tests: every case of <c>InsertRoundTripCase</c>, of
/// <c>CompositeLiftMatrixTests</c> and of <c>ColumnReadProjectionTests</c>, with each value and error scenario of
/// <c>ColumnReadProjectionTests</c>.
/// </summary>
public static class DifferentialCases
{
    /// <summary>The number of cases from <c>InsertRoundTripCase.CasesFor(TcpFeature.All)</c>.</summary>
    public const int InsertRoundTripCount = 286;

    /// <summary>The number of cases from <c>CompositeLiftMatrixTests.Cases()</c>.</summary>
    public const int CompositeLiftMatrixCount = 22;

    /// <summary>The number of column types that <c>ColumnReadProjectionTests</c> reads.</summary>
    public const int ColumnReadProjectionCount = 56;

    /// <summary>The number of <c>ColumnReadScenario</c> values of the tests of <c>ColumnReadProjectionTests</c>.</summary>
    public const int ColumnReadScenarioCount = 39;

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
    // type, and the values that the reference reads as the lifted type.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromCompositeLiftMatrix()
    {
        foreach (CompositeLiftMatrixTests.Case lift in CompositeLiftMatrixTests.Cases())
        {
            yield return (lift.ColumnType, id => TypeOnlyCase(id, CaseSource.CompositeLiftMatrix, lift.ColumnType, new[] { lift.Canonical, lift.Lifted }));
        }
    }

    // One case for each column type: the canonical type, each readable element type, and each target that a test
    // of ColumnReadProjectionTests reads the type as. The types are the two type lists, the types of the tests that
    // take only a type, and the types of the (type, target) tests.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromColumnReadProjection()
    {
        List<(string Type, Type Target)> pairs = ColumnReadProjectionPairs().ToList();
        IEnumerable<string> types = ColumnReadProjectionTests.RegisteredTypes
            .Concat(ColumnReadProjectionTests.WrappedTypes)
            .Concat(TestArguments(typeof(string)).Select(arguments => (string)arguments[0]))
            .Concat(pairs.Select(p => p.Type))
            .Distinct(StringComparer.Ordinal);

        foreach (string type in types)
        {
            IColumnCodec codec = Codec(type);
            IEnumerable<Type> asked = pairs.Where(p => p.Type == type).Select(p => p.Target).OrderBy(TypeNames.Of, StringComparer.Ordinal);
            List<Type> targets = new[] { codec.ElementType }.Concat(codec.ReadableElementTypes).Concat(asked).Distinct().ToList();
            yield return (type, id => TypeOnlyCase(id, CaseSource.ColumnReadProjection, type, targets));
        }
    }

    // One case for each scenario: its own values, read as its canonical type and as its target. The scenario states
    // the outcome of the ReadAs facet of its target.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromColumnReadScenarios()
    {
        foreach (object[] arguments in TestArguments(typeof(ColumnReadScenario)))
        {
            var scenario = (ColumnReadScenario)arguments[0];
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

    // The (column type, target) arguments of each test method of ColumnReadProjectionTests whose first two
    // parameters are a string and a Type.
    private static IEnumerable<(string Type, Type Target)> ColumnReadProjectionPairs()
        => TestArguments(typeof(string), typeof(Type)).Select(arguments => ((string)arguments[0], (Type)arguments[1]));

    // The arguments of each test of ColumnReadProjectionTests whose parameters start with the given types, from its
    // [TestCase] attributes and its TestCaseSource. A parameter list of one string takes only methods with that one
    // parameter: those tests take a column type.
    private static IEnumerable<object[]> TestArguments(params Type[] leading)
    {
        const BindingFlags Static = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;
        IEnumerable<MethodInfo> methods = typeof(ColumnReadProjectionTests)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.GetParameters() is var parameters
                && parameters.Length >= leading.Length
                && (leading.Length > 1 || parameters.Length == 1)
                && leading.Select((type, i) => parameters[i].ParameterType == type).All(match => match))
            .OrderBy(m => m.Name, StringComparer.Ordinal);

        foreach (MethodInfo method in methods)
        {
            foreach (TestCaseAttribute testCase in method.GetCustomAttributes<TestCaseAttribute>())
            {
                yield return testCase.Arguments;
            }

            foreach (TestCaseSourceAttribute source in method.GetCustomAttributes<TestCaseSourceAttribute>())
            {
                MethodInfo sourceMethod = typeof(ColumnReadProjectionTests).GetMethod(source.SourceName, Static)
                    ?? throw new InvalidOperationException($"ColumnReadProjectionTests has no static method '{source.SourceName}'.");
                foreach (object row in (IEnumerable)sourceMethod.Invoke(null, null))
                {
                    yield return row switch
                    {
                        TestCaseData data => data.Arguments,
                        object[] array => array,
                        _ => new[] { row },
                    };
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
