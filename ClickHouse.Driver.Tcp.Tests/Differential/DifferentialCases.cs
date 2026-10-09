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
/// <c>CompositeLiftMatrixTests</c> and of <c>ColumnReadProjectionTests</c>.
/// </summary>
public static class DifferentialCases
{
    /// <summary>The number of cases from <c>InsertRoundTripCase.CasesFor(TcpFeature.All)</c>.</summary>
    public const int InsertRoundTripCount = 286;

    /// <summary>The number of cases from <c>CompositeLiftMatrixTests.Cases()</c>.</summary>
    public const int CompositeLiftMatrixCount = 22;

    /// <summary>The number of column types that <c>ColumnReadProjectionTests</c> reads.</summary>
    public const int ColumnReadProjectionCount = 55;

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
    // of ColumnReadProjectionTests reads the type as.
    private static IEnumerable<(string, Func<string, DifferentialCase>)> FromColumnReadProjection()
    {
        List<(string Type, Type Target)> pairs = ColumnReadProjectionPairs().ToList();
        IEnumerable<string> types = ColumnReadProjectionTests.RegisteredTypes
            .Concat(ColumnReadProjectionTests.WrappedTypes)
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

    // The (column type, target) arguments of each test method of ColumnReadProjectionTests whose first two
    // parameters are a string and a Type, from its [TestCase] attributes and its TestCaseSource.
    private static IEnumerable<(string Type, Type Target)> ColumnReadProjectionPairs()
    {
        const BindingFlags Static = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;
        IEnumerable<MethodInfo> methods = typeof(ColumnReadProjectionTests)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.GetParameters() is { Length: >= 2 } parameters && parameters[0].ParameterType == typeof(string) && parameters[1].ParameterType == typeof(Type))
            .OrderBy(m => m.Name, StringComparer.Ordinal);

        foreach (MethodInfo method in methods)
        {
            foreach (TestCaseAttribute testCase in method.GetCustomAttributes<TestCaseAttribute>())
            {
                yield return ((string)testCase.Arguments[0], (Type)testCase.Arguments[1]);
            }

            foreach (TestCaseSourceAttribute source in method.GetCustomAttributes<TestCaseSourceAttribute>())
            {
                MethodInfo sourceMethod = typeof(ColumnReadProjectionTests).GetMethod(source.SourceName, Static)
                    ?? throw new InvalidOperationException($"ColumnReadProjectionTests has no static method '{source.SourceName}'.");
                foreach (object row in (IEnumerable)sourceMethod.Invoke(null, null))
                {
                    object[] arguments = row is TestCaseData data ? data.Arguments : (object[])row;
                    yield return ((string)arguments[0], (Type)arguments[1]);
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
