using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The POCO read plan against the reference plan of the differential tests (<see cref="PocoReadPlan{T}.BuildLegacy"/>),
/// for the matrix of column types and CLR targets of
/// <see cref="ReadRulesTests"/> (enums, casts, nullable targets, tuples, maps, the array casts), which the differential
/// case list does not have. Each scatter tier must give the outcome of the old plan: the same refusal, the same values,
/// or the same failure, for all the rows and for a window that starts past the first row of the block and past the
/// first row of the result.
/// </summary>
[TestFixture]
public class PocoReadRulesTests
{
    private static readonly (int Start, int Count, long RowOffset)[] Windows = { (0, 5, 0), (2, 3, 1_000) };

    private static IEnumerable<string> Types() => ReadRulesTests.ColumnTypes;

    [TestCaseSource(nameof(Types))]
    public void Materialize_EachTarget_GivesTheOutcomeOfTheOldPlan(string columnType)
    {
        var differences = new List<string>();
        foreach (Type target in ReadRulesTests.Targets)
        {
            string difference = (string)ConverterHarness.InvokeGeneric(typeof(PocoReadRulesTests), nameof(Compare), new[] { target }, columnType);
            if (difference is not null)
            {
                differences.Add($"{TypeNames.Of(target)}: {difference}");
            }
        }

        Assert.That(differences, Is.Empty, string.Join(Environment.NewLine, differences));
    }

    // The first difference between the old plan and a tier of the plan for one target, or null. Each read gets a block
    // decoded for it alone, so a column cache that one plan fills does not serve another.
    private static string Compare<T>(string columnType)
    {
        foreach ((int start, int count, long rowOffset) in Windows)
        {
            (T[] Values, string Failure) expected = Read<T>(columnType, block => PocoReadPlan<Row<T>>.BuildLegacy(PocoTypeDescriptor<Row<T>>.Build(), block), start, count, rowOffset);
            foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
            {
                (T[] Values, string Failure) actual = Read<T>(columnType, block => PocoReadPlan<Row<T>>.Build(PocoTypeDescriptor<Row<T>>.Build(), block, tier), start, count, rowOffset);
                string difference = Difference(expected, actual);
                if (difference is not null)
                {
                    return $"{tier}, rows [{start}, {start + count}) as rows from {rowOffset}: {difference}";
                }
            }
        }

        return null;
    }

    // The outcome of one read: the values, or the refusal of the plan or the failure of the read as text.
    private static (T[] Values, string Failure) Read<T>(string columnType, Func<Block, PocoReadPlan<Row<T>>> build, int start, int count, long rowOffset)
    {
        using Block block = ReadRulesTests.DecodeSample(columnType);
        PocoReadPlan<Row<T>> plan;
        try
        {
            plan = build(block);
        }
        catch (InvalidOperationException e)
        {
            return (null, $"refused: {e.Message}");
        }

        var rows = new Row<T>[count];
        try
        {
            plan.Materialize(block, rows, start, count, rowOffset);
        }
        catch (Exception e)
        {
            return (null, $"failed: {e.GetType().Name}: {e.Message} (inner exception: {e.InnerException?.GetType().Name ?? "none"})");
        }

        return (Array.ConvertAll(rows, row => row.Value), null);
    }

    private static string Difference<T>((T[] Values, string Failure) expected, (T[] Values, string Failure) actual)
    {
        if (expected.Failure is not null || actual.Failure is not null)
        {
            return expected.Failure == actual.Failure
                ? null
                : $"the old plan gives {expected.Failure ?? "values"}, and the plan gives {actual.Failure ?? "values"}.";
        }

        for (int i = 0; i < expected.Values.Length; i++)
        {
            string difference = ValueComparer.Difference(expected.Values[i], actual.Values[i]);
            if (difference is not null)
            {
                return $"row {i}: {difference}";
            }
        }

        return null;
    }
}
