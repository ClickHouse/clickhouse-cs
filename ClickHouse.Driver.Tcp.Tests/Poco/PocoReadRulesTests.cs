using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The two scatter tiers of the POCO read plan against each other, for the matrix of column types and CLR targets of
/// <see cref="ReadRulesTests"/> (enums, casts, nullable targets, tuples, maps, the array casts), which the differential
/// case list does not have. The <see cref="PocoScatterTier.Fill"/> tier must give the outcome of the
/// <see cref="PocoScatterTier.Emit"/> tier: the same refusal, the same values, or the same failure, for all the rows and
/// for a window that starts past the first row of the block and past the first row of the result.
/// </summary>
[TestFixture]
public class PocoReadRulesTests
{
    private static readonly (int Start, int Count, long RowOffset)[] Windows = { (0, 5, 0), (2, 3, 1_000) };

    private static IEnumerable<string> Types() => ReadRulesTests.ColumnTypes;

    [TestCaseSource(nameof(Types))]
    public void Materialize_EachTarget_GivesTheSameOutcomeInBothTiers(string columnType)
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

    // The first difference between the Emit tier and the Fill tier of the plan for one target, or null. Each read gets
    // a block decoded for it alone, so a column cache that one plan fills does not serve another.
    private static string Compare<T>(string columnType)
    {
        foreach ((int start, int count, long rowOffset) in Windows)
        {
            (T[] Values, string Failure) emit = Read<T>(columnType, PocoScatterTier.Emit, start, count, rowOffset);
            (T[] Values, string Failure) fill = Read<T>(columnType, PocoScatterTier.Fill, start, count, rowOffset);
            string difference = Difference(emit, fill);
            if (difference is not null)
            {
                return $"rows [{start}, {start + count}) as rows from {rowOffset}: {difference}";
            }
        }

        return null;
    }

    // The outcome of one read: the values, or the refusal of the plan or the failure of the read as text.
    private static (T[] Values, string Failure) Read<T>(string columnType, PocoScatterTier tier, int start, int count, long rowOffset)
    {
        using Block block = ReadRulesTests.DecodeSample(columnType);
        PocoReadPlan<Row<T>> plan;
        try
        {
            plan = PocoReadPlan<Row<T>>.Build(PocoTypeDescriptor<Row<T>>.Build(), block, tier);
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
                : $"the Emit tier gives {expected.Failure ?? "values"}, and the Fill tier gives {actual.Failure ?? "values"}.";
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
