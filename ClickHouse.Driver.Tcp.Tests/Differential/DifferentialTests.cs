using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The differential tests: for every case of <see cref="DifferentialCases"/>, each candidate arm of
/// <see cref="DifferentialRegistry.Current"/> must give the outcome of the client's entry point of its tier (the
/// baseline). The POCO reads run their trees through <c>Emit</c> (the baseline) and through <c>Fill</c>, so the two
/// paths must give the same outcomes. No server is necessary. See <see cref="DifferentialEngine"/>.
/// </summary>
[TestFixture]
public class DifferentialTests
{
    /// <summary>
    /// The baseline outcomes for all rows, counted by tier and kind, with an answer counted by its value. A change of
    /// what the client accepts, or of the case list, changes these counts.
    /// </summary>
    private static readonly Dictionary<(Tier Tier, string Kind), int> ExpectedBaselineOutcomes = new()
    {
        [(Tier.ReadAs, "Failed")] = 23,
        [(Tier.ReadAs, "Refused")] = 19,
        [(Tier.ReadAs, "Values")] = 545,
        [(Tier.Poco, "Failed")] = 23,
        [(Tier.Poco, "Refused")] = 19,
        [(Tier.Poco, "Values")] = 545,
        [(Tier.CanRead, "Answer False")] = 19,
        [(Tier.CanRead, "Answer True")] = 568,
        [(Tier.Write, "Bytes")] = 815,
        [(Tier.Write, "Unavailable")] = 42,
        [(Tier.CanWrite, "Answer False")] = 43,
        [(Tier.CanWrite, "Answer True")] = 814,
        [(Tier.PocoWrite, "Bytes")] = 789,
        [(Tier.PocoWrite, "Failed")] = 2,
        [(Tier.PocoWrite, "Refused")] = 24,
        [(Tier.PocoWrite, "Unavailable")] = 42,
        [(Tier.PocoCanWrite, "Answer False")] = 43,
        [(Tier.PocoCanWrite, "Answer True")] = 814,
        [(Tier.UntypedWrite, "Bytes")] = 789,
        [(Tier.UntypedWrite, "Failed")] = 2,
        [(Tier.UntypedWrite, "Refused")] = 24,
        [(Tier.UntypedWrite, "Unavailable")] = 42,
        [(Tier.UntypedCanWrite, "Answer False")] = 43,
        [(Tier.UntypedCanWrite, "Answer True")] = 814,
    };

    [TestCaseSource(typeof(DifferentialCases), nameof(DifferentialCases.All))]
    public void Run_Case_EveryCandidateGivesTheBaselineOutcome(DifferentialCase testCase)
    {
        CaseReport report = DifferentialEngine.ForCurrentRegistry(testCase);

        Assert.That(report.Mismatches, Is.Empty, string.Join(Environment.NewLine, report.Mismatches));
    }

    [Test]
    public void All_EachSource_HasTheExpectedNumberOfCases()
    {
        ILookup<CaseSource, DifferentialCase> bySource = DifferentialCases.All().ToLookup(c => c.Source);

        Assert.Multiple(() =>
        {
            Assert.That(bySource[CaseSource.InsertRoundTrip].Count(), Is.EqualTo(DifferentialCases.InsertRoundTripCount), "InsertRoundTripCase.CasesFor(TcpFeature.All)");
            Assert.That(bySource[CaseSource.CompositeLiftMatrix].Count(), Is.EqualTo(DifferentialCases.CompositeLiftMatrixCount), "CompositeLiftMatrixTests.Cases()");
            Assert.That(bySource[CaseSource.ColumnReadProjection].Count(), Is.EqualTo(DifferentialCases.ColumnReadProjectionCount), "the read targets table of DifferentialCases");
            Assert.That(bySource[CaseSource.ColumnReadScenario].Count(), Is.EqualTo(DifferentialCases.ColumnReadScenarioCount), "the scenarios of DifferentialCases");
            Assert.That(bySource.Sum(g => g.Count()), Is.EqualTo(DifferentialCases.All().Count()), "every case has one of these sources");
        });
    }

    [Test]
    public void Run_EveryCase_RunsEachReadAndWriteFacetOnATailThatStartsAboveZero()
    {
        string[] withoutTail = DifferentialCases.All()
            .Select(DifferentialEngine.ForCurrentRegistry)
            .SelectMany(report => report.Facets
                .Where(f => !f.Facet.IsAnswer && (report.TailStart <= 0 || f.Baseline?.Tail is null))
                .Select(f => f.Facet.ToString()))
            .ToArray();

        Assert.That(withoutTail, Is.Empty);
    }

    [Test]
    public void All_CaseIds_AreUnique()
    {
        string[] duplicates = DifferentialCases.All().GroupBy(c => c.Id).Where(g => g.Count() > 1).Select(g => g.Key).ToArray();

        Assert.That(duplicates, Is.Empty);
    }

    [Test]
    public void Current_RegistryAgainstTheCaseList_HasNoProblem()
    {
        List<string> problems = DifferentialRegistry.Current.Validate(DifferentialCases.All());

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    [Test]
    public void Run_EveryCase_GivesTheExpectedBaselineOutcomeCounts()
    {
        Dictionary<(Tier Tier, string Kind), int> actual = DifferentialCases.All()
            .SelectMany(c => DifferentialEngine.ForCurrentRegistry(c).Facets)
            .Where(f => f.Baseline is not null)
            .GroupBy(f => (f.Facet.Tier, KindOf(f.Baseline.All)))
            .ToDictionary(g => g.Key, g => g.Count());

        Assert.That(actual, Is.EquivalentTo(ExpectedBaselineOutcomes), "The counts are:" + Environment.NewLine + Describe(actual));
    }

    [Test]
    public void Current_FirstCandidateOfEachFacet_IsTheClientsEntryPoint()
    {
        var client = new Dictionary<Tier, Arm>
        {
            [Tier.ReadAs] = ClientArms.ReadAs,
            [Tier.Poco] = ClientArms.Poco,
            [Tier.CanRead] = ClientArms.CanRead,
            [Tier.Write] = ClientArms.Write,
            [Tier.CanWrite] = ClientArms.CanWrite,
            [Tier.PocoWrite] = ClientArms.PocoWrite,
            [Tier.PocoCanWrite] = ClientArms.PocoCanWrite,
            [Tier.UntypedWrite] = ClientArms.UntypedWrite,
            [Tier.UntypedCanWrite] = ClientArms.UntypedCanWrite,
        };
        string[] others = DifferentialCases.All()
            .SelectMany(c => c.Facets())
            .Select(f => (Facet: f, Arm: DifferentialRegistry.Current.CandidatesFor(f).FirstOrDefault()))
            .Where(x => !ReferenceEquals(x.Arm, client[x.Facet.Tier]))
            .Select(x => $"{x.Facet}: {x.Arm?.Name ?? "no candidate"}")
            .ToArray();

        Assert.That(others, Is.Empty);
    }

    [Test]
    public void Run_EveryCase_CanReadIsWhetherReadAsAndPocoMappingAcceptTheTarget()
    {
        string[] disagreements = Facets(Tier.CanRead)
            .SelectMany(x => new[] { Tier.ReadAs, Tier.Poco }
                .Select(tier => (Tier: tier, Read: Baseline(x.Report, tier, x.Result.Facet.Target)))
                .Where(read => x.Result.Baseline.All.Answer != (read.Read.All.Kind != OutcomeKind.Refused))
                .Select(read => $"{x.Result.Facet}: {x.Result.Baseline.All}; {read.Tier}: {read.Read.All}"))
            .ToArray();

        Assert.That(disagreements, Is.Empty);
    }

    // A Nested column is written only from a column of its own layout (a column that a query read, or a dense
    // NestedColumn), never from a CLR element type, so CanWrite says false for a type with a Nested in it, although
    // the write of such a column gives bytes.
    [Test]
    public void Run_EveryCase_CanWriteIsWhetherTheInsertAcceptsTheColumn()
    {
        string[] disagreements = Facets(Tier.CanWrite)
            .Where(x => !x.Report.Case.ColumnType.Contains("Nested(", StringComparison.Ordinal))
            .Select(x => (x.Result, Write: Baseline(x.Report, Tier.Write, x.Result.Facet.Input.Label)))
            .Where(x => x.Write.All.Kind != OutcomeKind.Unavailable && x.Result.Baseline.All.Answer != (x.Write.All.Kind != OutcomeKind.Refused))
            .Select(x => $"{x.Result.Facet}: {x.Result.Baseline.All}; Write: {x.Write.All}")
            .ToArray();

        Assert.That(disagreements, Is.Empty);
    }

    // A decoded column is written from its own storage (decision D3), so it gives back the bytes that it was read from.
    // The tail is checked by DecodedWriteDifferences.
    [Test]
    public void Run_EveryCase_TheDecodedColumnWritesTheBytesItWasReadFrom()
    {
        string[] differences = Facets(Tier.Write)
            .Where(x => x.Result.Facet.Input.Kind == WriteInputKind.Decoded)
            .Select(x => (x.Result, Source: Baseline(x.Report, Tier.Write, x.Report.Case.WriteInputs[0].Label)))
            .Select(x => (x.Result, Difference: Outcome.Difference(x.Source.All, x.Result.Baseline.All)))
            .Where(x => x.Difference is not null)
            .Select(x => $"{x.Result.Facet}: {x.Difference}")
            .ToArray();

        Assert.That(differences, Is.Empty);
    }

    // A decoded column is a column that its codec writes from its storage (decision D3): the insert gives it to the codec,
    // with no converter tree. Its element type is the canonical CLR type of the codec.
    [Test]
    public void Run_EveryCase_TheCodecWritesTheDecodedColumnFromItsStorage()
    {
        string[] failures = DifferentialCases.All()
            .Select(DifferentialEngine.ForCurrentRegistry)
            .Select(report => (report.Case, Source: Baseline(report, Tier.Write, report.Case.WriteInputs[0].Label).All))
            .Where(x => x.Source?.Kind == OutcomeKind.Bytes)
            .Select(x => StorageFailure(x.Case, x.Source.Bytes))
            .Where(failure => failure is not null)
            .ToArray();

        Assert.That(failures, Is.Empty);
    }

    [Test]
    public void Run_EveryCase_EveryWriteOfTheDecodedColumnGivesTheValuesOfTheSourceRows()
    {
        string[] differences = DifferentialCases.All()
            .Select(DifferentialEngine.ForCurrentRegistry)
            .SelectMany(DecodedWriteDifferences)
            .ToArray();

        Assert.That(differences, Is.Empty);
    }

    /// <summary>
    /// For each write of the decoded column of a case (the columnar insert, which writes it from its storage, and the row
    /// inserts, which gather its values), for all rows and for the tail: the bytes decode to the values of the same rows
    /// of the source column. Equal bytes need no decode. The bytes can differ and still be right: the write of a slice of
    /// a decoded LowCardinality column keeps the dictionary of the whole column, and a row insert builds the dictionary
    /// of its own values.
    /// </summary>
    /// <param name="report">The report of a case.</param>
    /// <returns>One message for each write that gives other values.</returns>
    internal static IEnumerable<string> DecodedWriteDifferences(CaseReport report)
    {
        DifferentialCase testCase = report.Case;
        RangeOutcomes source = Baseline(report, Tier.Write, testCase.WriteInputs[0].Label);
        int tailRows = testCase.RowCount == 1 ? 1 : testCase.RowCount - report.TailStart;
        foreach (FacetResult result in report.Facets.Where(f => f.Facet.Input?.Kind == WriteInputKind.Decoded && Facet.WritesBytes(f.Facet.Tier) && f.Baseline is not null))
        {
            foreach (Rows rows in new[] { Rows.All, Rows.Tail })
            {
                Outcome expected = source.For(rows);
                Outcome actual = result.Baseline.For(rows);
                if (expected?.Kind != OutcomeKind.Bytes || actual?.Kind != OutcomeKind.Bytes || Outcome.Difference(expected, actual) is null)
                {
                    continue;
                }

                int count = rows == Rows.All ? testCase.RowCount : tailRows;
                object[] expectedValues = DecodeValues(testCase.ColumnType, expected.Bytes, count);
                object[] actualValues = DecodeValues(testCase.ColumnType, actual.Bytes, count);
                for (int row = 0; row < count; row++)
                {
                    if (ValueComparer.Difference(expectedValues[row], actualValues[row]) is string difference)
                    {
                        yield return $"{result.Facet} {rows}, row {row} of the write: {difference}";
                        break;
                    }
                }
            }
        }
    }

    // The values of the rows that the bytes of a column write hold (the state prefix and the body).
    private static object[] DecodeValues(string columnType, byte[] bytes, int rows)
    {
        using IColumn column = Decode(columnType, bytes, rows);
        return Enumerable.Range(0, rows).Select(column.GetValue).ToArray();
    }

    // Why the codec of the case does not write the column that it decodes from the bytes from its storage, or null.
    private static string StorageFailure(DifferentialCase testCase, byte[] bytes)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(testCase.ColumnType, DifferentialEngine.Context);
        using IColumn column = Decode(testCase.ColumnType, bytes, testCase.RowCount);
        if (!codec.WritesFromStorage(column))
        {
            return $"{testCase.Id}: the codec does not write the decoded {column.GetType().Name} from its storage.";
        }

        return column.ElementType == codec.ElementType
            ? null
            : $"{testCase.Id}: the decoded column has the element type {column.ElementType}, and the codec {codec.ElementType}.";
    }

    // The column that a query reads from the bytes of a column write (the state prefix and the body).
    private static IColumn Decode(string columnType, byte[] bytes, int rows)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, DifferentialEngine.Context);
        using var stream = new MemoryStream(bytes);
        using var reader = new ClickHouseBinaryReader(stream);
        codec.ReadStatePrefixAsync(reader, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        return codec.ReadColumnAsync(reader, "value", columnType, rows, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    [Test]
    public void Run_EveryCase_TheRowAnswersAreWhetherTheRowInsertsAcceptTheColumn()
    {
        string[] disagreements = new[] { (Answer: Tier.PocoCanWrite, Write: Tier.PocoWrite), (Answer: Tier.UntypedCanWrite, Write: Tier.UntypedWrite) }
            .SelectMany(pair => Facets(pair.Answer)
                .Select(x => (x.Result, Write: Baseline(x.Report, pair.Write, x.Result.Facet.Input.Label)))
                .Where(x => x.Write.All.Kind != OutcomeKind.Unavailable && x.Result.Baseline.All.Answer != (x.Write.All.Kind != OutcomeKind.Refused))
                .Select(x => $"{x.Result.Facet}: {x.Result.Baseline.All}; {pair.Write}: {x.Write.All}"))
            .ToArray();

        Assert.That(disagreements, Is.Empty);
    }

    // A column read back as another CLR type holds the same values, so it writes the bytes of the source column, except
    // where the CLR type holds less: the 100-nanosecond ticks of DateTime, DateTimeOffset, TimeSpan and TimeOnly cannot
    // hold the counts of a DateTime64 or a Time64 of a scale above 7.
    [Test]
    public void Run_EveryCase_AReadBackColumnWritesTheBytesOfTheSourceColumn()
    {
        string[] differences = Facets(Tier.Write)
            .Where(x => x.Result.Facet.Input.Kind == WriteInputKind.ReadBack && !HoldsCountsFinerThanTicks(TypeParser.Parse(x.Report.Case.ColumnType)))
            .Select(x => (x.Result, Source: Baseline(x.Report, Tier.Write, x.Report.Case.WriteInputs[0].Label)))
            .SelectMany(x => new[] { Rows.All, Rows.Tail }
                .Select(rows => (Rows: rows, ReadBack: x.Result.Baseline.For(rows), Source: x.Source.For(rows)))
                .Where(r => r.ReadBack?.Kind == OutcomeKind.Bytes)
                .Select(r => (r.Rows, Difference: Outcome.Difference(r.Source, r.ReadBack)))
                .Where(r => r.Difference is not null)
                .Select(r => $"{x.Result.Facet} {r.Rows}: {r.Difference}"))
            .ToArray();

        Assert.That(differences, Is.Empty);
    }

    [Test]
    public void Run_EveryCase_PocoMappingReadsWhatReadAsReads()
    {
        string[] differences = Facets(Tier.Poco)
            .Select(x => (x.Result, ReadAs: Baseline(x.Report, Tier.ReadAs, x.Result.Facet.Target)))
            .SelectMany(x => new[] { Rows.All, Rows.Tail }
                .Select(rows => (Rows: rows, Poco: x.Result.Baseline.For(rows), ReadAs: x.ReadAs.For(rows)))
                .Where(r => r.Poco is not null && r.ReadAs is not null)
                .Select(r => (r.Rows, Difference: r.Poco.Kind != r.ReadAs.Kind ? $"{r.Poco.Kind}, ReadAs {r.ReadAs.Kind}"
                    : r.Poco.Kind == OutcomeKind.Values ? Outcome.Difference(r.ReadAs, r.Poco) : null))
                .Where(r => r.Difference is not null)
                .Select(r => $"{x.Result.Facet} {r.Rows}: {r.Difference}"))
            .ToArray();

        Assert.That(differences, Is.Empty);
    }

    // A decoded input is not compared: a row insert gathers its values, so it builds a LowCardinality dictionary and the
    // Dynamic types again from the values, where the columnar insert writes the column from its storage.
    [Test]
    public void Run_EveryCase_TheRowInsertsWriteTheBytesOfTheColumnarInsert()
    {
        string[] differences = Facets(Tier.PocoWrite).Concat(Facets(Tier.UntypedWrite))
            .Where(x => x.Result.Facet.Input.Kind != WriteInputKind.Decoded)
            .Select(x => (x.Result, Columnar: Baseline(x.Report, Tier.Write, x.Result.Facet.Input.Label)))
            .SelectMany(x => new[] { Rows.All, Rows.Tail }
                .Select(rows => (Rows: rows, Row: x.Result.Baseline.For(rows), Columnar: x.Columnar.For(rows)))
                .Where(r => r.Row?.Kind == OutcomeKind.Bytes && r.Columnar?.Kind == OutcomeKind.Bytes)
                .Select(r => (r.Rows, Difference: Outcome.Difference(r.Columnar, r.Row)))
                .Where(r => r.Difference is not null)
                .Select(r => $"{x.Result.Facet} {r.Rows}: {r.Difference}"))
            .ToArray();

        Assert.That(differences, Is.Empty);
    }

    private static bool HoldsCountsFinerThanTicks(TypeNode node)
        => (node.Name is "DateTime64" or "Time64") && node.Arguments.Count > 0 && int.Parse(node.Arguments[0].Name, CultureInfo.InvariantCulture) > 7
            || node.Arguments.Any(HoldsCountsFinerThanTicks);

    // The facets of a tier in every case, with the report of their case.
    private static IEnumerable<(CaseReport Report, FacetResult Result)> Facets(Tier tier)
        => DifferentialCases.All()
            .Select(DifferentialEngine.ForCurrentRegistry)
            .SelectMany(report => report.Facets.Where(f => f.Facet.Tier == tier).Select(f => (report, f)));

    private static RangeOutcomes Baseline(CaseReport report, Tier tier, Type target)
        => report.Facets.Single(f => f.Facet.Tier == tier && f.Facet.Target == target).Baseline;

    private static RangeOutcomes Baseline(CaseReport report, Tier tier, string inputLabel)
        => report.Facets.Single(f => f.Facet.Tier == tier && f.Facet.Input?.Label == inputLabel).Baseline;

    private static string KindOf(Outcome outcome) => outcome.Kind == OutcomeKind.Answer ? $"Answer {outcome.Answer}" : outcome.Kind.ToString();

    // The counts as C# source, in the form of ExpectedBaselineOutcomes.
    private static string Describe(Dictionary<(Tier Tier, string Kind), int> counts)
    {
        var text = new StringBuilder();
        foreach (KeyValuePair<(Tier Tier, string Kind), int> count in counts.OrderBy(c => c.Key.Tier).ThenBy(c => c.Key.Kind, StringComparer.Ordinal))
        {
            text.AppendLine($"[(Tier.{count.Key.Tier}, \"{count.Key.Kind}\")] = {count.Value},");
        }

        return text.ToString();
    }
}
