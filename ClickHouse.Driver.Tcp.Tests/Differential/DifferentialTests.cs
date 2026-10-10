using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The differential tests: for every case of <see cref="DifferentialCases"/>, each candidate arm of
/// <see cref="DifferentialRegistry.Current"/> must give the outcome of the client's entry point of its tier (the
/// baseline), or the outcome that is declared for the facet. The converter arms run their trees through
/// <c>Fill</c> and through <c>Emit</c>, so the two paths cannot drift apart. No server is necessary. See
/// <see cref="DifferentialEngine"/>.
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
        [(Tier.ReadAs, "Values")] = 525,
        [(Tier.Poco, "Failed")] = 23,
        [(Tier.Poco, "Refused")] = 19,
        [(Tier.Poco, "Values")] = 525,
        [(Tier.CanRead, "Answer False")] = 19,
        [(Tier.CanRead, "Answer True")] = 548,
        [(Tier.Write, "Bytes")] = 789,
        [(Tier.Write, "Unavailable")] = 42,
        [(Tier.CanWrite, "Answer False")] = 43,
        [(Tier.CanWrite, "Answer True")] = 788,
        [(Tier.PocoWrite, "Bytes")] = 763,
        [(Tier.PocoWrite, "Failed")] = 2,
        [(Tier.PocoWrite, "Refused")] = 24,
        [(Tier.PocoWrite, "Unavailable")] = 42,
        [(Tier.PocoCanWrite, "Answer False")] = 43,
        [(Tier.PocoCanWrite, "Answer True")] = 788,
        [(Tier.UntypedWrite, "Bytes")] = 763,
        [(Tier.UntypedWrite, "Failed")] = 2,
        [(Tier.UntypedWrite, "Refused")] = 24,
        [(Tier.UntypedWrite, "Unavailable")] = 42,
        [(Tier.UntypedCanWrite, "Answer False")] = 43,
        [(Tier.UntypedCanWrite, "Answer True")] = 788,
    };

    [TestCaseSource(typeof(DifferentialCases), nameof(DifferentialCases.All))]
    public void Run_Case_EveryCandidateGivesTheBaselineOutcomeOrTheDeclaredOne(DifferentialCase testCase)
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
            Assert.That(bySource[CaseSource.ColumnReadProjection].Count(), Is.EqualTo(DifferentialCases.ColumnReadProjectionCount), "the column types of ColumnReadProjectionTests");
            Assert.That(bySource[CaseSource.ColumnReadScenario].Count(), Is.EqualTo(DifferentialCases.ColumnReadScenarioCount), "the scenarios of ColumnReadProjectionTests");
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
        string[] others = DifferentialCases.All()
            .SelectMany(c => c.Facets())
            .Select(f => (Facet: f, Arm: DifferentialRegistry.Current.CandidatesFor(f).FirstOrDefault()))
            .Where(x => x.Arm is null || !x.Arm.Name.StartsWith("Client.", StringComparison.Ordinal))
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
