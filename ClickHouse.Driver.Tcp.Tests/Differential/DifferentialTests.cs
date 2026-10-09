using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The differential tests: for every case of <see cref="DifferentialCases"/>, each candidate arm of
/// <see cref="DifferentialRegistry.Current"/> must give the outcome of the reference arm (the old path), or the
/// outcome of a deliberate change. No server is necessary. See <see cref="DifferentialEngine"/>.
/// </summary>
[TestFixture]
public class DifferentialTests
{
    /// <summary>
    /// The reference outcomes for all rows, counted by tier and kind, with an answer counted by its value. A change
    /// of the old path, or of the case list, changes these counts.
    /// </summary>
    private static readonly Dictionary<(Tier Tier, string Kind), int> ExpectedReferenceOutcomes = new()
    {
        [(Tier.ReadAs, "Refused")] = 30,
        [(Tier.ReadAs, "Values")] = 455,
        [(Tier.Poco, "Failed")] = 5,
        [(Tier.Poco, "Refused")] = 17,
        [(Tier.Poco, "Values")] = 463,
        [(Tier.CanRead, "Answer False")] = 30,
        [(Tier.CanRead, "Answer True")] = 455,
        [(Tier.Write, "Bytes")] = 715,
        [(Tier.Write, "Refused")] = 12,
        [(Tier.Write, "Unavailable")] = 22,
        [(Tier.CanWrite, "Answer False")] = 58,
        [(Tier.CanWrite, "Answer True")] = 691,
    };

    [TestCaseSource(typeof(DifferentialCases), nameof(DifferentialCases.All))]
    public void Run_Case_EveryCandidateGivesTheReferenceOutcomeOrItsDeliberateChange(DifferentialCase testCase)
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
        });
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
    public void Run_EveryCase_GivesTheExpectedReferenceOutcomeCounts()
    {
        Dictionary<(Tier Tier, string Kind), int> actual = DifferentialCases.All()
            .SelectMany(c => DifferentialEngine.ForCurrentRegistry(c).Facets)
            .Where(f => f.Reference is not null)
            .GroupBy(f => (f.Facet.Tier, KindOf(f.Reference.All)))
            .ToDictionary(g => g.Key, g => g.Count());

        Assert.That(actual, Is.EquivalentTo(ExpectedReferenceOutcomes), "The counts are:" + Environment.NewLine + Describe(actual));
    }

    private static string KindOf(Outcome outcome) => outcome.Kind == OutcomeKind.Answer ? $"Answer {outcome.Answer}" : outcome.Kind.ToString();

    // The counts as C# source, in the form of ExpectedReferenceOutcomes.
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
