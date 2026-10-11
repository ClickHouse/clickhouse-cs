using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using ClickHouse.Driver.Tcp.Tests.Types;
using ClickHouse.Driver.Tcp.Types;
using static ClickHouse.Driver.Tcp.Tests.Differential.DifferentialEngine;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The differential tests, offline: every case of <see cref="DifferentialCases"/> runs through every read and write tier
/// of the client, for all rows and for a tail that starts above row 0 (<see cref="DifferentialEngine"/>), and the tiers
/// must agree by one rule (<see cref="CheckTiers"/>): a read or a write fails exactly when one of its rows fails alone,
/// with the failure of one of those rows (when several rows fail, a tier can name any one of them), and else gives the
/// values of its rows; tiers that read or write the same rows give the same kinds of outcome and the same values or bytes.
/// Values are compared strictly (<see cref="ValueComparer"/>).
/// </summary>
[TestFixture]
public class DifferentialTests
{
    /// <summary>The answers of <c>CanRead</c> for the read targets and of <c>CanWrite</c> for the write inputs that are not decoded, over the case list.</summary>
    private static readonly (int CanReadTrue, int CanReadFalse, int CanWriteTrue, int CanWriteFalse) ExpectedAnswers = (574, 19, 537, 31);

    /// <summary>
    /// The row inserts that the tests do not compare with the insert, with the kind of their outcome for all rows, for every
    /// input of the case. The <c>Geometry</c> case builds its column from decoded columns, so the insert writes it from its
    /// storage; a row insert gathers its values as objects through the converter layer, and a ring and a line string (and
    /// a polygon and a multi line string) have one CLR type, so the Variant writer refuses those rows. They still follow
    /// the rule of their own rows.
    /// </summary>
    private static readonly Dictionary<(string Case, string Tier), OutcomeKind> RowInsertOutcomes = new()
    {
        [("InsertRoundTrip: Geometry", ClientArms.PocoWrite.Name)] = OutcomeKind.Failed,
        [("InsertRoundTrip: Geometry", ClientArms.PocoWriteDelegate.Name)] = OutcomeKind.Failed,
        [("InsertRoundTrip: Geometry", ClientArms.UntypedWrite.Name)] = OutcomeKind.Failed,
    };

    private static readonly WriteArm[] RowInserts = { ClientArms.PocoWrite, ClientArms.PocoWriteDelegate, ClientArms.UntypedWrite };

    public static IEnumerable<DifferentialCase> Cases() => DifferentialCases.All();

    public static IEnumerable<DifferentialCase> ScenarioCases() => DifferentialCases.All().Where(c => c.Scenario is not null);

    /// <summary>
    /// <c>Block.ReadAs</c>, POCO mapping (the compiled loop) and POCO mapping in the tier of a runtime without dynamic code
    /// (<c>Bind</c>/<c>Fill</c>), by the rule of <see cref="CheckTiers"/>; <c>CanRead</c> is whether they read. The view of
    /// <c>Block.ReadAs</c> converts every row of the column, so its rule takes every row, also for the tail.
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void Read_Case_TheTiersReadTheSameValues(DifferentialCase testCase)
    {
        var problems = new List<string>();
        foreach (Type target in testCase.ReadTargets)
        {
            TierRun Tier(ReadArm arm, Texts texts) => new(arm.Name, texts, arm.ReadsEveryRow, r => Read(arm, testCase, target, r), p => ReadRows(arm, testCase, target, p));
            TierRun readAs = Tier(ClientArms.ReadAs, Texts.Exact), poco = Tier(ClientArms.Poco, Texts.Exact), fill = Tier(ClientArms.PocoFill, Texts.Exact);
            CheckTiers(problems, TypeNames.Of(target), testCase, new[] { readAs, poco, fill }, new[] { new Pair(readAs, poco, Texts.TypeOnly), new Pair(poco, fill, Texts.Exact) });

            bool canRead = ClickHouseTcpTypes.CanRead(testCase.ColumnType, target);
            Check(problems, $"{TypeNames.Of(target)}: CanRead is {canRead}, Block.ReadAs {readAs.Range(All(testCase))}", canRead == (readAs.Range(All(testCase)).Kind != OutcomeKind.Refused));
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// For each write input that the case builds or reads back, by the rule of <see cref="CheckTiers"/>: the insert, an
    /// independent write that gathers the values of each slice through the indexer (so the two give the same bytes only
    /// when the insert slices right), the POCO insert in both gather tiers and the untyped insert. A row insert refuses
    /// what <c>CanWrite</c> refuses, and has the outcomes of <see cref="RowInsertOutcomes"/>. <c>CanWrite</c> is whether
    /// each insert takes the column. A column read back as another CLR type writes the bytes of the source column, except
    /// where the CLR type holds less (a count finer than 100 ns).
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void Write_Case_TheInsertsWriteTheSameBytes(DifferentialCase testCase)
    {
        var problems = new List<string>();
        foreach (WriteInput input in testCase.WriteInputs.Where(i => i.Kind != WriteInputKind.Decoded))
        {
            TierRun[] tiers = WriteTiers(testCase, input, ClientArms.IndexerWrite);
            TierRun insert = tiers[0];
            if (insert.Range(All(testCase)).Kind == OutcomeKind.Unavailable)
            {
                continue;
            }

            bool canWrite = ClickHouseTcpTypes.CanWrite(testCase.ColumnType, input.ElementType);
            CheckTiers(problems, input.Label, testCase, tiers, WritePairs(testCase, input, tiers, canWrite));
            CheckPinned(problems, input.Label, testCase, tiers);
            foreach (RowRange range in new[] { All(testCase), Tail(testCase) })
            {
                if (input.Kind == WriteInputKind.ReadBack && !HoldsCountsFinerThanTicks(TypeParser.Parse(testCase.ColumnType)) && insert.Range(range).Kind == OutcomeKind.Bytes)
                {
                    Add(problems, $"{input} {range}: the insert against the source column", Outcome.Difference(Write(ClientArms.Write, testCase, testCase.WriteInputs[0], range), insert.Range(range)));
                }
            }

            // A Nested column is written only from a column of its own layout, never from a CLR element type, so CanWrite
            // is false for a type with a Nested in it, although the insert writes such a column.
            bool nested = testCase.ColumnType.Contains("Nested(", StringComparison.Ordinal);
            foreach (TierRun tier in tiers.Where(t => t != tiers[1] && (t != insert || !nested)))
            {
                Check(problems, $"{input}: CanWrite is {canWrite}, {tier.Name} {tier.Range(All(testCase))}", canWrite == (tier.Range(All(testCase)).Kind != OutcomeKind.Refused));
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The decoded column of a case is a column that its codec writes from its storage. Its insert gives back the bytes
    /// that it was read from. The insert and the row inserts of it follow the rule of <see cref="CheckTiers"/>, a row
    /// insert refuses it where <c>CanWrite</c> is false, and every write of it, for all rows and for the tail, gives
    /// bytes that decode to the values of the same rows of the source column. Equal bytes need no decode: the write of a
    /// slice of a LowCardinality column keeps the dictionary of the whole column, and a row insert builds the dictionary
    /// of its own values.
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void Write_Case_TheDecodedColumnWritesItsRows(DifferentialCase testCase)
    {
        var problems = new List<string>();
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(testCase.ColumnType, Context);
        using (IColumn decoded = DecodeColumn(testCase.ColumnType, SourceBytes(testCase), testCase.RowCount))
        {
            Check(problems, $"the codec does not write the decoded {decoded.GetType().Name} from its storage", codec.CanWrite(decoded));
            Check(problems, $"the decoded column has the element type {decoded.ElementType}, and the codec {codec.ElementType}", decoded.ElementType == codec.ElementType);
        }

        WriteInput input = testCase.WriteInputs.FirstOrDefault(i => i.Kind == WriteInputKind.Decoded);
        if (input is not null)
        {
            TierRun[] tiers = WriteTiers(testCase, input, indexer: null);
            CheckTiers(problems, input.Label, testCase, tiers, WritePairs(testCase, input, tiers, ClickHouseTcpTypes.CanWrite(testCase.ColumnType, input.ElementType), sameBytes: false));
            CheckPinned(problems, input.Label, testCase, tiers);
            Add(problems, "the insert of the decoded column against the source bytes", Outcome.Difference(Outcome.OfBytes(SourceBytes(testCase)), tiers[0].Range(All(testCase))));
            foreach (RowRange range in new[] { All(testCase), Tail(testCase) })
            {
                Outcome source = Write(ClientArms.Write, testCase, testCase.WriteInputs[0], range);
                foreach (TierRun tier in tiers)
                {
                    Outcome written = tier.Range(range);
                    if (source.Kind == OutcomeKind.Bytes && written.Kind == OutcomeKind.Bytes && Outcome.Difference(source, written) is not null)
                    {
                        Add(problems, $"{tier.Name} {range}", RowsDifference(testCase.ColumnType, source.Bytes, written.Bytes, range.Count));
                    }
                }
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A scenario case gives the outcome that it states for <c>Block.ReadAs</c>, for all rows and for the tail.</summary>
    [TestCaseSource(nameof(ScenarioCases))]
    public void Read_Case_GivesTheStatedOutcome(DifferentialCase testCase)
    {
        ColumnReadScenario scenario = testCase.Scenario;
        Assert.Multiple(() =>
        {
            foreach (RowRange range in new[] { All(testCase), Tail(testCase) })
            {
                Outcome actual = Read(ClientArms.ReadAs, testCase, scenario.Target, range);
                if (scenario.ExceptionType is null)
                {
                    Assert.That(Outcome.Difference(Outcome.OfValues(scenario.Target, scenario.Expected.Cast<object>().Skip(range.CaseRow).ToArray()), actual), Is.Null, $"{range}: {actual}");
                }
                else
                {
                    Assert.That((actual.Kind, actual.ExceptionType), Is.EqualTo((OutcomeKind.Failed, scenario.ExceptionType)), $"{range}: {actual}");
                    Assert.That(scenario.MessageParts.Where(part => actual.Message?.Contains(part, StringComparison.Ordinal) != true), Is.Empty, $"{range}: {actual}");
                }
            }
        });
    }

    /// <summary>No case drops out of the case list, and no answer of <c>CanRead</c> or <c>CanWrite</c> changes, silently.</summary>
    [Test]
    public void All_Cases_AreThePinnedCasesWithThePinnedAnswers()
    {
        DifferentialCase[] cases = DifferentialCases.All().ToArray();
        ILookup<CaseSource, DifferentialCase> bySource = cases.ToLookup(c => c.Source);
        bool[] canRead = cases.SelectMany(c => c.ReadTargets.Select(t => ClickHouseTcpTypes.CanRead(c.ColumnType, t))).ToArray();
        bool[] canWrite = cases.SelectMany(c => c.WriteInputs.Where(i => i.Kind != WriteInputKind.Decoded).Select(i => ClickHouseTcpTypes.CanWrite(c.ColumnType, i.ElementType))).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(bySource[CaseSource.InsertRoundTrip].Count(), Is.EqualTo(DifferentialCases.InsertRoundTripCount), "InsertRoundTripCase.CasesFor(TcpFeature.All)");
            Assert.That(bySource[CaseSource.CompositeLiftMatrix].Count(), Is.EqualTo(DifferentialCases.CompositeLiftMatrixCount), "CompositeLiftMatrixTests.Cases()");
            Assert.That(bySource[CaseSource.ColumnReadProjection].Count(), Is.EqualTo(DifferentialCases.ColumnReadProjectionCount), "the read targets table of DifferentialCases");
            Assert.That(bySource[CaseSource.ColumnReadScenario].Count(), Is.EqualTo(DifferentialCases.ColumnReadScenarioCount), "the scenarios of DifferentialCases");
            Assert.That(bySource[CaseSource.FailureScenario].Count(), Is.EqualTo(DifferentialCases.FailureScenarioCount), "the failure scenarios of DifferentialCases");
            Assert.That(cases.GroupBy(c => c.Id).Where(g => g.Count() > 1).Select(g => g.Key), Is.Empty, "case ids that are not unique");
            Assert.That(
                (canRead.Count(a => a), canRead.Count(a => !a), canWrite.Count(a => a), canWrite.Count(a => !a)),
                Is.EqualTo(ExpectedAnswers),
                "(CanRead true, CanRead false, CanWrite true, CanWrite false)");
        });
    }

    [TestCase(0f, -0f)]
    [TestCase(double.NaN, 0d)]
    public void Difference_FloatingPointValuesWithOtherBits_AreDifferent(object expected, object actual)
        => Assert.That(ValueComparer.Difference(expected, actual), Is.Not.Null);

    [Test]
    public void Difference_ValuesOfTheSameContent_AreTheSame()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ValueComparer.Difference(float.NaN, float.NaN), Is.Null);
            Assert.That(ValueComparer.Difference((new byte[] { 1 }, "a"), (new byte[] { 1 }, "a")), Is.Null);
            Assert.That(ValueComparer.Difference(new KeyValuePair<string, byte[]>("k", new byte[] { 3 }), new KeyValuePair<string, byte[]>("k", new byte[] { 3 })), Is.Null);
            Assert.That(ValueComparer.Difference(new[] { new uint?[] { 1, null } }, new[] { new uint?[] { 1, null } }), Is.Null);
            Assert.That(ValueComparer.Difference(new[] { new uint?[] { 1, null } }, new[] { new uint?[] { 1, 2 } }), Is.EqualTo("element 0: element 1: 2, not null"));
        });
    }

    [Test]
    public void Difference_ValuesThatEqualsCallsEqual_CanBeDifferent()
    {
        var unspecified = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified);

        Assert.Multiple(() =>
        {
            Assert.That(ValueComparer.Difference(unspecified, DateTime.SpecifyKind(unspecified, DateTimeKind.Utc)), Is.Not.Null, "the Kind differs");
            Assert.That(ValueComparer.Difference(new DateTimeOffset(unspecified, TimeSpan.Zero), new DateTimeOffset(unspecified.AddHours(1), TimeSpan.FromHours(1))), Is.Not.Null, "the offset differs");
            Assert.That(ValueComparer.Difference(1.0m, 1.00m), Is.Not.Null, "the scale differs");
            Assert.That(ValueComparer.Difference(new ClickHouseTcpDecimal(new BigInteger(10), 1), new ClickHouseTcpDecimal(new BigInteger(100), 2)), Is.Not.Null, "the scale differs");
            Assert.That(ValueComparer.Difference(1, 1L), Is.Not.Null, "the type differs");
        });
    }

    /// <summary>A range is refused with its rows, fails with the failure of one of its failing rows alone, and gives a result when none fails.</summary>
    [Test]
    public void RangeDifference_EachKindOfRange_FollowsItsRowsAlone()
    {
        Outcome bytes = Outcome.OfBytes(new byte[] { 1 });
        Outcome[] rows = { bytes, Outcome.Failure(new OverflowException("row 1")), bytes, Outcome.Failure(new OverflowException("row 3")) };

        Assert.Multiple(() =>
        {
            Assert.That(Outcome.RangeDifference(Outcome.Failure(new OverflowException("row 3")), rows, Texts.Exact), Is.Null, "one of the failing rows");
            Assert.That(Outcome.RangeDifference(Outcome.Failure(new OverflowException("row 2")), rows, Texts.Exact), Does.Contain("none of its rows"), "another message");
            Assert.That(Outcome.RangeDifference(Outcome.Failure(new OverflowException("row 2")), rows, Texts.NumbersMasked), Is.Null, "another number, masked");
            Assert.That(Outcome.RangeDifference(Outcome.Failure(new ArgumentException("row 1")), rows, Texts.TypeOnly), Does.Contain("none of its rows"), "another type");
            Assert.That(Outcome.RangeDifference(bytes, rows, Texts.Exact), Does.Contain("fail alone"), "a result although rows fail");
            Assert.That(Outcome.RangeDifference(Outcome.Failure(new OverflowException("row 1")), new[] { bytes, bytes }, Texts.Exact), Does.Contain("no row fails"), "a failure although no row fails");
            Assert.That(Outcome.RangeDifference(bytes, new[] { Outcome.Refusal(new ArgumentException("no")) }, Texts.Exact), Does.Contain("refused alone"), "a result although the rows are refused");
        });
    }

    /// <summary>One tier of a family: its outcome for each range and the outcome of each row alone, each computed once.</summary>
    private sealed class TierRun(string name, Texts texts, bool readsEveryRow, Func<RowRange, Outcome> range, Func<bool, Outcome[]> rows)
    {
        private readonly Dictionary<RowRange, Outcome> ranges = new();
        private readonly Dictionary<bool, Outcome[]> rowsAlone = new();

        public string Name { get; } = name;

        /// <summary>How a failure of a range is compared with the failures of its rows alone.</summary>
        public Texts Texts { get; } = texts;

        /// <summary>Whether a range reads every row of the column (<see cref="ReadArm.ReadsEveryRow"/>).</summary>
        public bool ReadsEveryRow { get; } = readsEveryRow;

        public Outcome Range(RowRange of) => ranges.TryGetValue(of, out Outcome outcome) ? outcome : ranges[of] = range(of);

        /// <summary>The outcome of each row of the column, alone; of the column with a preceding row when preceded.</summary>
        public Outcome[] Rows(bool preceded) => rowsAlone.TryGetValue(preceded, out Outcome[] outcomes) ? outcomes : rowsAlone[preceded] = rows(preceded);
    }

    /// <summary>
    /// Two tiers that read or write the same rows. <see cref="Expected"/> gives the kind of outcome of the second from the
    /// outcome of the first; by default the same kind. <see cref="SameResults"/> is false for two writes whose bytes can
    /// differ for the same values (a row insert builds the LowCardinality dictionary of its own values).
    /// </summary>
    private sealed record Pair(TierRun First, TierRun Second, Texts Texts, Func<Outcome, OutcomeKind> Expected = null, bool SameResults = true)
    {
        /// <summary>The rows (of the column without, and with, a preceding row) whose failure the second words in its own terms, so only the type is compared.</summary>
        public Func<bool, int, bool> OwnWording { get; init; }

        // The second outcome against the first: the expected kind; then, for the same kind, the same values or bytes, and the
        // same failure by texts (not compared when texts is null: two ranges can fail on different rows).
        public string Difference(Outcome first, Outcome second, Texts? texts)
        {
            OutcomeKind expected = Expected?.Invoke(first) ?? first.Kind;
            if (second.Kind != expected)
            {
                return $"{second}; {First.Name} {first}";
            }

            return second.Kind != first.Kind ? null : second.Kind switch
            {
                OutcomeKind.Values or OutcomeKind.Bytes => SameResults ? Outcome.Difference(first, second) : null,
                OutcomeKind.Failed when texts is Texts compare => Outcome.FailureDifference(first, second, compare),
                _ => null,
            };
        }
    }

    /// <summary>
    /// The rule of every family, for every tier and both ranges (all rows and the tail):
    /// <list type="bullet">
    /// <item>each range follows <see cref="Outcome.RangeDifference"/> against the rows that it reads or writes, each read or
    /// written alone by the same tier: refused with its rows, failed with the failure of one of its failing rows, a result
    /// when none of them fails; a range of values has the values of its rows alone;</item>
    /// <item>the two tiers of each pair give, for each row alone, the same kind of outcome (or the kind that the pair
    /// expects), the same values or bytes, and failures that are the same by the texts of the pair; and for each range
    /// that they both read or write, the same kind of outcome and the same values or bytes.</item>
    /// </list>
    /// </summary>
    private static void CheckTiers(List<string> problems, string label, DifferentialCase testCase, IReadOnlyList<TierRun> tiers, IEnumerable<Pair> pairs)
    {
        var ranges = new[] { All(testCase), Tail(testCase) };
        foreach (TierRun tier in tiers)
        {
            foreach (RowRange range in ranges)
            {
                Outcome[] rows = tier.Rows(range.Preceded);
                Outcome[] own = rows[range.Start..(range.Start + range.Count)];
                Outcome outcome = tier.Range(range);
                Add(problems, $"{label}: {tier.Name} {range}", Outcome.RangeDifference(outcome, tier.ReadsEveryRow ? rows : own, tier.Texts));
                Check(problems, $"{label}: {tier.Name} {range} broke a rule of its own: {outcome}", outcome.ExceptionType != typeof(ArmInvariantException));
                if (outcome.Kind == OutcomeKind.Values && Array.TrueForAll(own, row => row.Kind == OutcomeKind.Values))
                {
                    Add(problems, $"{label}: {tier.Name} {range} against its rows alone", Outcome.Difference(Outcome.OfValues(outcome.ValueType, own.SelectMany(row => row.Values).ToArray()), outcome));
                }
            }
        }

        foreach (Pair pair in pairs)
        {
            foreach (bool preceded in ranges.Select(range => range.Preceded).Distinct())
            {
                Outcome[] first = pair.First.Rows(preceded), second = pair.Second.Rows(preceded);
                for (int row = 0; row < first.Length; row++)
                {
                    Texts texts = pair.OwnWording?.Invoke(preceded, row) == true ? Texts.TypeOnly : pair.Texts;
                    Add(problems, $"{label}: {pair.Second.Name} against {pair.First.Name}, row {row} alone{(preceded ? " after a preceding row" : string.Empty)}", pair.Difference(first[row], second[row], texts));
                }
            }

            foreach (RowRange range in ranges.Where(_ => pair.First.ReadsEveryRow == pair.Second.ReadsEveryRow))
            {
                Add(problems, $"{label}: {pair.Second.Name} against {pair.First.Name}, {range}", pair.Difference(pair.First.Range(range), pair.Second.Range(range), texts: null));
            }
        }
    }

    // A row insert of RowInsertOutcomes has the pinned kind of outcome for all rows, so a pin cannot stay when it no longer
    // holds.
    private static void CheckPinned(List<string> problems, string label, DifferentialCase testCase, IEnumerable<TierRun> tiers)
    {
        foreach (TierRun tier in tiers)
        {
            if (RowInsertOutcomes.TryGetValue((testCase.Id, tier.Name), out OutcomeKind pinned) && tier.Range(All(testCase)).Kind != pinned)
            {
                problems.Add($"{label}: {tier.Name} {tier.Range(All(testCase))}, not the pinned {pinned}");
            }
        }
    }

    // The tiers of a write input: the insert, the write through the indexer when there is one, and the row inserts.
    private static TierRun[] WriteTiers(DifferentialCase testCase, WriteInput input, WriteArm indexer)
    {
        TierRun Tier(WriteArm arm, Texts texts) => new(arm.Name, texts, readsEveryRow: false, r => Write(arm, testCase, input, r), p => WriteRows(arm, testCase, input, p));
        return new[] { ClientArms.Write, indexer }.Where(arm => arm is not null).Select(arm => Tier(arm, Texts.Exact))
            .Concat(RowInserts.Select(arm => Tier(arm, Texts.NumbersMasked)))
            .ToArray();
    }

    // The insert against the write through the indexer and against each row insert, and the two POCO tiers against each
    // other. A row insert refuses what CanWrite refuses (a Nested column, which only a column of its own layout writes).
    // It refuses a null value itself when it gathers the row, in its own words, so only the type of that failure is
    // compared with the failure of the insert; any other failure comes from the converter layer, with the text of the
    // insert up to its numbers (a row insert names the row of its block). sameBytes is false for a decoded column, which
    // the insert writes from its storage and a row insert from its values; the family compares the values of those bytes.
    private static Pair[] WritePairs(DifferentialCase testCase, WriteInput input, TierRun[] tiers, bool canWrite, bool sameBytes = true)
    {
        TierRun insert = tiers[0];
        TierRun[] rows = tiers[^3..];
        OutcomeKind RowInsert(Outcome insertOutcome) => insertOutcome.Kind == OutcomeKind.Bytes && !canWrite ? OutcomeKind.Refused : insertOutcome.Kind;
        var pairs = new List<Pair> { new(rows[0], rows[1], Texts.Exact) };
        var nulls = new Dictionary<bool, bool[]>();
        bool IsNull(bool preceded, int row) => (nulls.TryGetValue(preceded, out bool[] of) ? of : nulls[preceded] = Nulls(testCase, input, preceded))[row];
        pairs.AddRange(new[] { rows[0], rows[2] }
            .Where(row => !RowInsertOutcomes.ContainsKey((testCase.Id, row.Name)))
            .Select(row => new Pair(insert, row, Texts.NumbersMasked, RowInsert, sameBytes) { OwnWording = IsNull }));
        if (tiers.Length == 5)
        {
            pairs.Add(new Pair(insert, tiers[1], Texts.Exact));
        }

        return pairs.ToArray();
    }

    // The first row whose values differ in two column writes of a type, or null.
    private static string RowsDifference(string columnType, byte[] expected, byte[] actual, int rows)
    {
        using IColumn expectedColumn = DecodeColumn(columnType, expected, rows);
        using IColumn actualColumn = DecodeColumn(columnType, actual, rows);
        for (int row = 0; row < rows; row++)
        {
            if (ValueComparer.Difference(expectedColumn.GetValue(row), actualColumn.GetValue(row)) is string difference)
            {
                return $"row {row} of the write: {difference}";
            }
        }

        return null;
    }

    // The 100-nanosecond ticks of DateTime, DateTimeOffset, TimeSpan and TimeOnly cannot hold the counts of a DateTime64 or
    // a Time64 of a scale above 7.
    private static bool HoldsCountsFinerThanTicks(TypeNode node)
        => (node.Name is "DateTime64" or "Time64") && node.Arguments.Count > 0 && int.Parse(node.Arguments[0].Name, CultureInfo.InvariantCulture) > 7
            || node.Arguments.Any(HoldsCountsFinerThanTicks);

    private static void Add(List<string> problems, string what, string difference)
    {
        if (difference is not null)
        {
            problems.Add($"{what}: {difference}");
        }
    }

    private static void Check(List<string> problems, string what, bool holds)
    {
        if (!holds)
        {
            problems.Add(what);
        }
    }
}
