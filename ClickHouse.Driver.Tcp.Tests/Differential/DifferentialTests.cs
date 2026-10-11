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
/// must agree. Values are compared strictly (<see cref="ValueComparer"/>). When several rows fail, a tier can name any
/// one of them, so a failure must be the failure of one of its rows (<see cref="Outcome.FailureDifference"/>).
/// </summary>
[TestFixture]
public class DifferentialTests
{
    /// <summary>The answers of <c>CanRead</c> for the read targets and of <c>CanWrite</c> for the write inputs that are not decoded, over the case list.</summary>
    private static readonly (int CanReadTrue, int CanReadFalse, int CanWriteTrue, int CanWriteFalse) ExpectedAnswers = (568, 19, 528, 31);

    public static IEnumerable<DifferentialCase> Cases() => DifferentialCases.All();

    public static IEnumerable<DifferentialCase> ScenarioCases() => DifferentialCases.All().Where(c => c.Scenario is not null);

    /// <summary>
    /// <c>Block.ReadAs</c>, POCO mapping (the compiled loop) and POCO mapping in the tier of a runtime without dynamic code
    /// (<c>Bind</c>/<c>Fill</c>) read the same values, or all refuse, or all fail; <c>CanRead</c> is whether they read;
    /// and each tier reads the same values for the tail alone as for those rows in the read of all rows.
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void Read_Case_TheTiersReadTheSameValues(DifferentialCase testCase)
    {
        var problems = new List<string>();
        foreach (Type target in testCase.ReadTargets)
        {
            string label = TypeNames.Of(target);
            var ranges = new[] { All(testCase), Tail(testCase) };
            Outcome[] readAs = Array.ConvertAll(ranges, r => Read(ClientArms.ReadAs, testCase, target, r));
            Outcome[] poco = Array.ConvertAll(ranges, r => Read(ClientArms.Poco, testCase, target, r));
            Outcome[] fill = Array.ConvertAll(ranges, r => Read(ClientArms.PocoFill, testCase, target, r));

            bool canRead = ClickHouseTcpTypes.CanRead(testCase.ColumnType, target);
            Check(problems, $"{label}: CanRead is {canRead}, Block.ReadAs {readAs[0]}", canRead == (readAs[0].Kind != OutcomeKind.Refused));
            Check(problems, $"{label}: Block.ReadAs {readAs[0].Kind}, POCO mapping {poco[0]}", readAs[0].Kind == poco[0].Kind);
            if (readAs[0].Kind == OutcomeKind.Values)
            {
                Add(problems, $"{label}: POCO mapping against Block.ReadAs", Outcome.Difference(readAs[0], poco[0]));
            }

            for (int i = 0; i < ranges.Length; i++)
            {
                RowRange range = ranges[i];
                IEnumerable<Outcome> RowFailuresOfPoco() => RowFailures(range, row => Read(ClientArms.Poco, testCase, target, row));
                Add(problems, $"{label} {range}: POCO mapping", poco[i].Kind == OutcomeKind.Failed ? Outcome.FailureDifference(poco[i], RowFailuresOfPoco()) : null);
                Add(problems, $"{label} {range}: the Fill tier against POCO mapping", poco[i].Kind == OutcomeKind.Failed && fill[i].Kind == OutcomeKind.Failed
                    ? Outcome.FailureDifference(fill[i], RowFailuresOfPoco())
                    : Outcome.Difference(poco[i], fill[i]));
            }

            foreach ((string tier, Outcome[] outcomes) in new[] { ("Block.ReadAs", readAs), ("POCO mapping", poco), ("the Fill tier", fill) })
            {
                Check(problems, $"{label}: {tier} broke a rule of its own: {outcomes[0]}; {outcomes[1]}", Array.TrueForAll(outcomes, o => o.ExceptionType != typeof(ArmInvariantException)));
                Add(problems, $"{label}: {tier} reads the tail alone as {outcomes[1]}, all rows as {outcomes[0]}", TailDifference(outcomes[0], outcomes[1], ranges[1].CaseRow));
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// For each write input that the case builds or reads back: the insert write and an independent write that gathers the
    /// values of the slice through the indexer give the same bytes, for all rows and for the tail (so the insert slices
    /// right); the POCO insert in both gather tiers and the untyped insert write the bytes of the insert; and
    /// <c>CanWrite</c> is whether each insert takes the column. A column read back as another CLR type writes the bytes
    /// of the source column, except where the CLR type holds less (a count finer than 100 ns).
    /// </summary>
    [TestCaseSource(nameof(Cases))]
    public void Write_Case_TheInsertsWriteTheSameBytes(DifferentialCase testCase)
    {
        var problems = new List<string>();
        var ranges = new[] { All(testCase), Tail(testCase) };
        foreach (WriteInput input in testCase.WriteInputs.Where(i => i.Kind != WriteInputKind.Decoded))
        {
            Outcome[] insert = Array.ConvertAll(ranges, r => Write(ClientArms.Write, testCase, input, r));
            if (insert[0].Kind == OutcomeKind.Unavailable)
            {
                continue;
            }

            Outcome[] indexer = Array.ConvertAll(ranges, r => Write(ClientArms.IndexerWrite, testCase, input, r));
            Outcome[] poco = Array.ConvertAll(ranges, r => Write(ClientArms.PocoWrite, testCase, input, r));
            Outcome[] viaDelegates = Array.ConvertAll(ranges, r => Write(ClientArms.PocoWriteDelegate, testCase, input, r));
            Outcome[] untyped = Array.ConvertAll(ranges, r => Write(ClientArms.UntypedWrite, testCase, input, r));
            for (int i = 0; i < ranges.Length; i++)
            {
                RowRange range = ranges[i];
                string label = $"{input} {range}";
                IEnumerable<Outcome> RowFailuresOfInsert() => RowFailures(range, row => Write(ClientArms.Write, testCase, input, row));
                Add(problems, $"{label}: the insert", insert[i].Kind == OutcomeKind.Failed ? Outcome.FailureDifference(insert[i], RowFailuresOfInsert()) : null);
                Add(problems, $"{label}: the write through the indexer against the insert", indexer[i].Kind switch
                {
                    OutcomeKind.Refused => insert[i].Kind == OutcomeKind.Refused ? null : $"refused: {indexer[i]}; the insert {insert[i]}",
                    OutcomeKind.Failed when insert[i].Kind == OutcomeKind.Failed => Outcome.FailureDifference(indexer[i], RowFailuresOfInsert()),
                    _ => Outcome.Difference(insert[i], indexer[i]),
                });

                // A failure of a row insert can name the row of its block, so only its type is compared.
                Add(problems, $"{label}: the POCO insert of the Delegate tier against the compiled one", poco[i].Kind == OutcomeKind.Failed && viaDelegates[i].Kind == OutcomeKind.Failed
                    ? (poco[i].ExceptionType == viaDelegates[i].ExceptionType ? null : $"{viaDelegates[i]}; compiled {poco[i]}")
                    : Outcome.Difference(poco[i], viaDelegates[i]));
                foreach ((string tier, Outcome row) in new[] { ("the POCO insert", poco[i]), ("the untyped insert", untyped[i]) })
                {
                    Add(problems, $"{label}: {tier} against the insert", row.Kind == OutcomeKind.Bytes && insert[i].Kind == OutcomeKind.Bytes ? Outcome.Difference(insert[i], row) : null);
                }

                if (input.Kind == WriteInputKind.ReadBack && !HoldsCountsFinerThanTicks(TypeParser.Parse(testCase.ColumnType)) && insert[i].Kind == OutcomeKind.Bytes)
                {
                    Add(problems, $"{label}: the insert against the source column", Outcome.Difference(Write(ClientArms.Write, testCase, testCase.WriteInputs[0], range), insert[i]));
                }
            }

            // A Nested column is written only from a column of its own layout, never from a CLR element type, so CanWrite
            // is false for a type with a Nested in it, although the insert writes such a column.
            bool canWrite = ClickHouseTcpTypes.CanWrite(testCase.ColumnType, input.ElementType);
            bool nested = testCase.ColumnType.Contains("Nested(", StringComparison.Ordinal);
            Check(problems, $"{input}: CanWrite is {canWrite}, the insert {insert[0]}", nested || canWrite == (insert[0].Kind != OutcomeKind.Refused));
            Check(problems, $"{input}: CanWrite is {canWrite}, the POCO insert {poco[0]}", canWrite == (poco[0].Kind != OutcomeKind.Refused));
            Check(problems, $"{input}: CanWrite is {canWrite}, the untyped insert {untyped[0]}", canWrite == (untyped[0].Kind != OutcomeKind.Refused));
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The decoded column of a case is a column that its codec writes from its storage. Its insert gives back the bytes
    /// that it was read from, and each insert of it (the columnar insert, which writes it from its storage, and the row
    /// inserts, which gather its values) writes, for all rows and for the tail, bytes that decode to the values of the
    /// same rows of the source column. Equal bytes need no decode: the write of a slice of a LowCardinality column keeps
    /// the dictionary of the whole column, and a row insert builds the dictionary of its own values.
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
            Outcome all = Write(ClientArms.Write, testCase, input, All(testCase));
            Add(problems, "the insert of the decoded column against the source bytes", Outcome.Difference(Outcome.OfBytes(SourceBytes(testCase)), all));
            foreach (RowRange range in new[] { All(testCase), Tail(testCase) })
            {
                Outcome source = Write(ClientArms.Write, testCase, testCase.WriteInputs[0], range);
                var written = new[] { ClientArms.Write, ClientArms.PocoWrite, ClientArms.PocoWriteDelegate, ClientArms.UntypedWrite }.ToDictionary(arm => arm, arm => Write(arm, testCase, input, range));
                Check(problems, $"{range}: the POCO insert of the Delegate tier {written[ClientArms.PocoWriteDelegate]}, the compiled one {written[ClientArms.PocoWrite]}", written[ClientArms.PocoWrite].Kind == written[ClientArms.PocoWriteDelegate].Kind);
                foreach ((WriteArm arm, Outcome bytes) in written)
                {
                    if (source.Kind == OutcomeKind.Bytes && bytes.Kind == OutcomeKind.Bytes && Outcome.Difference(source, bytes) is not null)
                    {
                        Add(problems, $"{arm.Name} {range}", RowsDifference(testCase.ColumnType, source.Bytes, bytes.Bytes, range.Count));
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

    /// <summary>A failure of several rows is accepted when it is the failure of one of its rows alone, and only then.</summary>
    [Test]
    public void FailureDifference_FailureOfOneOfTheRows_IsTheOnlyFailureThatMatches()
    {
        Outcome[] rows = { Outcome.Failure(new OverflowException("row 1")), Outcome.Failure(new OverflowException("row 3")) };

        Assert.Multiple(() =>
        {
            Assert.That(Outcome.FailureDifference(Outcome.Failure(new OverflowException("row 3")), rows), Is.Null);
            Assert.That(Outcome.FailureDifference(Outcome.Failure(new OverflowException("row 2")), rows), Does.Contain("none of its rows"), "another row");
            Assert.That(Outcome.FailureDifference(Outcome.Failure(new ArgumentException("row 1")), rows), Does.Contain("none of its rows"), "another type");
            Assert.That(Outcome.FailureDifference(Outcome.OfBytes(new byte[] { 1 }), rows), Does.Contain("not Failed"), "no failure");
            Assert.That(Outcome.FailureDifference(Outcome.Failure(new OverflowException("row 1")), Array.Empty<Outcome>()), Does.Contain("none of its rows"), "no row fails alone");
        });
    }

    // Why a tier reads the tail alone otherwise than the same rows of the read of all rows, or null. A read of all rows
    // that fails can have a tail that fails or that gives values, because the tail may not hold the failing row.
    private static string TailDifference(Outcome all, Outcome tail, int caseRow) => all.Kind switch
    {
        OutcomeKind.Values => Outcome.Difference(all.Tail(caseRow), tail),
        OutcomeKind.Refused => Outcome.Difference(all, tail),
        OutcomeKind.Failed when tail.Kind is not (OutcomeKind.Failed or OutcomeKind.Values) => $"the tail is {tail.Kind}",
        _ => null,
    };

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
