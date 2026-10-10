using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Tests of the differential tests themselves: each wrong candidate is reported, a stated outcome is checked for the
/// baseline, the first candidate is the baseline, and the registry finds its own problems.
/// </summary>
[TestFixture]
public class DifferentialSelfTests
{
    private const string UInt8Case = "InsertRoundTrip: UInt8 [4 rows]";
    private const string UInt64Case = "ColumnReadProjection: UInt64";
    private const string NullableDateTimeCase = "ColumnReadProjection: Nullable(DateTime('UTC'))";
    private const string OneRowArrayCase = "InsertRoundTrip: Array(Int16) [1 rows]";
    private const string ArrayUInt8Case = "InsertRoundTrip: Array(UInt8) [3 rows]";

    // The sample values of the UInt64 case.
    private static readonly object[] UInt64Values = { 0UL, 1UL, ulong.MaxValue, 7UL, 1UL << 40 };

    [Test]
    public void Run_CandidateThatReadsOtherValues_ReportsTheFirstDifferentValue()
    {
        CaseReport report = RunWith(UInt8Case, r => r.Add(new ReversingReadArm(), expectedFacets: 1));

        Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<byte> rows [0, 4): Reversing gives").And.Contains("value 0: 255, not 0"));
    }

    [Test]
    public void Run_CandidateThatIgnoresTheStart_ReportsTheTail()
    {
        CaseReport report = RunWith(UInt8Case, r => r.Add(new StartIgnoringReadArm(), expectedFacets: 1));

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.Some.Contains("Start ignoring reads rows [2, 4) alone as"));
            Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<byte> rows [2, 4): Start ignoring gives"));
            Assert.That(report.Mismatches, Has.None.Contains("rows [0, 4)"), "the read of all rows is right");
        });
    }

    [Test]
    public void Run_CandidateThatBreaksARuleOfItsOwn_ReportsTheRule()
    {
        CaseReport report = RunWith(UInt8Case, r => r.Add(new RuleBreakingReadArm(), expectedFacets: 1));

        Assert.That(report.Mismatches, Has.Some.Contains("Rule breaking broke a rule of its own: a rule of the arm"));
    }

    [Test]
    public void Run_CandidateThatWritesOtherBytes_ReportsTheFirstDifferentByte()
    {
        CaseReport report = RunWith(UInt8Case, r => r.Add(new ExtraByteWriteArm(), expectedFacets: 2));

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.Some.Contains("Write[insert] rows [0, 4): Extra byte gives 5 bytes").And.Contains("5 bytes, not 4; the first difference is at byte 4"));
            Assert.That(report.Mismatches, Has.Some.Contains("Write[insert] rows [2, 4): Extra byte gives 3 bytes"));
            Assert.That(report.Mismatches, Has.Some.Contains("Write[decoded] rows [0, 4): Extra byte gives"));
        });
    }

    [Test]
    public void Run_CandidateWithAnotherAnswer_ReportsTheAnswer()
    {
        CaseReport report = RunWith(UInt64Case, r => r.Add(new NegatingAnswerArm(Tier.CanRead), expectedFacets: 3));

        Assert.That(report.Mismatches, Has.Some.Contains("CanRead<ulong?>: Negating CanRead gives answer False; Client.CanRead gives answer True"));
    }

    [Test]
    public void Run_ArmRefusal_MatchesARefusalOfTheBaselineButNotValues()
    {
        // The client refuses ReadAs<DateTime> of UInt64 and reads ReadAs<ulong>.
        CaseReport report = RunWith(UInt64Case, r => r.Add(new RefusingReadArm(), expectedFacets: 3));

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.None.Contains("ReadAs<DateTime>"), "an ArmRefusal and an InvalidCastException are both refusals");
            Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<ulong> rows [0, 5): Refusing gives refused with ArmRefusal").And.Contains("the kind is Refused, not Values"));
        });
    }

    [Test]
    public void Run_CandidateThatReadsWhatTheBaselineRefuses_ReportsTheReading()
    {
        CaseReport report = RunWith(UInt64Case, r => r.Add(new DefaultValuesReadArm(UInt64Case, typeof(DateTime)), expectedFacets: 1));

        Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<DateTime> rows [0, 5): Default values gives 5 values").And.Contains("the kind is Values, not Refused"));
    }

    [Test]
    public void Run_CandidateThatFailsForAllRowsAndRefusesTheTail_ReportsTheTail()
    {
        CaseReport report = RunWith(OneRowArrayCase, r => r.Add(new RefusingTailReadArm(OneRowArrayCase), expectedFacets: 1));

        Assert.That(report.Mismatches, Has.Some.Contains("Refusing tail reads rows [1, 2) after a preceding row alone as refused").And.Contains("the read of all rows fails, and the tail is Refused"));
    }

    [Test]
    public void Run_OneRowCaseWithAReaderThatIgnoresTheStart_ReportsTheTail()
    {
        CaseReport report = RunWith(OneRowArrayCase, r => r.Add(new StartIgnoringReadArm(OneRowArrayCase), expectedFacets: 1));

        Assert.Multiple(() =>
        {
            Assert.That(report.TailStart, Is.EqualTo(1));
            Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<short[]> rows [1, 2) after a preceding row: Start ignoring gives").And.Contains("an array of 8 elements, not 4"));
        });
    }

    [Test]
    public void Run_OneRowCaseWithAWriterThatIgnoresTheStart_ReportsTheSlice()
    {
        CaseReport report = RunWith(OneRowArrayCase, r => r.Add(new StartIgnoringWriteArm(OneRowArrayCase), expectedFacets: 2));

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.Some.Contains("Write[insert] rows [1, 2) after a preceding row: Start ignoring gives"));
            Assert.That(report.Mismatches, Has.Some.Contains("Write[decoded] rows [1, 2) after a preceding row: Start ignoring gives"));
            Assert.That(report.Mismatches, Has.None.Contains("rows [0, 1)"), "the write of all rows is right");
        });
    }

    [Test]
    public void DecodedWriteDifferences_DenseWriteThatIgnoresTheStart_ReportsTheTail()
    {
        // The write of the decoded column is the only candidate of its facet, so the comparison of the decoded rows with
        // the source rows is the check that finds a dense write that ignores the start.
        var registry = new DifferentialRegistry();
        registry.AddForEveryFacet(new DecodedStartIgnoringWriteArm());
        CaseReport report = DifferentialEngine.Run(DifferentialCases.All().Single(c => c.Id == ArrayUInt8Case), registry);

        string[] differences = DifferentialTests.DecodedWriteDifferences(report).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Is.Empty, "one candidate, so the engine compares nothing");
            Assert.That(differences, Has.Some.Contains("Write[decoded] Tail"));
            Assert.That(differences, Has.None.Contains("Write[decoded] All"), "the write of all rows starts at row 0");
        });
    }

    [Test]
    public void DecodedWriteDifferences_ClientWrites_ReportNothing()
    {
        var registry = new DifferentialRegistry();
        registry.AddForEveryFacet(ClientArms.Write);
        CaseReport report = DifferentialEngine.Run(DifferentialCases.All().Single(c => c.Id == ArrayUInt8Case), registry);

        Assert.That(DifferentialTests.DecodedWriteDifferences(report), Is.Empty);
    }

    [Test]
    public void Run_StatedOutcomeThatTheBaselineDoesNotGive_IsReported()
    {
        // The client reads Time 0 as TimeOnly 00:00:00, not as 00:00:01.
        var testCase = new DifferentialCase(
            "A case that states a wrong value",
            CaseSource.ColumnReadScenario,
            "Time",
            1,
            new[] { typeof(int), typeof(TimeOnly) },
            new[] { WriteInput.Built("canonical", typeof(int), name => new ArrayColumn<int>(name, "Time", new[] { 0 })) },
            new[] { new StatedOutcome(Tier.ReadAs, typeof(TimeOnly), Expectation.Values(new TimeOnly(0, 0, 1))) });

        CaseReport report = DifferentialEngine.Run(testCase, DifferentialRegistry.WithClientArms());

        Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<TimeOnly> rows [0, 1): Client.ReadAs gives").And.Contains("the source test states the values [00:00:01.0000000]").And.Contains("value 0: 00:00:00.0000000, not 00:00:01.0000000"));
    }

    /// <summary>
    /// A stated failure is checked for the tail too: the text of the POCO failure of all rows (the NULL at row 1) is
    /// not the failure of the tail, whose first NULL is row 4.
    /// </summary>
    [Test]
    public void Run_StatedFailureWithTheTextOfAllRows_ReportsTheTail()
    {
        CaseReport report = DifferentialEngine.Run(
            WithStated(NullableDateTimeCase, Tier.Poco, typeof(DateTime), Expectation.Fails<InvalidOperationException>("is NULL at row 1")),
            DifferentialRegistry.WithClientArms());

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.Some.Contains("Poco<DateTime> rows [2, 5): Client.Poco gives failed").And.Contains("does not contain \"is NULL at row 1\""));
            Assert.That(report.Mismatches, Has.None.Contains("rows [0, 5)"), "the read of all rows fails at row 1");
        });
    }

    /// <summary>
    /// A stated failure that the baseline gives for both ranges is met, and a candidate that gives values for the tail
    /// of that failure differs from the baseline there.
    /// </summary>
    [Test]
    public void Run_StatedFailureAndACandidateWithValuesForTheTail_ReportsTheCandidate()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithClientArms();
        registry.Add(new ValuesTailReadArm(NullableDateTimeCase, typeof(DateTime)), expectedFacets: 1);

        CaseReport report = DifferentialEngine.Run(
            WithStated(NullableDateTimeCase, Tier.Poco, typeof(DateTime), Expectation.Fails<InvalidOperationException>("is NULL")),
            registry);

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.Some.Contains("Poco<DateTime> rows [2, 5): Values tail gives").And.Contains("the kind is Values, not Failed"));
            Assert.That(report.Mismatches, Has.None.Contains("the source test states"), "the baseline fails at a NULL for both ranges");
        });
    }

    [Test]
    public void Run_FirstCandidate_IsTheBaseline()
    {
        CaseReport agreeing = RunWith(UInt8Case, r => r.AddForEveryFacet(RenamedArm.Of("First", ClientArms.ReadAs)), WithTheClientWriteOnly());
        CaseReport disagreeing = RunWith(
            UInt8Case,
            r =>
            {
                r.AddForEveryFacet(RenamedArm.Of("First", ClientArms.ReadAs));
                r.Add(new ReversingReadArm(), expectedFacets: 1);
            },
            WithTheClientWriteOnly());

        Assert.Multiple(() =>
        {
            Assert.That(agreeing.Mismatches, Is.Empty);
            Assert.That(disagreeing.Mismatches, Has.Some.Contains("Reversing gives").And.Contains("First gives"));
        });
    }

    [Test]
    public void Validate_ArmWithAnotherFacetCount_ReportsBothCounts()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithClientArms();
        registry.Add(new ReversingReadArm(), expectedFacets: 2);

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.EqualTo("The arm 'Reversing' covers 1 ReadAs facets, not 2."));
    }

    [Test]
    public void Validate_ArmForEveryFacetThatSkipsOne_ReportsTheCount()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithClientArms();
        registry.AddForEveryFacet(new ReversingReadArm());

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.StartsWith("The arm 'Reversing' covers 1 ReadAs facets, not "));
    }

    [Test]
    public void Validate_DuplicateArmNames_AreReported()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithClientArms();
        registry.Add(new ReversingReadArm(), expectedFacets: 1);
        registry.Add(new ReversingReadArm(), expectedFacets: 1);

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.EqualTo("2 candidate arms are called 'Reversing'."));
    }

    [Test]
    public void Current_Registry_HasTheRegistrationsOfTheAssembly()
        => Assert.That(DifferentialRegistry.Current.Candidates.Select(a => a.Name), Does.Contain("Client.Poco: Fill"));

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
            Assert.That(ValueComparer.Difference(new byte[] { 1, 2 }, new byte[] { 1, 2 }), Is.Null);
            Assert.That(ValueComparer.Difference((new byte[] { 1 }, "a"), (new byte[] { 1 }, "a")), Is.Null);
            Assert.That(ValueComparer.Difference(new KeyValuePair<string, byte[]>("k", new byte[] { 3 }), new KeyValuePair<string, byte[]>("k", new byte[] { 3 })), Is.Null);
            Assert.That(ValueComparer.Difference(new[] { new uint?[] { 1, null } }, new[] { new uint?[] { 1, null } }), Is.Null);
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
            Assert.That(ValueComparer.Difference(new KeyValuePair<string, int>("k", 1), new KeyValuePair<string, int>("k", 2)), Is.EqualTo("value: 2, not 1"));
        });
    }

    private static CaseReport RunWith(string caseId, Action<DifferentialRegistry> register, DifferentialRegistry registry = null)
    {
        registry ??= DifferentialRegistry.WithClientArms();
        register(registry);
        return DifferentialEngine.Run(DifferentialCases.All().Single(c => c.Id == caseId), registry);
    }

    // A registry whose only candidate is the client's insert write, which writes the source column of every case, so
    // the read facets have only the candidates that a test adds.
    private static DifferentialRegistry WithTheClientWriteOnly()
    {
        var registry = new DifferentialRegistry();
        registry.AddForEveryFacet(ClientArms.Write);
        return registry;
    }

    // The case with one stated outcome in place of its own.
    private static DifferentialCase WithStated(string caseId, Tier tier, Type target, Expectation expected)
    {
        DifferentialCase source = DifferentialCases.All().Single(c => c.Id == caseId);
        return new DifferentialCase(source.Id, source.Source, source.ColumnType, source.RowCount, source.ReadTargets, source.WriteInputs, new[] { new StatedOutcome(tier, target, expected) });
    }

    /// <summary>The client's read, with the values in reverse order. Covers ReadAs&lt;byte&gt; of the UInt8 case.</summary>
    private sealed class ReversingReadArm : ReadArm
    {
        public ReversingReadArm()
            : base("Reversing", Tier.ReadAs)
        {
        }

        public override bool Covers(Facet facet) => facet.Case.Id == UInt8Case;

        public override RowReader<T> Bind<T>(Block block)
        {
            RowReader<T> inner = ClientArms.Poco.Bind<T>(block);
            return (start, count) =>
            {
                T[] values = inner(start, count);
                Array.Reverse(values);
                return values;
            };
        }
    }

    /// <summary>The client's read, always from row 0. Covers the ReadAs facets of one case.</summary>
    private sealed class StartIgnoringReadArm : ReadArm
    {
        private readonly string caseId;

        public StartIgnoringReadArm(string caseId = UInt8Case)
            : base("Start ignoring", Tier.ReadAs) => this.caseId = caseId;

        public override bool Covers(Facet facet) => facet.Case.Id == caseId;

        public override RowReader<T> Bind<T>(Block block)
        {
            RowReader<T> inner = ClientArms.Poco.Bind<T>(block);
            return (_, count) => inner(0, count);
        }
    }

    /// <summary>An arm that finds a fault in itself. Covers ReadAs&lt;byte&gt; of the UInt8 case.</summary>
    private sealed class RuleBreakingReadArm : ReadArm
    {
        public RuleBreakingReadArm()
            : base("Rule breaking", Tier.ReadAs)
        {
        }

        public override bool Covers(Facet facet) => facet.Case.Id == UInt8Case;

        public override RowReader<T> Bind<T>(Block block) => (_, _) => throw new ArmInvariantException("a rule of the arm");
    }

    /// <summary>Refuses every reading. Covers the ReadAs facets of the UInt64 case.</summary>
    private sealed class RefusingReadArm : ReadArm
    {
        public RefusingReadArm()
            : base("Refusing", Tier.ReadAs)
        {
        }

        public override bool Covers(Facet facet) => facet.Case.Id == UInt64Case;

        public override RowReader<T> Bind<T>(Block block) => throw new ArmRefusal("a test refuses every reading");
    }

    /// <summary>
    /// Reads the default value of the target for every row. Covers ReadAs facets of one case.
    /// </summary>
    private sealed class DefaultValuesReadArm : ReadArm
    {
        private readonly string caseId;
        private readonly Type target;

        public DefaultValuesReadArm(string caseId, Type target)
            : base("Default values", Tier.ReadAs)
        {
            this.caseId = caseId;
            this.target = target;
        }

        public override bool Covers(Facet facet) => facet.Case.Id == caseId && facet.Target == target;

        public override RowReader<T> Bind<T>(Block block) => (_, count) => new T[count];
    }

    /// <summary>The client's POCO read, with default values for the tail.</summary>
    private sealed class ValuesTailReadArm : ReadArm
    {
        private readonly string caseId;
        private readonly Type target;

        public ValuesTailReadArm(string caseId, Type target)
            : base("Values tail", Tier.Poco)
        {
            this.caseId = caseId;
            this.target = target;
        }

        public override bool Covers(Facet facet) => facet.Case.Id == caseId && facet.Target == target;

        public override RowReader<T> Bind<T>(Block block)
        {
            RowReader<T> inner = ClientArms.Poco.Bind<T>(block);
            return (start, count) => start == 0 ? inner(start, count) : new T[count];
        }
    }

    /// <summary>Fails for all rows, and refuses the block of the tail. Covers ReadAs of one one-row case.</summary>
    private sealed class RefusingTailReadArm : ReadArm
    {
        private readonly string caseId;

        public RefusingTailReadArm(string caseId)
            : base("Refusing tail", Tier.ReadAs) => this.caseId = caseId;

        public override bool Covers(Facet facet) => facet.Case.Id == caseId;

        // The tail of a one-row case reads a block of two rows.
        public override RowReader<T> Bind<T>(Block block)
            => block.RowCount == 2
                ? throw new ArmRefusal("a test refuses the tail")
                : (_, _) => throw new InvalidOperationException("a test fails every read");
    }

    /// <summary>The client's write, then one more byte. Covers the Write facets of the UInt8 case.</summary>
    private sealed class ExtraByteWriteArm : WriteArm
    {
        public ExtraByteWriteArm()
            : base("Extra byte")
        {
        }

        public override bool Covers(Facet facet) => facet.Case.Id == UInt8Case;

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            SliceWriter inner = ClientArms.Write.Bind(column, columnType, context);
            return (writer, start, length) =>
            {
                inner(writer, start, length);
                writer.WriteByte(0xEE);
            };
        }
    }

    /// <summary>The client's write, always from row 0. Covers the Write facets of one case.</summary>
    private sealed class StartIgnoringWriteArm : WriteArm
    {
        private readonly string caseId;

        public StartIgnoringWriteArm(string caseId)
            : base("Start ignoring") => this.caseId = caseId;

        public override bool Covers(Facet facet) => facet.Case.Id == caseId;

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            SliceWriter inner = ClientArms.Write.Bind(column, columnType, context);
            return (writer, _, length) => inner(writer, 0, length);
        }
    }

    /// <summary>The client's write; a decoded input is written from row 0 whatever the start.</summary>
    private sealed class DecodedStartIgnoringWriteArm : WriteArm
    {
        public DecodedStartIgnoringWriteArm()
            : base("Decoded start ignoring")
        {
        }

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            SliceWriter inner = ClientArms.Write.Bind(column, columnType, context);
            return column is IDenseArrayColumn ? (writer, _, length) => inner(writer, 0, length) : inner;
        }
    }

    /// <summary>The opposite of the client's answer.</summary>
    private sealed class NegatingAnswerArm : AnswerArm
    {
        public NegatingAnswerArm(Tier tier)
            : base($"Negating {tier}", tier)
        {
        }

        public override bool Covers(Facet facet) => facet.Case.Id == UInt64Case;

        public override bool Answer(string columnType, Type elementType)
            => !(Tier == Tier.CanRead ? ClientArms.CanRead : ClientArms.CanWrite).Answer(columnType, elementType);
    }
}
