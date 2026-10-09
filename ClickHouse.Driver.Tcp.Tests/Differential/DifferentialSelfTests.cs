using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Tests of the differential tests themselves: each wrong candidate is reported, a deliberate change is checked
/// in both directions, and the registry finds its own problems.
/// </summary>
[TestFixture]
public class DifferentialSelfTests
{
    private const string UInt8Case = "InsertRoundTrip: UInt8 [4 rows]";
    private const string UInt64Case = "ColumnReadProjection: UInt64";
    private const string NullableDateTimeCase = "ColumnReadProjection: Nullable(DateTime('UTC'))";
    private const string LowCardinalityStringCase = "ColumnReadProjection: LowCardinality(String)";
    private const string OneRowArrayCase = "InsertRoundTrip: Array(Int16) [1 rows]";

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

        Assert.That(report.Mismatches, Has.Some.Contains("CanRead<ulong?>: Negating CanRead gives answer True; Client.CanRead gives answer False"));
    }

    [Test]
    public void Run_ArmRefusal_MatchesARefusalOfTheReferenceButNotValues()
    {
        // The reference refuses ReadAs<ulong?> and reads ReadAs<ulong>.
        CaseReport report = RunWith(UInt64Case, r => r.Add(new RefusingReadArm(), expectedFacets: 3));

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.None.Contains("ReadAs<ulong?>"), "an ArmRefusal and an InvalidCastException are both refusals");
            Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<ulong> rows [0, 5): Refusing gives refused with ArmRefusal").And.Contains("the kind is Refused, not Values"));
        });
    }

    [Test]
    public void Run_CandidateWithAnUndeclaredChange_ReportsTheChange()
    {
        CaseReport report = RunWith(UInt64Case, r => r.Add(new PocoAsReadAsArm(UInt64Case, typeof(ulong), typeof(ulong?)), expectedFacets: 2));

        Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<ulong?> rows [0, 5): Poco as ReadAs gives 5 values").And.Contains("the kind is Values, not Refused"));
    }

    [TestCase("SameAs(Poco, ulong?)")]
    [TestCase("SameAs(ReadAs, ulong)")]
    [TestCase("Values")]
    public void Run_DeclaredChangeThatTheCandidateMakes_ReportsNothing(string expectation)
    {
        Expectation expected = expectation switch
        {
            "SameAs(Poco, ulong?)" => Expectation.SameAs(Tier.Poco, typeof(ulong?)),
            "SameAs(ReadAs, ulong)" => Expectation.SameAs(Tier.ReadAs, typeof(ulong)),
            _ => Expectation.Values(0UL, 1UL, ulong.MaxValue, 7UL, 1UL << 40),
        };

        CaseReport report = RunWith(UInt64Case, r =>
        {
            r.Add(new PocoAsReadAsArm(UInt64Case, typeof(ulong), typeof(ulong?)), expectedFacets: 2);
            r.DeclareChange(UInt64Case, Tier.ReadAs, typeof(ulong?), expected, "a test");
        });

        Assert.That(report.Mismatches, Is.Empty);
    }

    [Test]
    public void Run_DeclaredFailureThatTheCandidateMakes_ReportsNothing()
    {
        // The reference refuses ReadAs<DateTime> over a Nullable column; the POCO plan reads it and fails at the first NULL.
        CaseReport report = RunWith(NullableDateTimeCase, r =>
        {
            r.Add(new PocoAsReadAsArm(NullableDateTimeCase, typeof(DateTime), typeof(uint)), expectedFacets: 2);
            r.DeclareChange(NullableDateTimeCase, Tier.ReadAs, typeof(DateTime), Expectation.Fails<InvalidOperationException>("is NULL at row 1"), "a test");
            r.DeclareChange(NullableDateTimeCase, Tier.ReadAs, typeof(uint), Expectation.Fails<InvalidOperationException>("is NULL at row 1"), "a test");
        });

        Assert.That(report.Mismatches, Is.Empty);
    }

    [Test]
    public void Run_DeclaredWriteThatTheCandidateMakes_ReportsNothing()
    {
        // The reference refuses LowCardinality(String) from byte[] (ClickHouse/integrations#792).
        CaseReport report = RunWith(LowCardinalityStringCase, r =>
        {
            r.Add(new BytesAsTextWriteArm(LowCardinalityStringCase), expectedFacets: 1);
            r.DeclareChange(LowCardinalityStringCase, Tier.Write, "read back as byte[]", Expectation.SameAsWrite("canonical"), "a test");
        });

        Assert.That(report.Mismatches, Is.Empty);
    }

    [Test]
    public void Run_DeclaredAnswerThatTheCandidateGives_ReportsNothing()
    {
        CaseReport report = RunWith(UInt64Case, r =>
        {
            r.Add(new NegatingAnswerArm(Tier.CanRead, target: typeof(ulong?)), expectedFacets: 1);
            r.DeclareChange(UInt64Case, Tier.CanRead, typeof(ulong?), Expectation.Answer(true), "a test");
        });

        Assert.That(report.Mismatches, Is.Empty);
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
    public void Run_StatedOutcomeThatTheReferenceDoesNotGive_IsReported()
    {
        // The reference reads Time 0 as TimeOnly 00:00:00, not as 00:00:01.
        var testCase = new DifferentialCase(
            "A case that states a wrong value",
            CaseSource.ColumnReadScenario,
            "Time",
            1,
            new[] { typeof(int), typeof(TimeOnly) },
            new[] { WriteInput.Built("canonical", typeof(int), name => new ArrayColumn<int>(name, "Time", new[] { 0 })) },
            new[] { new StatedOutcome(Tier.ReadAs, typeof(TimeOnly), Expectation.Values(new TimeOnly(0, 0, 1))) });

        CaseReport report = DifferentialEngine.Run(testCase, DifferentialRegistry.WithReference());

        Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<TimeOnly> rows [0, 1): Client.ReadAs gives").And.Contains("the source test states the values [00:00:01.0000000]").And.Contains("value 0: 00:00:00.0000000, not 00:00:01.0000000"));
    }

    [Test]
    public void Run_DeclaredChangeThatIsTheReferenceOutcome_ReportsThatItIsNoChange()
    {
        CaseReport report = RunWith(UInt64Case, r =>
        {
            r.Add(new PocoAsReadAsArm(UInt64Case, typeof(ulong), typeof(ulong?)), expectedFacets: 2);
            r.DeclareChange(UInt64Case, Tier.ReadAs, typeof(ulong?), Expectation.Refused<InvalidCastException>("cannot be read as"), "a test");
        });

        Assert.Multiple(() =>
        {
            Assert.That(report.Mismatches, Has.Some.Contains("Client.ReadAs already gives that").And.Contains("A deliberate change must differ from the reference."));
            Assert.That(report.Mismatches, Has.Some.Contains("Poco as ReadAs gives 5 values").And.Contains("the kind is Values, not Refused"));
        });
    }

    [Test]
    public void Run_DeclaredChangeThatTheCandidateDoesNotMake_ReportsTheCandidate()
    {
        CaseReport report = RunWith(UInt64Case, r =>
        {
            r.Add(new PocoAsReadAsArm(UInt64Case, typeof(ulong), typeof(ulong?)), expectedFacets: 2);
            r.DeclareChange(UInt64Case, Tier.ReadAs, typeof(ulong?), Expectation.Values(1UL, 2UL, 3UL, 4UL, 5UL), "a test");
        });

        Assert.That(report.Mismatches, Has.Some.Contains("the deliberate change").And.Contains("value 0: 0, not 1"));
    }

    [Test]
    public void Run_DeclaredChangeWithNoCandidate_ReportsTheFacet()
    {
        CaseReport report = RunWith(UInt64Case, r => r.DeclareChange(UInt64Case, Tier.ReadAs, typeof(ulong?), Expectation.SameAs(Tier.Poco, typeof(ulong?)), "a test"));

        Assert.That(report.Mismatches, Has.Some.Contains("ReadAs<ulong?>: the deliberate change").And.Contains("no candidate arm covers it"));
    }

    [Test]
    public void Run_NoReference_ComparesWithTheFirstCandidate()
    {
        CaseReport agreeing = RunWith(UInt8Case, r =>
        {
            r.RemoveReference(Tier.ReadAs);
            r.AddForEveryFacet(RenamedArm.Of("First", ClientArms.ReadAs));
        });
        CaseReport disagreeing = RunWith(UInt8Case, r =>
        {
            r.RemoveReference(Tier.ReadAs);
            r.AddForEveryFacet(RenamedArm.Of("First", ClientArms.ReadAs));
            r.Add(new ReversingReadArm(), expectedFacets: 1);
        });

        Assert.Multiple(() =>
        {
            Assert.That(agreeing.Mismatches, Is.Empty);
            Assert.That(disagreeing.Mismatches, Has.Some.Contains("Reversing gives").And.Contains("First gives"));
        });
    }

    [Test]
    public void Validate_ArmWithAnotherFacetCount_ReportsBothCounts()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithReference();
        registry.Add(new ReversingReadArm(), expectedFacets: 2);

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.EqualTo("The arm 'Reversing' covers 1 ReadAs facets, not 2."));
    }

    [Test]
    public void Validate_ArmForEveryFacetThatSkipsOne_ReportsTheCount()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithReference();
        registry.AddForEveryFacet(new ReversingReadArm());

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.StartsWith("The arm 'Reversing' covers 1 ReadAs facets, not "));
    }

    [Test]
    public void Validate_DuplicateArmNames_AreReported()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithReference();
        registry.Add(new ReversingReadArm(), expectedFacets: 1);
        registry.Add(new ReversingReadArm(), expectedFacets: 1);

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.EqualTo("2 candidate arms are called 'Reversing'."));
    }

    [Test]
    public void Validate_DeclaredChangeOfAnotherSize_ReportsTheCount()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithReference();
        registry.DeclareChange("No such case", Tier.ReadAs, typeof(int), Expectation.Answer(true), "a test");

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.EqualTo("The deliberate change 'No such case ReadAs<int>' selects 0 facets, not 1."));
    }

    [Test]
    public void Validate_DeclaredChangeWithNoCandidate_IsReported()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithReference();
        registry.DeclareChange(UInt64Case, Tier.ReadAs, typeof(ulong?), Expectation.SameAs(Tier.Poco, typeof(ulong?)), "a test");

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.Contains("no candidate arm covers it"));
    }

    [Test]
    public void Validate_TwoChangesOfOneFacet_AreReported()
    {
        DifferentialRegistry registry = DifferentialRegistry.WithReference();
        registry.Add(new PocoAsReadAsArm(UInt64Case, typeof(ulong), typeof(ulong?)), expectedFacets: 2);
        registry.DeclareChange(UInt64Case, Tier.ReadAs, typeof(ulong?), Expectation.SameAs(Tier.Poco, typeof(ulong?)), "a test");
        registry.DeclareChanges("ReadAs<ulong?> of UInt64", f => f.Case.Id == UInt64Case && f.Tier == Tier.ReadAs && f.Target == typeof(ulong?), _ => Expectation.SameAs(Tier.Poco, typeof(ulong?)), "a test", expectedFacets: 1);

        Assert.That(registry.Validate(DifferentialCases.All()), Has.Some.Contains("both select this facet"));
    }

    [Test]
    public void Current_Registry_HasTheRegistrationsOfTheAssembly()
        => Assert.That(DifferentialRegistry.Current.Candidates.Select(a => a.Name), Does.Contain("Old path again: ReadAs"));

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

    [Test]
    public void Verify_Bytes_ChecksAllRowsAndTheTailApart()
    {
        Expectation expected = Expectation.Bytes(new byte[] { 1, 2 }, new byte[] { 2 });

        Assert.Multiple(() =>
        {
            Assert.That(expected.Verify(Outcome.OfBytes(new byte[] { 1, 2 }), Rows.All, 1, references: null), Is.Null);
            Assert.That(expected.Verify(Outcome.OfBytes(new byte[] { 2 }), Rows.Tail, 1, references: null), Is.Null);
            Assert.That(expected.Verify(Outcome.OfBytes(new byte[] { 1, 2 }), Rows.Tail, 1, references: null), Is.Not.Null);
        });
    }

    private static CaseReport RunWith(string caseId, Action<DifferentialRegistry> register)
    {
        DifferentialRegistry registry = DifferentialRegistry.WithReference();
        register(registry);
        return DifferentialEngine.Run(DifferentialCases.All().Single(c => c.Id == caseId), registry);
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
            RowReader<T> inner = ClientArms.ReadAs.Bind<T>(block);
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
            RowReader<T> inner = ClientArms.ReadAs.Bind<T>(block);
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

    /// <summary>The POCO read plan as a ReadAs candidate, which accepts some readings that ReadAs refuses.</summary>
    private sealed class PocoAsReadAsArm : ReadArm
    {
        private readonly string caseId;
        private readonly Type[] targets;

        public PocoAsReadAsArm(string caseId, params Type[] targets)
            : base("Poco as ReadAs", Tier.ReadAs)
        {
            this.caseId = caseId;
            this.targets = targets;
        }

        public override bool Covers(Facet facet) => facet.Case.Id == caseId && (targets.Length == 0 || targets.Contains(facet.Target));

        public override RowReader<T> Bind<T>(Block block) => ClientArms.Poco.Bind<T>(block);
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

    /// <summary>Writes byte[] values as the text they spell, with the client's write.</summary>
    private sealed class BytesAsTextWriteArm : WriteArm
    {
        private readonly string caseId;

        public BytesAsTextWriteArm(string caseId)
            : base("Bytes as text") => this.caseId = caseId;

        public override bool Covers(Facet facet) => facet.Case.Id == caseId && facet.Input.ElementType == typeof(byte[]);

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            string[] text = column.Values.ToArray().Select(v => Encoding.UTF8.GetString((byte[])(object)v)).ToArray();
            return ClientArms.Write.Bind(new ArrayColumn<string>(column.Name, columnType, text), columnType, context);
        }
    }

    /// <summary>The opposite of the client's answer.</summary>
    private sealed class NegatingAnswerArm : AnswerArm
    {
        private readonly Type target;

        public NegatingAnswerArm(Tier tier, Type target = null)
            : base($"Negating {tier}", tier) => this.target = target;

        public override bool Covers(Facet facet) => facet.Case.Id == UInt64Case && (target is null || facet.Target == target);

        public override bool Answer(string columnType, Type elementType)
            => !(Tier == Tier.CanRead ? ClickHouseTcpTypes.CanRead(columnType, elementType) : ClickHouseTcpTypes.CanWrite(columnType, elementType));
    }
}
