using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>The outcomes of one arm for one facet: all rows, and the tail.</summary>
internal sealed class RangeOutcomes
{
    public RangeOutcomes(Outcome all, Outcome tail)
    {
        All = all;
        Tail = tail;
    }

    public Outcome All { get; }

    /// <summary>The outcome for the tail, or null for an answer facet or a case with fewer than two rows.</summary>
    public Outcome Tail { get; }

    public Outcome For(Rows rows) => rows == Rows.All ? All : Tail;
}

/// <summary>The outcomes of every arm for one facet.</summary>
internal sealed class FacetResult
{
    public FacetResult(Facet facet, Arm referenceArm, RangeOutcomes reference, IReadOnlyList<(Arm Arm, RangeOutcomes Outcomes)> candidates)
    {
        Facet = facet;
        ReferenceArm = referenceArm;
        Reference = reference;
        Candidates = candidates;
    }

    public Facet Facet { get; }

    /// <summary>The reference arm of the facet's tier, or null when the tier has none.</summary>
    public Arm ReferenceArm { get; }

    /// <summary>The reference outcomes, or null when the tier has no reference.</summary>
    public RangeOutcomes Reference { get; }

    public IReadOnlyList<(Arm Arm, RangeOutcomes Outcomes)> Candidates { get; }
}

/// <summary>The result of a case: the outcomes of each facet, and each difference that the case found.</summary>
internal sealed class CaseReport
{
    public CaseReport(DifferentialCase testCase, IReadOnlyList<FacetResult> facets, IReadOnlyList<string> mismatches)
    {
        Case = testCase;
        Facets = facets;
        Mismatches = mismatches;
    }

    public DifferentialCase Case { get; }

    public IReadOnlyList<FacetResult> Facets { get; }

    /// <summary>One message for each difference. Empty when every candidate agrees with the reference.</summary>
    public IReadOnlyList<string> Mismatches { get; }
}

/// <summary>
/// Runs the facets of a case with every arm of a registry, and compares the outcomes.
/// </summary>
/// <remarks>
/// <para>
/// The reference write of the case's first write input gives the bytes of the column. Each call of an arm gets
/// a block that is decoded from those bytes for that call only, with <c>ReadStatePrefixAsync</c> and
/// <c>ReadColumnAsync</c>. A cache that a column fills on its first read is therefore empty for each arm.
/// </para>
/// <para>
/// Each facet runs for all rows and for the tail, <c>[RowCount / 2, RowCount)</c>. A candidate must give the
/// same outcome as the reference for both, or the outcome that a deliberate change declares. Each arm, the
/// reference too, must also read the same values for the tail alone as for the same rows of the full read.
/// </para>
/// </remarks>
internal sealed class DifferentialEngine : IReferenceOutcomes
{
    private static readonly ConcurrentDictionary<string, CaseReport> CurrentReports = new(StringComparer.Ordinal);

    private static readonly MethodInfo ReadMethod = typeof(DifferentialEngine).GetMethod(nameof(Read), BindingFlags.NonPublic | BindingFlags.Instance);
    private static readonly MethodInfo WriteMethod = typeof(DifferentialEngine).GetMethod(nameof(Write), BindingFlags.NonPublic | BindingFlags.Instance);
    private static readonly MethodInfo ReadBackColumnMethod = typeof(DifferentialEngine).GetMethod(nameof(ReadBackColumn), BindingFlags.NonPublic | BindingFlags.Static);

    private readonly DifferentialCase testCase;
    private readonly DifferentialRegistry registry;
    private readonly Dictionary<Facet, FacetResult> results = new();
    private readonly List<string> mismatches = new();
    private readonly int tailStart;
    private readonly bool hasTail;
    private byte[] sourceBytes;

    private DifferentialEngine(DifferentialCase testCase, DifferentialRegistry registry)
    {
        this.testCase = testCase;
        this.registry = registry;
        tailStart = testCase.RowCount / 2;
        hasTail = testCase.RowCount >= 2;
    }

    /// <summary>The context that every case resolves its codecs with.</summary>
    internal static ResolveContext Context { get; } = new() { ServerTimezone = "UTC" };

    /// <summary>Runs a case with <see cref="DifferentialRegistry.Current"/>. The report is computed once for each case.</summary>
    /// <param name="testCase">The case.</param>
    /// <returns>The report.</returns>
    public static CaseReport ForCurrentRegistry(DifferentialCase testCase)
        => CurrentReports.GetOrAdd(testCase.Id, _ => Run(testCase, DifferentialRegistry.Current));

    /// <summary>Runs a case with every arm of a registry.</summary>
    /// <param name="testCase">The case.</param>
    /// <param name="registry">The registry.</param>
    /// <returns>The report.</returns>
    public static CaseReport Run(DifferentialCase testCase, DifferentialRegistry registry)
    {
        var engine = new DifferentialEngine(testCase, registry);
        engine.Execute();
        return new CaseReport(testCase, engine.results.Values.ToList(), engine.mismatches);
    }

    /// <inheritdoc/>
    Outcome IReferenceOutcomes.Read(Tier tier, Type target, Rows rows)
        => results.Values.FirstOrDefault(r => r.Facet.Tier == tier && r.Facet.Target == target)?.Reference?.For(rows);

    /// <inheritdoc/>
    Outcome IReferenceOutcomes.Write(Tier tier, string inputLabel, Rows rows)
        => results.Values.FirstOrDefault(r => r.Facet.Tier == tier && r.Facet.Input?.Label == inputLabel)?.Reference?.For(rows);

    private static object Invoke(MethodInfo method, Type typeArgument, object target, params object[] arguments)
    {
        try
        {
            return method.MakeGenericMethod(typeArgument).Invoke(target, arguments);
        }
        catch (TargetInvocationException e) when (e.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            throw;
        }
    }

    private static byte[] Capture(SliceWriter write, int start, int length)
    {
        using var stream = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(stream))
        {
            write(writer, start, length);
            writer.FlushAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        return stream.ToArray();
    }

    private static IColumn ReadBackColumn<T>(string columnType, Outcome values)
        => new ArrayColumn<T>("value", columnType, values.ValuesAs<T>());

    private void Execute()
    {
        List<Facet> facets = testCase.Facets().ToList();

        if (!EncodeSource(facets))
        {
            return;
        }

        // Read facets first: a read-back write input writes the values of the reference read.
        foreach (Facet facet in facets.Where(f => f.Input is null).Concat(facets.Where(f => f.Input is not null)))
        {
            results[facet] = Evaluate(facet);
        }

        foreach (Facet facet in facets)
        {
            Compare(results[facet]);
        }
    }

    private bool EncodeSource(List<Facet> facets)
    {
        Facet source = facets.First(f => f.Tier == Tier.Write && f.Input == testCase.WriteInputs[0]);
        var arm = (WriteArm)(registry.Reference(Tier.Write) ?? registry.CandidatesFor(source).FirstOrDefault());
        if (arm is null)
        {
            mismatches.Add($"{source}: no arm can write the source column.");
            return false;
        }

        Outcome outcome = Run(arm, source).All;
        if (outcome.Kind != OutcomeKind.Bytes)
        {
            mismatches.Add($"{source}: {arm.Name} cannot write the source column: {outcome}.");
            return false;
        }

        sourceBytes = outcome.Bytes;
        try
        {
            using Block block = Decode(checkConsumed: true);
        }
        catch (Exception e)
        {
            mismatches.Add($"{source}: the bytes of {arm.Name} do not decode: {e.GetType().Name}: {e.Message}");
            return false;
        }

        return true;
    }

    private Block Decode(bool checkConsumed = false)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(testCase.ColumnType, Context);
        using var stream = new MemoryStream(sourceBytes);
        using var reader = new ClickHouseBinaryReader(stream);
        codec.ReadStatePrefixAsync(reader, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        IColumn column = codec.ReadColumnAsync(reader, "value", testCase.ColumnType, testCase.RowCount, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        if (checkConsumed && (reader.BufferedBytes != 0 || stream.Position != stream.Length))
        {
            column.Dispose();
            throw new InvalidDataException($"The decode left {reader.BufferedBytes + stream.Length - stream.Position} of {sourceBytes.Length} bytes.");
        }

        return new Block(string.Empty, BlockInfo.Default, testCase.RowCount, new[] { column }, ColumnCodecRegistry.Default, Context);
    }

    private FacetResult Evaluate(Facet facet)
    {
        Arm reference = registry.Reference(facet.Tier);
        RangeOutcomes referenceOutcomes = reference is null ? null : Run(reference, facet);
        var candidates = registry.CandidatesFor(facet).Select(arm => (arm, Run(arm, facet))).ToList();
        return new FacetResult(facet, reference, referenceOutcomes, candidates);
    }

    private RangeOutcomes Run(Arm arm, Facet facet)
    {
        switch (arm)
        {
            case ReadArm read:
                return new RangeOutcomes(
                    (Outcome)Invoke(ReadMethod, facet.Target, this, read, 0, testCase.RowCount),
                    hasTail ? (Outcome)Invoke(ReadMethod, facet.Target, this, read, tailStart, testCase.RowCount - tailStart) : null);

            case WriteArm write:
                return new RangeOutcomes(
                    (Outcome)Invoke(WriteMethod, facet.Input.ElementType, this, write, facet, 0, testCase.RowCount),
                    hasTail ? (Outcome)Invoke(WriteMethod, facet.Input.ElementType, this, write, facet, tailStart, testCase.RowCount - tailStart) : null);

            case AnswerArm answer:
                try
                {
                    return new RangeOutcomes(Outcome.OfAnswer(answer.Answer(testCase.ColumnType, facet.Target ?? facet.Input.ElementType)), tail: null);
                }
                catch (Exception e)
                {
                    return new RangeOutcomes(Outcome.Failure(e), tail: null);
                }

            default:
                throw new InvalidOperationException($"The arm '{arm.Name}' is a {arm.GetType().Name}, which the engine cannot run.");
        }
    }

    private Outcome Read<T>(ReadArm arm, int start, int count)
    {
        using Block block = Decode();
        RowReader<T> reader;
        try
        {
            reader = arm.Bind<T>(block);
        }
        catch (Exception e)
        {
            return Outcome.Refusal(e);
        }

        try
        {
            T[] values = reader(start, count);
            return values?.Length == count
                ? Outcome.OfValues(values)
                : Outcome.Failure(new ArmInvariantException($"The reader gave {values?.Length.ToString() ?? "null"} values for {count} rows."));
        }
        catch (Exception e)
        {
            return Outcome.Failure(e);
        }
    }

    private Outcome Write<T>(WriteArm arm, Facet facet, int start, int length)
    {
        Block block = null;
        IColumn column = null;
        try
        {
            switch (facet.Input.Kind)
            {
                case WriteInputKind.Built:
                    column = facet.Input.Build("value");
                    break;

                case WriteInputKind.Decoded:
                    block = Decode();
                    column = block[0];
                    break;

                default:
                    Outcome values = ReadBackValues(facet.Input.ElementType);
                    if (values.Kind != OutcomeKind.Values)
                    {
                        return values;
                    }

                    column = (IColumn)Invoke(ReadBackColumnMethod, facet.Input.ElementType, null, testCase.ColumnType, values);
                    break;
            }

            if (column is not IColumn<T> typed)
            {
                return Outcome.Failure(new ArmInvariantException($"The write input is a {column.GetType().Name}, which is no IColumn<{TypeNames.Of(typeof(T))}>."));
            }

            SliceWriter writer;
            try
            {
                writer = arm.Bind(typed, testCase.ColumnType, Context);
            }
            catch (Exception e)
            {
                return Outcome.Refusal(e);
            }

            try
            {
                return Outcome.OfBytes(Capture(writer, start, length));
            }
            catch (Exception e)
            {
                return Outcome.Failure(e);
            }
        }
        finally
        {
            if (block is not null)
            {
                block.Dispose();
            }
            else
            {
                column?.Dispose();
            }
        }
    }

    // The values of the reference read as the target: ReadAs first, then Poco. With no reference, the first
    // ReadAs candidate.
    private Outcome ReadBackValues(Type target)
    {
        foreach (Tier tier in new[] { Tier.ReadAs, Tier.Poco })
        {
            FacetResult result = results.Values.FirstOrDefault(r => r.Facet.Tier == tier && r.Facet.Target == target);
            Outcome all = result?.Reference?.All ?? result?.Candidates.FirstOrDefault().Outcomes?.All;
            if (all?.Kind == OutcomeKind.Values)
            {
                return all;
            }
        }

        return Outcome.Unavailable($"no read as {TypeNames.Of(target)} gives values");
    }

    private void Compare(FacetResult result)
    {
        Facet facet = result.Facet;
        var arms = new List<(Arm Arm, RangeOutcomes Outcomes)>();
        if (result.Reference is not null)
        {
            arms.Add((result.ReferenceArm, result.Reference));
        }

        arms.AddRange(result.Candidates);
        foreach ((Arm arm, RangeOutcomes outcomes) in arms)
        {
            CheckInvariants(facet, arm, outcomes);
        }

        DeliberateChange change;
        try
        {
            change = registry.ChangeFor(facet);
        }
        catch (InvalidOperationException e)
        {
            mismatches.Add(e.Message);
            return;
        }

        if (change is null)
        {
            CompareWithBaseline(facet, arms);
        }
        else
        {
            CompareWithChange(facet, change, result);
        }
    }

    private void CheckInvariants(Facet facet, Arm arm, RangeOutcomes outcomes)
    {
        foreach (Outcome outcome in new[] { outcomes.All, outcomes.Tail })
        {
            if (outcome?.ExceptionType == typeof(ArmInvariantException))
            {
                mismatches.Add($"{facet}: {arm.Name} broke a rule of its own: {outcome.Message}");
                return;
            }
        }

        if (!facet.ReadsValues || outcomes.Tail is null)
        {
            return;
        }

        string difference = outcomes.All.Kind switch
        {
            OutcomeKind.Values => Outcome.Difference(outcomes.All.Tail(tailStart), outcomes.Tail),
            OutcomeKind.Refused => Outcome.Difference(outcomes.All, outcomes.Tail),
            _ => null,
        };

        if (difference is not null)
        {
            mismatches.Add(
                $"{facet}: {arm.Name} reads rows [{tailStart}, {testCase.RowCount}) alone as {outcomes.Tail}, " +
                $"and the same rows of the read of all rows as {(outcomes.All.Kind == OutcomeKind.Values ? outcomes.All.Tail(tailStart) : outcomes.All)}: {difference}.");
        }
    }

    private void CompareWithBaseline(Facet facet, List<(Arm Arm, RangeOutcomes Outcomes)> arms)
    {
        if (arms.Count < 2)
        {
            return;
        }

        (Arm baselineArm, RangeOutcomes baseline) = arms[0];
        foreach ((Arm arm, RangeOutcomes outcomes) in arms.Skip(1))
        {
            foreach (Rows rows in new[] { Rows.All, Rows.Tail })
            {
                Outcome expected = baseline.For(rows);
                Outcome actual = outcomes.For(rows);
                if (expected is null || actual is null)
                {
                    continue;
                }

                string difference = Outcome.Difference(expected, actual);
                if (difference is not null)
                {
                    mismatches.Add($"{facet}{Describe(facet, rows)}: {arm.Name} gives {actual}; {baselineArm.Name} gives {expected}: {difference}.");
                }
            }
        }
    }

    private void CompareWithChange(Facet facet, DeliberateChange change, FacetResult result)
    {
        Expectation expected = change.ExpectationFor(facet);
        if (result.Reference is not null && expected.Verify(result.Reference.All, Rows.All, tailStart, this) is null)
        {
            mismatches.Add(
                $"{facet}: the deliberate change '{change.Name}' expects {expected}, and {result.ReferenceArm.Name} already gives that ({result.Reference.All}). " +
                "A deliberate change must differ from the reference.");
        }

        if (result.Candidates.Count == 0)
        {
            mismatches.Add($"{facet}: the deliberate change '{change.Name}' selects this facet, but no candidate arm covers it.");
            return;
        }

        foreach ((Arm arm, RangeOutcomes outcomes) in result.Candidates)
        {
            foreach (Rows rows in new[] { Rows.All, Rows.Tail })
            {
                Outcome actual = outcomes.For(rows);
                string difference = actual is null ? null : expected.Verify(actual, rows, tailStart, this);
                if (difference is not null)
                {
                    mismatches.Add($"{facet}{Describe(facet, rows)}: {arm.Name} gives {actual}; the deliberate change '{change.Name}' expects {expected}: {difference}.");
                }
            }
        }
    }

    // The row range, for a facet that has one.
    private string Describe(Facet facet, Rows rows)
        => facet.IsAnswer ? string.Empty : rows == Rows.All ? $" rows [0, {testCase.RowCount})" : $" rows [{tailStart}, {testCase.RowCount})";
}
