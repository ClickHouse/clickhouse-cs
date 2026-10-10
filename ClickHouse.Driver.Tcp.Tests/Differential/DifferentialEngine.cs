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
    public FacetResult(Facet facet, IReadOnlyList<(Arm Arm, RangeOutcomes Outcomes)> candidates)
    {
        Facet = facet;
        Candidates = candidates;
    }

    public Facet Facet { get; }

    /// <summary>The outcomes of each candidate that covers the facet, in the order the candidates were added.</summary>
    public IReadOnlyList<(Arm Arm, RangeOutcomes Outcomes)> Candidates { get; }

    /// <summary>The arm of the first candidate, or null when no candidate covers the facet.</summary>
    public Arm BaselineArm => Candidates.Count == 0 ? null : Candidates[0].Arm;

    /// <summary>The outcomes of the first candidate, or null when no candidate covers the facet.</summary>
    public RangeOutcomes Baseline => Candidates.Count == 0 ? null : Candidates[0].Outcomes;
}

/// <summary>The result of a case: the outcomes of each facet, and each difference that the case found.</summary>
internal sealed class CaseReport
{
    public CaseReport(DifferentialCase testCase, IReadOnlyList<FacetResult> facets, IReadOnlyList<string> mismatches, int tailStart)
    {
        Case = testCase;
        Facets = facets;
        Mismatches = mismatches;
        TailStart = tailStart;
    }

    public DifferentialCase Case { get; }

    /// <summary>The first row of the tail in the column that the tail reads or writes. Zero when the case has no rows.</summary>
    public int TailStart { get; }

    public IReadOnlyList<FacetResult> Facets { get; }

    /// <summary>One message for each difference. Empty when every candidate agrees with the baseline.</summary>
    public IReadOnlyList<string> Mismatches { get; }
}

/// <summary>
/// Runs the facets of a case with every arm of a registry, and compares the outcomes.
/// </summary>
/// <remarks>
/// <para>
/// The baseline write of the case's first write input gives the bytes of the column. Each call of an arm gets
/// a block that is decoded from those bytes for that call only, with <c>ReadStatePrefixAsync</c> and
/// <c>ReadColumnAsync</c>. A cache that a column fills on its first read is therefore empty for each arm.
/// </para>
/// <para>
/// Each facet runs for all rows and for the tail. The tail of a case of two rows or more is
/// <c>[RowCount / 2, RowCount)</c>. The tail of a case of one row is row 1 of a column that has a row before the
/// case's row: for an <c>Array</c> type, the row's elements twice; for other types, a copy. So every read and every
/// write also runs with a start above zero, after a preceding row. The first candidate that covers a facet is its
/// baseline. Every other candidate must give the baseline's outcome for both ranges, or every candidate must give the
/// outcome that is declared for the facet. Each arm must also read the same values for the tail alone as for the same
/// rows of the case in the full read.
/// </para>
/// <para>
/// An outcome that the source test of the case states (<see cref="DifferentialCase.Stated"/>) must be the outcome of
/// the baseline.
/// </para>
/// </remarks>
internal sealed class DifferentialEngine
{
    private static readonly ConcurrentDictionary<string, CaseReport> CurrentReports = new(StringComparer.Ordinal);

    private static readonly MethodInfo ReadMethod = typeof(DifferentialEngine).GetMethod(nameof(Read), BindingFlags.NonPublic | BindingFlags.Instance);
    private static readonly MethodInfo WriteMethod = typeof(DifferentialEngine).GetMethod(nameof(Write), BindingFlags.NonPublic | BindingFlags.Instance);
    private static readonly MethodInfo ReadBackColumnMethod = typeof(DifferentialEngine).GetMethod(nameof(ReadBackColumn), BindingFlags.NonPublic | BindingFlags.Static);

    private readonly DifferentialCase testCase;
    private readonly DifferentialRegistry registry;
    private readonly Dictionary<Facet, FacetResult> results = new();
    private readonly List<string> mismatches = new();
    private readonly bool hasTail;
    private readonly bool tailHasPrecedingRow;
    private readonly int tailStart;
    private readonly int tailCount;
    private readonly int tailCaseRow;
    private byte[] sourceBytes;
    private byte[] precededSourceBytes;

    private DifferentialEngine(DifferentialCase testCase, DifferentialRegistry registry)
    {
        this.testCase = testCase;
        this.registry = registry;
        hasTail = testCase.RowCount >= 1;
        tailHasPrecedingRow = testCase.RowCount == 1;
        tailStart = tailHasPrecedingRow ? 1 : testCase.RowCount / 2;
        tailCount = tailHasPrecedingRow ? 1 : testCase.RowCount - tailStart;
        tailCaseRow = tailHasPrecedingRow ? 0 : tailStart;
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
        return new CaseReport(testCase, engine.results.Values.ToList(), engine.mismatches, engine.hasTail ? engine.tailStart : 0);
    }

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

    private static Array Twice(Array elements)
    {
        Array twice = Array.CreateInstance(elements.GetType().GetElementType(), elements.Length * 2);
        Array.Copy(elements, 0, twice, 0, elements.Length);
        Array.Copy(elements, 0, twice, elements.Length, elements.Length);
        return twice;
    }

    private void Execute()
    {
        List<Facet> facets = testCase.Facets().ToList();

        if (!EncodeSource(facets))
        {
            return;
        }

        // Read facets first: a read-back write input writes the values of the baseline read.
        foreach (Facet facet in facets.Where(f => f.Input is null).Concat(facets.Where(f => f.Input is not null)))
        {
            results[facet] = Evaluate(facet);
        }

        foreach (Facet facet in facets)
        {
            Compare(results[facet]);
        }

        CheckStated();
    }

    private bool EncodeSource(List<Facet> facets)
    {
        Facet source = facets.First(f => f.Tier == Tier.Write && f.Input == testCase.WriteInputs[0]);
        var arm = (WriteArm)registry.CandidatesFor(source).FirstOrDefault();
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
        if (tailHasPrecedingRow)
        {
            Outcome preceded = (Outcome)Invoke(WriteMethod, source.Input.ElementType, this, arm, source, 0, 2, true);
            if (preceded.Kind != OutcomeKind.Bytes)
            {
                mismatches.Add($"{source}: {arm.Name} cannot write the source column with a preceding row: {preceded}.");
                return false;
            }

            precededSourceBytes = preceded.Bytes;
        }

        try
        {
            using Block block = Decode(preceded: false, checkConsumed: true);
            using Block precededBlock = tailHasPrecedingRow ? Decode(preceded: true, checkConsumed: true) : null;
        }
        catch (Exception e)
        {
            mismatches.Add($"{source}: the bytes of {arm.Name} do not decode: {e.GetType().Name}: {e.Message}");
            return false;
        }

        return true;
    }

    // A block of the source rows, or of the source rows with a preceding row.
    private Block Decode(bool preceded, bool checkConsumed = false)
    {
        byte[] bytes = preceded ? precededSourceBytes : sourceBytes;
        int rows = preceded ? testCase.RowCount + 1 : testCase.RowCount;
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(testCase.ColumnType, Context);
        using var stream = new MemoryStream(bytes);
        using var reader = new ClickHouseBinaryReader(stream);
        codec.ReadStatePrefixAsync(reader, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        IColumn column = codec.ReadColumnAsync(reader, "value", testCase.ColumnType, rows, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        if (checkConsumed && (reader.BufferedBytes != 0 || stream.Position != stream.Length))
        {
            column.Dispose();
            throw new InvalidDataException($"The decode left {reader.BufferedBytes + stream.Length - stream.Position} of {bytes.Length} bytes.");
        }

        return new Block(string.Empty, BlockInfo.Default, rows, new[] { column }, ColumnCodecRegistry.Default, Context);
    }

    // The column with a row before its only row. For an Array type the preceding row has the row's elements twice, so
    // the tail's child offset is not 0 and the preceding row differs from the row. For other types it is a copy.
    private IColumn WithPrecedingRow<T>(IColumn column)
    {
        if (column is not ArrayColumn<T> array || array.RowCount != 1)
        {
            throw new InvalidOperationException(
                $"{testCase.Id}: no row can be put before the only row of a {column.GetType().Name}. Give the case two rows or more.");
        }

        T row = array[0];
        T preceding = row is Array { Length: > 0 } elements && TypeParser.Parse(testCase.ColumnType).Name == "Array"
            ? (T)(object)Twice(elements)
            : row;
        var preceded = new ArrayColumn<T>(column.Name, column.TypeName, new[] { preceding, row });
        column.Dispose();
        return preceded;
    }

    private FacetResult Evaluate(Facet facet)
        => new(facet, registry.CandidatesFor(facet).Select(arm => (arm, Run(arm, facet))).ToList());

    private RangeOutcomes Run(Arm arm, Facet facet)
    {
        switch (arm)
        {
            case ReadArm read:
                return new RangeOutcomes(
                    (Outcome)Invoke(ReadMethod, facet.Target, this, read, 0, testCase.RowCount, false),
                    hasTail ? (Outcome)Invoke(ReadMethod, facet.Target, this, read, tailStart, tailCount, tailHasPrecedingRow) : null);

            case WriteArm write:
                return new RangeOutcomes(
                    (Outcome)Invoke(WriteMethod, facet.Input.ElementType, this, write, facet, 0, testCase.RowCount, false),
                    hasTail ? (Outcome)Invoke(WriteMethod, facet.Input.ElementType, this, write, facet, tailStart, tailCount, tailHasPrecedingRow) : null);

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

    private Outcome Read<T>(ReadArm arm, int start, int count, bool preceded)
    {
        using Block block = Decode(preceded);
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

    private Outcome Write<T>(WriteArm arm, Facet facet, int start, int length, bool preceded)
    {
        Block block = null;
        IColumn column = null;
        try
        {
            switch (facet.Input.Kind)
            {
                case WriteInputKind.Built:
                    column = facet.Input.Build("value");
                    column = preceded ? WithPrecedingRow<T>(column) : column;
                    break;

                case WriteInputKind.Decoded:
                    block = Decode(preceded);
                    column = block[0];
                    break;

                default:
                    Outcome values = ReadBackValues(facet.Input.ElementType);
                    if (values.Kind != OutcomeKind.Values)
                    {
                        return values;
                    }

                    column = (IColumn)Invoke(ReadBackColumnMethod, facet.Input.ElementType, null, testCase.ColumnType, values);
                    column = preceded ? WithPrecedingRow<T>(column) : column;
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

    // The values of the baseline read as the target: ReadAs first, then Poco.
    private Outcome ReadBackValues(Type target)
    {
        foreach (Tier tier in new[] { Tier.ReadAs, Tier.Poco })
        {
            FacetResult result = results.Values.FirstOrDefault(r => r.Facet.Tier == tier && r.Facet.Target == target);
            Outcome all = result?.Baseline?.All;
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
        foreach ((Arm arm, RangeOutcomes outcomes) in result.Candidates)
        {
            CheckInvariants(facet, arm, outcomes);
        }

        DeclaredOutcome declared;
        try
        {
            declared = registry.DeclaredFor(facet);
        }
        catch (InvalidOperationException e)
        {
            mismatches.Add(e.Message);
            return;
        }

        if (declared is null)
        {
            CompareWithBaseline(facet, result.Candidates);
        }
        else
        {
            CompareWithDeclared(facet, declared, result);
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

        // A read of all rows that fails can have a tail that fails too or that gives values, because the tail may not
        // contain the row that fails. It cannot have a tail that is refused.
        string difference = outcomes.All.Kind switch
        {
            OutcomeKind.Values => Outcome.Difference(outcomes.All.Tail(tailCaseRow), outcomes.Tail),
            OutcomeKind.Refused => Outcome.Difference(outcomes.All, outcomes.Tail),
            OutcomeKind.Failed when outcomes.Tail.Kind is not (OutcomeKind.Failed or OutcomeKind.Values)
                => $"the read of all rows fails, and the tail is {outcomes.Tail.Kind}",
            _ => null,
        };

        if (difference is not null)
        {
            mismatches.Add(
                $"{facet}: {arm.Name} reads{Describe(facet, Rows.Tail)} alone as {outcomes.Tail}, " +
                $"and the same rows of the case in the read of all rows as {(outcomes.All.Kind == OutcomeKind.Values ? outcomes.All.Tail(tailCaseRow) : outcomes.All)}: {difference}.");
        }
    }

    private void CompareWithBaseline(Facet facet, IReadOnlyList<(Arm Arm, RangeOutcomes Outcomes)> arms)
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

    private void CompareWithDeclared(Facet facet, DeclaredOutcome declared, FacetResult result)
    {
        Expectation expected = declared.ExpectationFor(facet);
        if (result.Candidates.Count == 0)
        {
            mismatches.Add($"{facet}: the declared outcome '{declared.Name}' selects this facet, but no candidate arm covers it.");
            return;
        }

        foreach ((Arm arm, RangeOutcomes outcomes) in result.Candidates)
        {
            foreach (Rows rows in new[] { Rows.All, Rows.Tail })
            {
                Outcome actual = outcomes.For(rows);
                string difference = actual is null ? null : expected.Verify(actual, rows, tailCaseRow);
                if (difference is not null)
                {
                    mismatches.Add($"{facet}{Describe(facet, rows)}: {arm.Name} gives {actual}; the declared outcome '{declared.Name}' expects {expected}: {difference}.");
                }
            }
        }
    }

    // Each outcome that the source test states must be the outcome of the baseline.
    private void CheckStated()
    {
        foreach (StatedOutcome stated in testCase.Stated)
        {
            FacetResult result = results.Values.FirstOrDefault(r => r.Facet.Tier == stated.Tier && r.Facet.Target == stated.Target);
            (Arm arm, RangeOutcomes outcomes) = (result?.BaselineArm, result?.Baseline);
            if (arm is null)
            {
                mismatches.Add($"{testCase.Id}: the case states an outcome for {stated.Tier}<{TypeNames.Of(stated.Target)}>, and no arm runs that facet.");
                continue;
            }

            foreach (Rows rows in new[] { Rows.All, Rows.Tail })
            {
                Outcome actual = outcomes.For(rows);
                string difference = actual is null ? null : stated.Expected.Verify(actual, rows, tailCaseRow);
                if (difference is not null)
                {
                    mismatches.Add($"{result.Facet}{Describe(result.Facet, rows)}: {arm.Name} gives {actual}; the source test states {stated.Expected}: {difference}.");
                }
            }
        }
    }

    // The row range, for a facet that has one.
    private string Describe(Facet facet, Rows rows)
        => facet.IsAnswer ? string.Empty
            : rows == Rows.All ? $" rows [0, {testCase.RowCount})"
            : $" rows [{tailStart}, {tailStart + tailCount}){(tailHasPrecedingRow ? " after a preceding row" : string.Empty)}";
}
