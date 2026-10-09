using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>The test class that a <see cref="DifferentialCase"/> comes from.</summary>
public enum CaseSource
{
    /// <summary>A case of <c>InsertRoundTripCase.Cases()</c>, for a server with every feature.</summary>
    InsertRoundTrip,

    /// <summary>A case of <c>CompositeLiftMatrixTests.Cases()</c>.</summary>
    CompositeLiftMatrix,

    /// <summary>A column type that <c>ColumnReadProjectionTests</c> reads, with each CLR type it reads that type as.</summary>
    ColumnReadProjection,
}

/// <summary>What a <see cref="WriteInput"/> writes.</summary>
public enum WriteInputKind
{
    /// <summary>A column that the case builds.</summary>
    Built,

    /// <summary>The decoded column, written again (the dense write path).</summary>
    Decoded,

    /// <summary>The values that the reference read gives for one read target, in an array column.</summary>
    ReadBack,
}

/// <summary>
/// One case of the differential tests: a column type, the column that the reference writes and decodes, the CLR
/// types to read the decoded column as, and the columns to write.
/// </summary>
public sealed class DifferentialCase
{
    internal DifferentialCase(
        string id,
        CaseSource source,
        string columnType,
        int rowCount,
        IReadOnlyList<Type> readTargets,
        IReadOnlyList<WriteInput> writeInputs)
    {
        if (writeInputs.Count == 0 || writeInputs[0].Kind != WriteInputKind.Built)
        {
            throw new ArgumentException($"Case '{id}': the first write input must be a built column, because its bytes are the bytes that the read facets decode.", nameof(writeInputs));
        }

        Id = id;
        Source = source;
        ColumnType = columnType;
        RowCount = rowCount;
        ReadTargets = readTargets;
        WriteInputs = writeInputs;
    }

    /// <summary>The unique name of the case. A deliberate change names the case by this value.</summary>
    public string Id { get; }

    /// <summary>The test class that the case comes from.</summary>
    public CaseSource Source { get; }

    /// <summary>The ClickHouse type of the column.</summary>
    public string ColumnType { get; }

    /// <summary>The number of rows of every column of the case.</summary>
    public int RowCount { get; }

    /// <summary>The CLR types that the read facets read the decoded column as.</summary>
    public IReadOnlyList<Type> ReadTargets { get; }

    /// <summary>
    /// The columns that the write facets write. The first is the source: the reference writes it, and the read
    /// facets decode those bytes.
    /// </summary>
    public IReadOnlyList<WriteInput> WriteInputs { get; }

    /// <summary>The facets of the case: each read target in each read tier, then each write input in each write tier.</summary>
    /// <returns>The facets, in a fixed order.</returns>
    internal IEnumerable<Facet> Facets()
    {
        foreach (Type target in ReadTargets)
        {
            yield return Facet.Read(this, Tier.ReadAs, target);
            yield return Facet.Read(this, Tier.Poco, target);
            yield return Facet.Read(this, Tier.CanRead, target);
        }

        foreach (WriteInput input in WriteInputs)
        {
            yield return Facet.Write(this, Tier.Write, input);
            yield return Facet.Write(this, Tier.CanWrite, input);
        }
    }

    /// <inheritdoc/>
    public override string ToString() => Id;
}

/// <summary>One column that the write facets of a case write.</summary>
public sealed class WriteInput
{
    private WriteInput(string label, WriteInputKind kind, Type elementType, Func<string, IColumn> build)
    {
        Label = label;
        Kind = kind;
        ElementType = elementType;
        Build = build;
    }

    /// <summary>The name of the input in its case, for example <c>insert</c> or <c>read back as DateTime[]</c>.</summary>
    public string Label { get; }

    /// <summary>What the input writes.</summary>
    public WriteInputKind Kind { get; }

    /// <summary>The CLR element type of the column.</summary>
    public Type ElementType { get; }

    /// <summary>Builds the column with a given name. Set only for <see cref="WriteInputKind.Built"/>.</summary>
    internal Func<string, IColumn> Build { get; }

    /// <summary>A column that the case builds.</summary>
    /// <param name="label">The name of the input in its case.</param>
    /// <param name="elementType">The CLR element type of the column.</param>
    /// <param name="build">Builds the column with a given name.</param>
    /// <returns>The input.</returns>
    internal static WriteInput Built(string label, Type elementType, Func<string, IColumn> build)
        => new(label, WriteInputKind.Built, elementType, build);

    /// <summary>The decoded column, written again.</summary>
    /// <param name="elementType">The CLR element type of the decoded column.</param>
    /// <returns>The input.</returns>
    internal static WriteInput Decoded(Type elementType) => new("decoded", WriteInputKind.Decoded, elementType, build: null);

    /// <summary>The values that the reference read gives for <paramref name="target"/>.</summary>
    /// <param name="target">The read target whose values to write.</param>
    /// <returns>The input.</returns>
    internal static WriteInput ReadBack(Type target) => new($"read back as {TypeNames.Of(target)}", WriteInputKind.ReadBack, target, build: null);

    /// <inheritdoc/>
    public override string ToString() => Label;
}

/// <summary>An entry point of the client that the differential tests compare.</summary>
internal enum Tier
{
    /// <summary><c>Block.ReadAs&lt;T&gt;</c>: the values of a column as <c>T</c>.</summary>
    ReadAs,

    /// <summary>The POCO read plan: the values of a column in the <c>Value</c> property of <c>Row&lt;T&gt;</c>.</summary>
    Poco,

    /// <summary><c>ClickHouseTcpTypes.CanRead</c>.</summary>
    CanRead,

    /// <summary>The write of a column: <c>BeginWrite</c>, the state prefix and the body.</summary>
    Write,

    /// <summary><c>ClickHouseTcpTypes.CanWrite</c>.</summary>
    CanWrite,
}

/// <summary>One thing that a case checks: a read target in a read tier, or a write input in a write tier.</summary>
internal sealed class Facet
{
    private Facet(DifferentialCase testCase, Tier tier, Type target, WriteInput input, string name)
    {
        Case = testCase;
        Tier = tier;
        Target = target;
        Input = input;
        Name = name;
    }

    /// <summary>The case of the facet.</summary>
    public DifferentialCase Case { get; }

    /// <summary>The entry point that the facet checks.</summary>
    public Tier Tier { get; }

    /// <summary>The read target, for <see cref="Tier.ReadAs"/>, <see cref="Tier.Poco"/> and <see cref="Tier.CanRead"/>.</summary>
    public Type Target { get; }

    /// <summary>The write input, for <see cref="Tier.Write"/> and <see cref="Tier.CanWrite"/>.</summary>
    public WriteInput Input { get; }

    /// <summary>The name of the facet in its case, for example <c>ReadAs&lt;DateTime&gt;</c> or <c>Write[insert]</c>.</summary>
    public string Name { get; }

    /// <summary>Whether the facet reads values (<see cref="Tier.ReadAs"/> or <see cref="Tier.Poco"/>).</summary>
    public bool ReadsValues => Tier is Tier.ReadAs or Tier.Poco;

    /// <summary>Whether the facet gives a yes-or-no answer (<see cref="Tier.CanRead"/> or <see cref="Tier.CanWrite"/>).</summary>
    public bool IsAnswer => Tier is Tier.CanRead or Tier.CanWrite;

    public static Facet Read(DifferentialCase testCase, Tier tier, Type target)
        => new(testCase, tier, target, input: null, $"{tier}<{TypeNames.Of(target)}>");

    public static Facet Write(DifferentialCase testCase, Tier tier, WriteInput input)
        => new(testCase, tier, target: null, input, $"{tier}[{input.Label}]");

    /// <inheritdoc/>
    public override string ToString() => $"{Case.Id} {Name}";
}
