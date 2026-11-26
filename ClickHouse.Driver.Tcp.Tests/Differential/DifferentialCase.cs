using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Tests.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>The test class that a <see cref="DifferentialCase"/> comes from.</summary>
public enum CaseSource
{
    /// <summary>A case of <c>InsertRoundTripCase.Cases()</c>, for a server with every feature.</summary>
    InsertRoundTrip,

    /// <summary>A case of <c>CompositeLiftMatrixTests.Cases()</c>.</summary>
    CompositeLiftMatrix,

    /// <summary>A column type of the read targets table of <c>DifferentialCases</c>, with the CLR types to read it as.</summary>
    ColumnReadProjection,

    /// <summary>A value or error scenario of <c>DifferentialCases</c>: its own values, read as its target.</summary>
    ColumnReadScenario,

    /// <summary>A scenario of <c>DifferentialCases</c> with values that fail some rows of every insert, or of every read.</summary>
    FailureScenario,
}

/// <summary>What a <see cref="WriteInput"/> writes.</summary>
public enum WriteInputKind
{
    /// <summary>A column that the case builds.</summary>
    Built,

    /// <summary>The decoded column, written again (the write from storage).</summary>
    Decoded,

    /// <summary>The values that <c>Block.ReadAs</c> gives for one read target, in an array column.</summary>
    ReadBack,
}

/// <summary>
/// One case of the differential tests: a column type, the column whose insert write gives the bytes that the reads
/// decode, the CLR types to read the decoded column as, and the columns to write.
/// </summary>
public sealed class DifferentialCase
{
    internal DifferentialCase(string id, CaseSource source, string columnType, int rowCount, IReadOnlyList<Type> readTargets, IReadOnlyList<WriteInput> writeInputs, ColumnReadScenario scenario = null)
    {
        if (writeInputs.Count == 0 || writeInputs[0].Kind != WriteInputKind.Built)
        {
            throw new ArgumentException($"Case '{id}': the first write input must be a built column, because its bytes are the bytes that the reads decode.", nameof(writeInputs));
        }

        Id = id;
        Source = source;
        ColumnType = columnType;
        RowCount = rowCount;
        ReadTargets = readTargets;
        WriteInputs = writeInputs;
        Scenario = scenario;
    }

    /// <summary>The unique name of the case.</summary>
    public string Id { get; }

    /// <summary>The test class that the case comes from.</summary>
    public CaseSource Source { get; }

    /// <summary>The ClickHouse type of the column.</summary>
    public string ColumnType { get; }

    /// <summary>The number of rows of every column of the case.</summary>
    public int RowCount { get; }

    /// <summary>The CLR types that the reads read the decoded column as.</summary>
    public IReadOnlyList<Type> ReadTargets { get; }

    /// <summary>The columns that the writes write. The first is the source: its insert write gives the bytes that the reads decode.</summary>
    public IReadOnlyList<WriteInput> WriteInputs { get; }

    /// <summary>The scenario of a <see cref="CaseSource.ColumnReadScenario"/> case, which states the outcome of <c>Block.ReadAs</c> of its target.</summary>
    internal ColumnReadScenario Scenario { get; }

    /// <inheritdoc/>
    public override string ToString() => Id;
}

/// <summary>One column that the writes of a case write.</summary>
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

    internal static WriteInput Built(string label, Type elementType, Func<string, IColumn> build) => new(label, WriteInputKind.Built, elementType, build);

    internal static WriteInput Decoded(Type elementType) => new("decoded", WriteInputKind.Decoded, elementType, build: null);

    internal static WriteInput ReadBack(Type target) => new($"read back as {TypeNames.Of(target)}", WriteInputKind.ReadBack, target, build: null);

    /// <inheritdoc/>
    public override string ToString() => Label;
}
