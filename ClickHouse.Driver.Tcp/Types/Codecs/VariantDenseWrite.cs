using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The write of the column of one alternative of a <c>Variant</c>, for a range of its rows.</summary>
internal interface IVariantAlternativeWrite
{
    /// <summary>Begins the write of rows [<paramref name="start"/>, <paramref name="start"/> + <paramref name="length"/>).</summary>
    /// <param name="values">The column of the alternative.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <returns>The state that the prefix and the body share, or <see langword="null"/>.</returns>
    IColumnWriteState Begin(IColumn values, int start, int length);

    /// <summary>Writes the state prefix of the rows.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The column of the alternative.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    void WritePrefix(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state);

    /// <summary>Writes the body of the rows.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The column of the alternative.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    void Write(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state);
}

/// <summary>
/// The write of a decoded <c>Variant</c> column from its storage, into a <c>Variant</c> of the same alternatives or of
/// more alternatives: the discriminators mode and the prefix of each alternative of the target, then the discriminator
/// of each row (through the map from the alternatives of the source to those of the target, the NULL marker kept), then
/// the values of each alternative of the target. An alternative of the target that the source does not have holds no row.
/// </summary>
/// <remarks>
/// The column must be a <see cref="VariantColumn"/>, or a column with its invariants: each discriminator is the index of
/// an alternative or the NULL marker, and <see cref="IVariantColumn.LocalIndices"/> gives the index of each row in the
/// column of its alternative.
/// </remarks>
internal sealed class VariantDenseWrite
{
    private readonly string typeName;
    private readonly IVariantAlternativeWrite[] alternatives;
    private readonly int[] sourceOf;
    private readonly IColumn[] unpaired;
    private readonly byte[] discriminators;
    private readonly int sourceCount;

    /// <summary>Initializes the write.</summary>
    /// <param name="typeName">The target type, for the messages.</param>
    /// <param name="alternatives">The write of each alternative of the target, in its discriminator order.</param>
    /// <param name="targetOfSource">
    /// The discriminator in the target of each alternative of the source, or <see langword="null"/> when the source has the
    /// alternatives of the target in the same order.
    /// </param>
    /// <param name="unpaired">
    /// For each alternative of the target that the source does not have, an empty column that its write takes; else
    /// <see langword="null"/>. Not used when <paramref name="targetOfSource"/> is <see langword="null"/>.
    /// </param>
    public VariantDenseWrite(string typeName, IVariantAlternativeWrite[] alternatives, int[] targetOfSource, IColumn[] unpaired)
    {
        this.typeName = typeName;
        this.alternatives = alternatives;
        sourceOf = new int[alternatives.Length];
        if (targetOfSource is null)
        {
            sourceCount = alternatives.Length;
            for (int j = 0; j < sourceOf.Length; j++)
            {
                sourceOf[j] = j;
            }

            return;
        }

        sourceCount = targetOfSource.Length;
        this.unpaired = unpaired;
        sourceOf.AsSpan().Fill(-1);
        discriminators = new byte[256];
        discriminators.AsSpan().Fill(IVariantColumn.NullDiscriminator);
        for (int i = 0; i < targetOfSource.Length; i++)
        {
            sourceOf[targetOfSource[i]] = i;
            discriminators[i] = (byte)targetOfSource[i];
        }
    }

    /// <summary>
    /// Begins the write of rows [<paramref name="start"/>, <paramref name="start"/> + <paramref name="length"/>): the
    /// rows of each alternative are the run of its values whose rows are in the range.
    /// </summary>
    /// <param name="column">The decoded column.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <returns>The state that the prefix and the body share. The caller disposes it.</returns>
    public IColumnWriteState Begin(IVariantColumn column, int start, int length)
    {
        // The first row of an alternative in the range has the number of its values before the range as its local
        // index, so one pass over the range gives the run of each alternative.
        var runStart = new int[sourceCount];
        var runLength = new int[sourceCount];
        ReadOnlySpan<byte> rows = column.Discriminators;
        ReadOnlySpan<int> localIndices = column.LocalIndices;
        for (int r = start; r < start + length; r++)
        {
            byte d = rows[r];
            if (d == IVariantColumn.NullDiscriminator)
            {
                continue;
            }

            if (runLength[d] == 0)
            {
                runStart[d] = localIndices[r];
            }

            runLength[d]++;
        }

        var state = new VariantWriteState(alternatives.Length);
        try
        {
            for (int j = 0; j < alternatives.Length; j++)
            {
                int i = sourceOf[j];
                if (i >= 0)
                {
                    state.Columns[j] = column.GetTypeColumn(i);
                    state.Starts[j] = runStart[i];
                    state.Lengths[j] = runLength[i];
                }
                else
                {
                    state.Columns[j] = unpaired[j];
                }

                state.States[j] = alternatives[j].Begin(state.Columns[j], state.Starts[j], state.Lengths[j]);
            }

            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <summary>Writes the discriminators mode, then the prefix of each alternative of the target.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    public void WritePrefix(ClickHouseBinaryWriter writer, IColumnWriteState state)
    {
        var own = state.Expect<VariantWriteState>(typeName);
        writer.WriteUInt64(VariantWire.BasicDiscriminatorsMode);
        for (int j = 0; j < alternatives.Length; j++)
        {
            alternatives[j].WritePrefix(writer, own.Columns[j], own.Starts[j], own.Lengths[j], own.States[j]);
        }
    }

    /// <summary>Writes the discriminator of each row, then the values of each alternative of the target.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The decoded column.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    public void Write(ClickHouseBinaryWriter writer, IVariantColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<VariantWriteState>(typeName);
        ReadOnlySpan<byte> rows = column.Discriminators.Slice(start, length);
        if (discriminators is null)
        {
            writer.WriteBytes(rows);
        }
        else
        {
            foreach (byte d in rows)
            {
                writer.WriteByte(discriminators[d]);
            }
        }

        for (int j = 0; j < alternatives.Length; j++)
        {
            alternatives[j].Write(writer, own.Columns[j], own.Starts[j], own.Lengths[j], own.States[j]);
        }
    }

    // The column, the run and the write state of each alternative of the target, for one range of rows. The columns are
    // borrowed from the decoded column, so the state owns none of them.
    private sealed class VariantWriteState : IColumnWriteState
    {
        public VariantWriteState(int count)
        {
            Columns = new IColumn[count];
            Starts = new int[count];
            Lengths = new int[count];
            States = new IColumnWriteState[count];
        }

        public IColumn[] Columns { get; }

        public int[] Starts { get; }

        public int[] Lengths { get; }

        public IColumnWriteState[] States { get; }

        public void Dispose()
        {
            for (int j = 0; j < States.Length; j++)
            {
                States[j]?.Dispose();
                States[j] = null;
            }
        }
    }
}
