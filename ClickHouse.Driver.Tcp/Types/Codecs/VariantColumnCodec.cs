using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The wire constants for the variant serialization-state prefix.</summary>
internal static class VariantWire
{
    /// <summary>
    /// The discriminators-mode value in the state prefix selecting the BASIC layout, where every row's
    /// discriminator is written literally (as opposed to COMPACT run-length granules).
    /// </summary>
    public const ulong BasicDiscriminatorsMode = 0;
}

/// <summary>
/// A codec for the ClickHouse <c>Variant(T1, ..., Tn)</c> column — a discriminated union where each row holds a
/// value of exactly one alternative type, or NULL. The wire layout is columnar: a serialization-state prefix
/// (a <c>UInt64</c> discriminators mode), then one <c>UInt8</c> discriminator per row, then a dense run per
/// alternative type holding the values of the rows that selected it (in row order). NULL is the reserved
/// discriminator <c>255</c> and consumes no value from any run; the alternatives are therefore never themselves
/// <c>Nullable</c>. The server canonicalizes the alternatives (sorted by name) before sending the type string,
/// so the declared order already is the discriminator order — this codec does not reorder it.
///
/// <para>
/// The column data for <c>Variant(String, UInt64)</c> holding <c>[42, 'hi', NULL, 7, 'yo']</c>. <c>String</c> sorts
/// before <c>UInt64</c>, so discriminator <c>0</c> is the string alternative. Each run holds only its own rows, in
/// row order, and no run states its own length — a length is recoverable only by counting the discriminators.
/// <code>
/// 00 00 00 00 00 00 00 00  discriminators mode = 0 (BASIC)
///                          then one state prefix per alternative (both empty here)
/// 01 00 FF 01 00           one discriminator per row: UInt64, String, NULL, UInt64, String
/// 02 68 69                 String run, rows 1 and 4: len 2, "hi"
/// 02 79 6F                                           len 2, "yo"
/// 2A 00 00 00 00 00 00 00  UInt64 run, rows 0 and 3: 42
/// 07 00 00 00 00 00 00 00                            7
/// </code>
/// Reading row 4 therefore takes two steps: its discriminator says the string alternative, and the number of
/// earlier rows that also chose it says which value in that run — index 1, <c>"yo"</c>.
/// </para>
///
/// <para>
/// Only the BASIC discriminators mode (every row's discriminator written literally) is supported. COMPACT exists
/// for MergeTree part serialization, chosen by the table-level
/// <c>use_compact_variant_discriminators_serialization</c> setting; the native protocol's writer leaves that
/// setting at its default, so a server never sends COMPACT over the wire. A COMPACT prefix is rejected.
/// </para>
///
/// <para>
/// The codec writes a decoded <see cref="VariantColumn"/> of the same alternatives only (<see cref="CanWrite"/>): its
/// discriminator stream, then its per-type child columns, with no copy. The converter layer writes every other
/// column, and places each value by its CLR type.
/// </para>
/// </summary>
internal sealed class VariantColumnCodec : IColumnCodec
{
    // The discriminator is a single byte and 255 marks NULL, so at most 255 alternatives (indices 0..254) can be
    // addressed under the BASIC layout.
    private const int MaxTypes = 255;

    private readonly IColumnCodec[] children;

    private VariantColumnCodec(string typeName, IColumnCodec[] children)
    {
        TypeName = typeName;
        this.children = children;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <summary>
    /// The alternatives' ClickHouse type names, in discriminator order — alternative <c>i</c> is what discriminator
    /// <c>i</c> selects. For a spelled-out <c>Variant(...)</c> this only restates the type string, but for an alias
    /// the client expands itself (<c>Geometry</c>) it is the whole of the client's claim about the ordering, which
    /// nothing on the wire carries and which callers of the dense column depend on to mean what they intend.
    /// </summary>
    internal IReadOnlyList<string> AlternativeTypeNames => Array.ConvertAll(children, child => child.TypeName);

    /// <inheritdoc/>
    public Type ElementType => typeof(object);

    /// <summary>Builds a <c>Variant(...)</c> codec, resolving each alternative's codec through the registry.</summary>
    /// <param name="node">The parsed <c>Variant</c> node; its arguments are the alternative types in discriminator order.</param>
    /// <param name="context">The resolution context, forwarded to each alternative codec's factory.</param>
    /// <param name="registry">The registry used to resolve the alternative codecs.</param>
    /// <param name="typeName">The name to report as the codec's <see cref="IColumnCodec.TypeName"/>, or null to use
    /// <paramref name="node"/>'s own. An alias whose structure is a variant (<c>Geometry</c>) passes its own name so
    /// diagnostics name the type the server sent rather than the structure it stands for.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The variant has no alternatives, or an alternative is <c>Nullable</c>.</exception>
    /// <exception cref="NotSupportedException">The variant has more alternatives than the BASIC layout can address.</exception>
    public static VariantColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry, string typeName = null)
    {
        if (node.Arguments.Count == 0)
        {
            throw new FormatException($"Variant type '{node}' must have at least one alternative type argument.");
        }

        if (node.Arguments.Count > MaxTypes)
        {
            throw new NotSupportedException(
                $"Variant type '{node}' has {node.Arguments.Count} alternatives; the discriminator is one byte, so at most {MaxTypes} are addressable.");
        }

        var childCodecs = new IColumnCodec[node.Arguments.Count];
        for (int i = 0; i < childCodecs.Length; i++)
        {
            TypeNode argument = node.Arguments[i];
            if (string.Equals(argument.Name, "Nullable", StringComparison.Ordinal))
            {
                throw new FormatException(
                    $"Variant alternative '{argument}' must not be Nullable; a Variant carries NULL through its discriminator, not a nullable alternative.");
            }

            // Dynamic needs per-operation state that Variant does not propagate to alternatives.
            if (string.Equals(argument.Name, "Dynamic", StringComparison.Ordinal))
            {
                throw new FormatException(
                    $"Variant alternative '{argument}' must not be Dynamic; this client cannot write a Dynamic alternative, "
                    + "whose type list would desynchronize from its body.");
            }

            childCodecs[i] = registry.ResolveNode(argument, in context);
        }

        return new VariantColumnCodec(typeName ?? node.ToString(), childCodecs);
    }

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        ulong mode = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
        if (mode != VariantWire.BasicDiscriminatorsMode)
        {
            throw new ClickHouseTcpProtocolException(
                $"Variant column '{TypeName}' uses discriminators mode {mode}; this client only supports BASIC (0).");
        }

        foreach (IColumnCodec child in children)
        {
            await child.ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        // A zero-row block puts neither discriminators nor values on the wire, so there is nothing to count and
        // every run is empty.
        if (rowCount == 0)
        {
            IColumn[] emptyRuns = await ReadTypeColumnsAsync(reader, columnName, new int[children.Length], cancellationToken).ConfigureAwait(false);
            return new VariantColumn(columnName, columnType, Array.Empty<byte>(), emptyRuns, 0, pooledDiscriminators: false, ownsColumns: true);
        }

        byte[] discriminators = ArrayPool<byte>.Shared.Rent(rowCount);
        IColumn[] typeColumns = null;
        try
        {
            await reader.ReadBytesAsync(discriminators.AsMemory(0, rowCount), cancellationToken).ConfigureAwait(false);

            // No run states its own length on the wire, so the whole discriminator stream has to be counted before
            // a single run can be read.
            int[] rowsPerType = CountRowsPerType(discriminators, rowCount, columnName, columnType);
            typeColumns = await ReadTypeColumnsAsync(reader, columnName, rowsPerType, cancellationToken).ConfigureAwait(false);

            return new VariantColumn(columnName, columnType, discriminators, typeColumns, rowCount, pooledDiscriminators: true, ownsColumns: true);
        }
        catch
        {
            // The column that would have taken over the rented buffer and the runs was never constructed.
            // typeColumns stays null when ReadTypeColumnsAsync fails, having already disposed its own partial read.
            ArrayPool<byte>.Shared.Return(discriminators);
            DisposeColumns(typeColumns, typeColumns?.Length ?? 0);
            throw;
        }
    }

    /// <inheritdoc/>
    // A decoded Variant column of the same alternatives, whose alternative columns the alternative codecs write.
    //
    // The test is the concrete VariantColumn, not the public IVariantColumn: the write trusts invariants only that
    // class's constructor establishes (every discriminator is either a valid alternative index or the NULL marker,
    // and LocalIndices is exactly as long as the column with a correct per-type running index). A caller's
    // implementation of the interface goes to the converter layer, which validates as it goes.
    public bool CanWrite(IColumn column)
    {
        if (column is not VariantColumn dense || !HasTheSameAlternatives(dense))
        {
            return false;
        }

        for (int i = 0; i < children.Length; i++)
        {
            if (!children[i].CanWrite(dense.GetTypeColumn(i)))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    // Each alternative's slice is the contiguous run of its values whose rows fall in [start, start + length), so
    // the prefix and body phases share one set of child slices and states. Every alternative gets a state even when
    // no row selects it: the alternative set is fixed by the type rather than by the data, so each one's prefix
    // belongs on the wire regardless of which rows arrived.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length)
        => BuildDenseState((VariantColumn)column, start, length);

    // Discriminator indices name the same alternatives only when the alternatives and their order are the same.
    private bool HasTheSameAlternatives(VariantColumn dense)
    {
        if (dense.TypeCount != children.Length)
        {
            return false;
        }

        IReadOnlyList<string> names = dense.TypeNames;
        for (int i = 0; i < children.Length; i++)
        {
            if (!string.Equals(names[i], children[i].TypeName, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    // A fixed mode word, then every alternative's own prefix over its child column, including the alternatives no
    // row selected.
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<VariantWriteState>(TypeName);
        writer.WriteUInt64(VariantWire.BasicDiscriminatorsMode);
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteStatePrefix(writer, own.ChildColumns[i], own.ChildStart[i], own.ChildLength[i], own.ChildStates[i]);
        }
    }

    /// <inheritdoc/>
    // The row-order discriminator stream, then each alternative's values in alternative order.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<VariantWriteState>(TypeName);
        writer.WriteBytes(((VariantColumn)column).Discriminators.Slice(start, length));
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteColumn(writer, own.ChildColumns[i], own.ChildStart[i], own.ChildLength[i], own.ChildStates[i]);
        }
    }

    // How many rows chose each alternative — the length of every run that follows the discriminators.
    private int[] CountRowsPerType(ReadOnlySpan<byte> discriminators, int rowCount, string columnName, string columnType)
    {
        var rowsPerType = new int[children.Length];
        for (int row = 0; row < rowCount; row++)
        {
            byte d = discriminators[row];

            // A NULL row takes a slot in no run.
            if (d == IVariantColumn.NullDiscriminator)
            {
                continue;
            }

            // Rejected here, and not left to VariantColumn: its constructor indexes per-type counters by
            // discriminator, so an out-of-range one would surface there as an IndexOutOfRangeException naming
            // neither the column nor the row.
            if (d >= children.Length)
            {
                throw new ClickHouseTcpProtocolException(
                    $"Variant column '{columnName}' ({columnType}) has discriminator {d} at row {row}, but the type declares only {children.Length} alternative(s).");
            }

            rowsPerType[d]++;
        }

        return rowsPerType;
    }

    // One dense run per alternative, in declared order. A run of zero rows is still read, so an alternative no row
    // selected still gets the call its codec may expect.
    private async ValueTask<IColumn[]> ReadTypeColumnsAsync(ClickHouseBinaryReader reader, string columnName, int[] rowsPerType, CancellationToken cancellationToken)
    {
        var typeColumns = new IColumn[children.Length];
        int read = 0;
        try
        {
            for (int i = 0; i < children.Length; i++)
            {
                typeColumns[i] = await children[i].ReadColumnAsync(reader, columnName, children[i].TypeName, rowsPerType[i], cancellationToken).ConfigureAwait(false);
                read = i + 1;
            }
        }
        catch
        {
            // No variant column owns these yet.
            DisposeColumns(typeColumns, read);
            throw;
        }

        return typeColumns;
    }

    // The dense path: the discriminators and per-type child columns already exist, so each alternative's slice is
    // the contiguous run of its values whose originating rows fall in [start, start + length) — found by counting
    // that type's discriminators before and within the slice, since values are stored in row order. The child
    // columns are borrowed from the dense column, so nothing is copied and the state owns none of them.
    private VariantWriteState BuildDenseState(VariantColumn dense, int start, int length)
    {
        int typeCount = children.Length;
        var childColumns = new IColumn[typeCount];

        // Each type's values sit contiguously in its child column in row order, so writing this slice needs, per
        // type, the count of its values before the slice (the child-column start offset) and within it (the length
        // to write). Both come from a single pass over the slice: the precomputed LocalIndices give each row its
        // index within its type's child column, so the first in-slice row of a given discriminator already carries
        // that type's before-slice count. This avoids rescanning [0, start) per slice, which would make a
        // multi-block insert quadratic. A length of 0 flags the first in-slice occurrence, and an absent type keeps
        // start/length 0 — an empty slice its codec still writes a prefix for.
        var childStart = new int[typeCount];
        var childLength = new int[typeCount];
        ReadOnlySpan<byte> discriminators = dense.Discriminators;
        ReadOnlySpan<int> localIndices = dense.LocalIndices;
        for (int i = start; i < start + length; i++)
        {
            byte d = discriminators[i];
            if (d == IVariantColumn.NullDiscriminator)
            {
                continue;
            }

            if (childLength[d] == 0)
            {
                childStart[d] = localIndices[i];
            }

            childLength[d]++;
        }

        for (int i = 0; i < typeCount; i++)
        {
            childColumns[i] = dense.GetTypeColumn(i);
        }

        return OpenChildStates(childColumns, childStart, childLength);
    }

    // Opens each alternative's own write state over its child column and assembles the slice's state. A later
    // alternative's BeginWrite throwing must not leak the states already opened.
    private VariantWriteState OpenChildStates(IColumn[] childColumns, int[] childStart, int[] childLength)
    {
        var childStates = new IColumnWriteState[children.Length];
        int opened = 0;
        try
        {
            for (int i = 0; i < children.Length; i++)
            {
                childStates[i] = children[i].BeginWrite(childColumns[i], childStart[i], childLength[i]);
                opened = i + 1;
            }
        }
        catch
        {
            for (int i = 0; i < opened; i++)
            {
                childStates[i]?.Dispose();
            }

            throw;
        }

        return new VariantWriteState
        {
            ChildColumns = childColumns,
            ChildStart = childStart,
            ChildLength = childLength,
            ChildStates = childStates,
        };
    }

    // The write scratch of one slice, shared across the prefix and body phases: one child column per alternative,
    // borrowed from the decoded column, with the slice of it that alternative occupies and its own codec's state. An
    // alternative no row selected carries an empty slice rather than being absent, so its prefix is still written.
    private sealed class VariantWriteState : IColumnWriteState
    {
        public IColumn[] ChildColumns;
        public int[] ChildStart;
        public int[] ChildLength;
        public IColumnWriteState[] ChildStates;

        public void Dispose()
        {
            if (ChildStates is not null)
            {
                foreach (IColumnWriteState state in ChildStates)
                {
                    state?.Dispose();
                }
            }
        }
    }

    // Disposes the first count entries of a partially read run array.
    private static void DisposeColumns(IColumn[] columns, int count)
    {
        for (int i = 0; i < count; i++)
        {
            columns[i]?.Dispose();
        }
    }
}
