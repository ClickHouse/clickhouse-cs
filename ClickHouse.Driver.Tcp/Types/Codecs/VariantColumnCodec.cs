using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection;
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
/// On the write path a dense <see cref="VariantColumn"/> is serialized straight from its discriminator stream and
/// per-type child columns with no copy. A flat <c>IColumn&lt;object&gt;</c> — a caller's column or what an
/// <c>Array(Variant(...))</c> flattens into — is scattered by each value's runtime CLR type into per-type
/// buffers, which boxes; this is the ergonomic, not the hot, path.
/// </para>
/// </summary>
internal sealed class VariantColumnCodec : IColumnCodec
{
    // The discriminator is a single byte and 255 marks NULL, so at most 255 alternatives (indices 0..254) can be
    // addressed under the BASIC layout.
    private const int MaxTypes = 255;

    private readonly IColumnCodec[] children;
    private readonly Func<string, string, object[], int, IColumn>[] childFlatBuilders;
    private readonly Dictionary<Type, int> discriminatorByClrType;
    private readonly bool allChildrenWritable;

    private VariantColumnCodec(string typeName, IColumnCodec[] children)
    {
        TypeName = typeName;
        this.children = children;

        int typeCount = children.Length;
        childFlatBuilders = new Func<string, string, object[], int, IColumn>[typeCount];
        MethodInfo builderTemplate = typeof(VariantColumnCodec).GetMethod(nameof(BuildFlatColumn), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method '{nameof(BuildFlatColumn)}' was not found.");

        // Map each alternative's canonical element type to its discriminator, so the ergonomic write path can
        // pick a row's alternative from the runtime type of its value. Only the canonical element type is keyed
        // here — not a codec's extra convenience write types (e.g. DateTime alongside DateTimeOffset): the
        // per-alternative bucket is materialized as that exact element type (BuildFlatColumn<ElementType>), so a
        // convenience-typed value would fail the bucket cast. Rejecting it up front with a clear "no alternative"
        // error beats a deep InvalidCastException. If two alternatives claimed the same CLR type the lower
        // discriminator would win (the server forbids duplicate alternative types, so this is only a tie-break).
        discriminatorByClrType = new Dictionary<Type, int>();
        bool writable = true;
        for (int i = 0; i < typeCount; i++)
        {
            childFlatBuilders[i] = (Func<string, string, object[], int, IColumn>)builderTemplate
                .MakeGenericMethod(children[i].ElementType)
                .CreateDelegate(typeof(Func<string, string, object[], int, IColumn>));

            discriminatorByClrType.TryAdd(children[i].ElementType, i);

            // Probe writability with an empty child column so a Variant over a non-writable alternative (e.g.
            // Nothing) is rejected up front rather than mid-write.
            IColumn probe = childFlatBuilders[i](string.Empty, children[i].TypeName, Array.Empty<object>(), 0);
            writable &= children[i].CanWrite(probe);
        }

        allChildrenWritable = writable;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(object);

    // A Variant is never nested inside Nullable (the server rejects Nullable(Variant(...))), so this placeholder
    // is a formality the interface requires and is never written.
    /// <inheritdoc/>
    public object NullPlaceholder => null;

    /// <summary>Builds a <c>Variant(...)</c> codec, resolving each alternative's codec through the registry.</summary>
    /// <param name="node">The parsed <c>Variant</c> node; its arguments are the alternative types in discriminator order.</param>
    /// <param name="context">The resolution context, forwarded to each alternative codec's factory.</param>
    /// <param name="registry">The registry used to resolve the alternative codecs.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The variant has no alternatives, or an alternative is <c>Nullable</c>.</exception>
    /// <exception cref="NotSupportedException">The variant has more alternatives than the BASIC layout can address.</exception>
    public static VariantColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
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

            // The server rejects Dynamic inside Variant (Dynamic is a superset of Variant). Reject it client-side
            // too: a Variant does not thread the per-operation write state a data-dependent alternative needs, so a
            // Dynamic alternative would desynchronize its type-list prefix from its body.
            if (string.Equals(argument.Name, "Dynamic", StringComparison.Ordinal))
            {
                throw new FormatException(
                    $"Variant alternative '{argument}' must not be Dynamic; the server does not allow a Dynamic type inside a Variant.");
            }

            childCodecs[i] = registry.ResolveNode(argument, in context);
        }

        return new VariantColumnCodec(node.ToString(), childCodecs);
    }

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        ulong mode = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
        if (mode != VariantWire.BasicDiscriminatorsMode)
        {
            throw new NotSupportedException(
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
    // A dense variant column is only writable when its alternatives match this codec's; a bare IColumn<object> is
    // scattered by runtime CLR type. A variant column of a different arity is rejected here rather than silently
    // re-scattered (which could reorder its discriminators).
    //
    // The dense test is the concrete VariantColumn, not the public IVariantColumn: the dense writer trusts
    // invariants only that class's constructor establishes (every discriminator is either a valid alternative index
    // or the NULL marker, and LocalIndices is exactly as long as the column with a correct per-type running index).
    // Matching on the public interface would let a caller-supplied implementation reach the writer with none of
    // that checked, and the first thing the writer does is put the discriminators on the wire — so a bad value
    // would desync the block mid-stream rather than fail cleanly. Anything else writable arrives as IColumn<object>
    // and goes down the scattered path, which validates as it goes.
    public bool CanWrite(IColumn column)
        => allChildrenWritable && (column is VariantColumn dense ? dense.TypeCount == children.Length : column is IColumn<object>);

    /// <inheritdoc/>
    // Project the slice into one column per alternative once, and open each alternative's own write state over it,
    // so the prefix and body phases share a single projection and a child never sees the variant's own column.
    // Every alternative gets a column and a state even when no row selects it: the alternative set is fixed by the
    // type rather than by the data, so each one's prefix belongs on the wire regardless of which rows arrived.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length)
        => column is VariantColumn dense && dense.TypeCount == children.Length
            ? BuildDenseState(dense, start, length)
            : BuildScatteredState(column, start, length);

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using VariantWriteState state = BeginWriteCore(column, start, length);
        WriteStatePrefixCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        WriteStatePrefixCore(writer, state.Expect<VariantWriteState>(TypeName));
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using VariantWriteState state = BeginWriteCore(column, start, length);
        WriteBodyCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        WriteBodyCore(writer, state.Expect<VariantWriteState>(TypeName));
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
                throw new FormatException(
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

    // A fixed mode word, then every alternative's own prefix over its projected column — including the alternatives
    // no row selected, whose column is simply empty.
    private void WriteStatePrefixCore(ClickHouseBinaryWriter writer, VariantWriteState state)
    {
        writer.WriteUInt64(VariantWire.BasicDiscriminatorsMode);
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteStatePrefix(writer, state.ChildColumns[i], state.ChildStart[i], state.ChildLength[i], state.ChildStates[i]);
        }
    }

    // The row-order discriminator stream, then each alternative's values in alternative order.
    private void WriteBodyCore(ClickHouseBinaryWriter writer, VariantWriteState state)
    {
        writer.WriteBytes(state.Discriminators is not null
            ? state.Discriminators.AsSpan(0, state.Length)
            : state.Dense.Discriminators.Slice(state.Start, state.Length));

        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteColumn(writer, state.ChildColumns[i], state.ChildStart[i], state.ChildLength[i], state.ChildStates[i]);
        }
    }

    private VariantWriteState BeginWriteCore(IColumn column, int start, int length)
        => (VariantWriteState)BeginWrite(column, start, length);

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

        return OpenChildStates(childColumns, childStart, childLength, dense, start, length, discriminators: null);
    }

    // The ergonomic path: scatter a flat column of boxed values into per-type buckets by each value's runtime CLR
    // type, building the discriminator stream as it goes, then project each bucket into its alternative's own typed
    // column. Mirrors the tuple codec's flat write, bucketing by discriminator instead of distributing across
    // parallel columns. The buckets are pooled scratch for the projection only, so they go back before returning;
    // the discriminator buffer outlives this call because the body phase writes it, so the state owns it.
    private VariantWriteState BuildScatteredState(IColumn column, int start, int length)
    {
        int typeCount = children.Length;
        byte[] discriminators = ArrayPool<byte>.Shared.Rent(length);
        var buckets = new object[typeCount][];
        var filled = new int[typeCount];
        for (int i = 0; i < typeCount; i++)
        {
            buckets[i] = ArrayPool<object>.Shared.Rent(length);
        }

        try
        {
            for (int row = 0; row < length; row++)
            {
                object value = column.GetValue(start + row);
                if (value is null)
                {
                    discriminators[row] = IVariantColumn.NullDiscriminator;
                    continue;
                }

                int discriminator = DiscriminatorFor(value.GetType());
                discriminators[row] = (byte)discriminator;
                buckets[discriminator][filled[discriminator]++] = value;
            }

            var childColumns = new IColumn[typeCount];
            for (int i = 0; i < typeCount; i++)
            {
                childColumns[i] = childFlatBuilders[i](column.Name, children[i].TypeName, buckets[i], filled[i]);
            }

            // Each projected column starts at 0 and runs for the rows that selected that alternative.
            return OpenChildStates(childColumns, new int[typeCount], filled, dense: null, start, length, discriminators);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(discriminators);
            throw;
        }
        finally
        {
            for (int i = 0; i < typeCount; i++)
            {
                ArrayPool<object>.Shared.Return(buckets[i], clearArray: true);
            }
        }
    }

    // Opens each alternative's own write state over its projected column and assembles the slice's state. A later
    // alternative's BeginWrite throwing must not leak the states already opened, nor the rented discriminators.
    private VariantWriteState OpenChildStates(
        IColumn[] childColumns,
        int[] childStart,
        int[] childLength,
        VariantColumn dense,
        int start,
        int length,
        byte[] discriminators)
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

            if (discriminators is not null)
            {
                ArrayPool<byte>.Shared.Return(discriminators);
            }

            throw;
        }

        return new VariantWriteState
        {
            ChildColumns = childColumns,
            ChildStart = childStart,
            ChildLength = childLength,
            ChildStates = childStates,
            Dense = dense,
            Start = start,
            Length = length,
            Discriminators = discriminators,
        };
    }

    // Resolves the discriminator for a value's runtime CLR type, or throws if no alternative accepts it.
    private int DiscriminatorFor(Type clrType)
    {
        if (discriminatorByClrType.TryGetValue(clrType, out int discriminator))
        {
            return discriminator;
        }

        throw new ArgumentException(
            $"Variant '{TypeName}' has no alternative for a value of CLR type '{clrType}'. Supported CLR types: {string.Join(", ", discriminatorByClrType.Keys)}.");
    }

    // The write scratch of one slice, shared across the prefix and body phases. ChildColumns holds one column per
    // alternative — borrowed from the dense column, or projected out of the boxed values — with the slice of it that
    // alternative occupies and its own codec's state; an alternative no row selected carries an empty slice rather
    // than being absent, so its prefix is still written. Discriminators is the scattered path's own row-order stream,
    // rented and returned on dispose; for a dense column it is null and Dense supplies the stream instead.
    private sealed class VariantWriteState : IColumnWriteState
    {
        public IColumn[] ChildColumns;
        public int[] ChildStart;
        public int[] ChildLength;
        public IColumnWriteState[] ChildStates;
        public VariantColumn Dense;
        public int Start;
        public int Length;
        public byte[] Discriminators;

        public void Dispose()
        {
            if (ChildStates is not null)
            {
                foreach (IColumnWriteState state in ChildStates)
                {
                    state?.Dispose();
                }
            }

            if (Discriminators is not null)
            {
                ArrayPool<byte>.Shared.Return(Discriminators);
                Discriminators = null;
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

    // Builds a flat typed column from boxed values — the ergonomic write path's per-type projection.
    private static IColumn BuildFlatColumn<T>(string name, string typeName, object[] boxed, int count)
    {
        var values = new T[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (T)boxed[i];
        }

        return new ArrayColumn<T>(name, typeName, values);
    }
}
