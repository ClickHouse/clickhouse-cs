using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Dynamic</c> column — a column whose per-row value type is discovered at runtime.
/// Unlike <c>Variant</c>, the set of types is not in the type string; it is carried on the wire, in the column's
/// state prefix, and recomputed per block from the data. Only the flattened serialization (version 3) is read or
/// written; it is selected by the query setting
/// <c>output_format_native_use_flattened_dynamic_and_json_serialization = 1</c>, and any other version is
/// rejected as a protocol error.
///
/// <para>
/// The wire layout per non-empty block is: a <c>UInt64</c> version (3), a <c>VarUInt</c> type count, that many
/// type-name strings, each runtime type's own state prefix (empty for leaf types), one discriminator per row
/// (whose width grows with the type count; NULL is the discriminator value equal to the type count), then one
/// dense run per type in wire order holding the values of the rows that selected it. The version and type list
/// are the <em>state prefix</em>; the discriminators and runs are the <em>body</em> — so under an element-
/// flattening composite (<c>Array(Dynamic)</c>) the type list precedes the composite's own framing.
/// </para>
/// </summary>
internal sealed class DynamicColumnCodec : IColumnCodec
{
    /// <summary>
    /// The serialization version that heads a <c>Dynamic</c> column's state prefix in the flattened layout — the
    /// only layout this client reads or writes. It is selected by the query setting
    /// <c>output_format_native_use_flattened_dynamic_and_json_serialization = 1</c>; the non-flat versions
    /// (1, 2, 4) carry an internal/on-disk representation this client rejects.
    /// </summary>
    internal const ulong FlattenedVersion = 3;

    /// <summary>
    /// A defensive ceiling on the runtime type count read from the wire, so a corrupt length prefix cannot drive
    /// an unbounded allocation. Far larger than any real <c>Dynamic</c> type set.
    /// </summary>
    private const int MaxTypes = 1_000_000;

    private readonly ColumnCodecRegistry registry;
    private readonly ResolveContext context;

    // The runtime type list read by ReadStatePrefixAsync and consumed by the immediately following
    // ReadColumnAsync. A codec instance serves one column of one block (the registry builds a fresh one per
    // resolve), and reads on a connection are sequential, so carrying this between the two phases is safe.
    private string[] prefixTypeNames;
    private IColumnCodec[] prefixChildren;

    private DynamicColumnCodec(string typeName, ColumnCodecRegistry registry, in ResolveContext context)
    {
        TypeName = typeName;
        this.registry = registry;
        this.context = context;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(object);

    /// <summary>Builds a <c>Dynamic</c> codec.</summary>
    /// <param name="node">The parsed <c>Dynamic</c> node; an optional <c>max_types=N</c> argument bounds the server's tracked type set but does not affect the wire.</param>
    /// <param name="context">The resolution context, captured so runtime child types (e.g. a timezone-bearing <c>DateTime</c>) resolve consistently.</param>
    /// <param name="registry">The registry used to resolve runtime child codecs lazily from the wire/inferred type names.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The node carries an argument that is not the <c>max_types=N</c> form.</exception>
    public static DynamicColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        // max_types only bounds the server's tracked type set; it does not change the flattened wire layout, so it
        // is validated for a clear error but otherwise carried only in the type name (node.ToString()).
        foreach (TypeNode argument in node.Arguments)
        {
            if (!TryParseMaxTypes(argument.Name, out _))
            {
                throw new FormatException(
                    $"Dynamic type '{node}' has unsupported argument '{argument.Name}'; only 'max_types=N' is recognized.");
            }
        }

        return new DynamicColumnCodec(node.ToString(), registry, in context);
    }

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        ulong version = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
        if (version != FlattenedVersion)
        {
            throw new ClickHouseTcpProtocolException(
                $"Dynamic column '{TypeName}' uses serialization version {version}; this client supports only the flattened version {FlattenedVersion}. " +
                "Enable it with the query setting output_format_native_use_flattened_dynamic_and_json_serialization=1.");
        }

        ulong rawTypeCount = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        if (rawTypeCount > MaxTypes)
        {
            throw new ClickHouseTcpProtocolException(
                $"Dynamic column '{TypeName}' declares {rawTypeCount} runtime types, exceeding the supported maximum of {MaxTypes} (corrupt stream).");
        }

        int typeCount = (int)rawTypeCount;
        var names = new string[typeCount];
        var children = new IColumnCodec[typeCount];
        for (int i = 0; i < typeCount; i++)
        {
            names[i] = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);

            // The runtime type names come off the wire, so one this client cannot resolve is a disagreement
            // with the server, not a caller error.
            try
            {
                children[i] = registry.Resolve(names[i], in context);
            }
            catch (Exception e) when (e is FormatException or NotSupportedException)
            {
                throw new ClickHouseTcpProtocolException(
                    $"Dynamic column '{TypeName}' carries runtime type '{names[i]}', which this client cannot read: {e.Message}", e);
            }
        }

        // Each runtime type contributes its own state prefix after the type-name list; a leaf type's is empty, a
        // stateful runtime type (e.g. LowCardinality) reads its version marker here.
        for (int i = 0; i < typeCount; i++)
        {
            await children[i].ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
        }

        prefixTypeNames = names;
        prefixChildren = children;
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            // A zero-row Dynamic has no discriminators or runtime-type runs.
            return new DynamicColumn(columnName, columnType, Array.Empty<string>(), Array.Empty<int>(), Array.Empty<IColumn>(), 0, pooledDiscriminators: false, ownsColumns: true);
        }

        string[] names = prefixTypeNames;
        IColumnCodec[] children = prefixChildren;
        prefixTypeNames = null;
        prefixChildren = null;
        if (names is null || children is null)
        {
            throw new ClickHouseTcpProtocolException(
                $"Dynamic column '{columnName}' ({columnType}) has {rowCount} row(s) but its state prefix was not read.");
        }

        int typeCount = children.Length;
        int width = DiscriminatorWidth(typeCount);
        int[] discriminators = ArrayPool<int>.Shared.Rent(rowCount);
        var typeColumns = new IColumn[typeCount];
        int read = 0;
        try
        {
            await ReadDiscriminatorsAsync(reader, discriminators.AsMemory(0, rowCount), width, cancellationToken).ConfigureAwait(false);

            var counts = new int[typeCount];
            for (int row = 0; row < rowCount; row++)
            {
                int d = discriminators[row];
                if (d == typeCount)
                {
                    continue; // NULL: consumes no value from any run.
                }

                if ((uint)d > (uint)typeCount)
                {
                    throw new ClickHouseTcpProtocolException(
                        $"Dynamic column '{columnName}' ({columnType}) has discriminator {d} at row {row}, but the block declares only {typeCount} runtime type(s).");
                }

                counts[d]++;
            }

            for (int i = 0; i < typeCount; i++)
            {
                typeColumns[i] = await children[i].ReadColumnAsync(reader, columnName, names[i], counts[i], cancellationToken).ConfigureAwait(false);
                read = i + 1;
            }

            return new DynamicColumn(columnName, columnType, names, discriminators, typeColumns, rowCount, pooledDiscriminators: true, ownsColumns: true);
        }
        catch
        {
            ArrayPool<int>.Shared.Return(discriminators);
            for (int i = 0; i < read; i++)
            {
                typeColumns[i].Dispose();
            }

            throw;
        }
    }

    /// <inheritdoc/>
    // A decoded Dynamic column whose type columns the codecs of its types write.
    //
    // The test is the concrete DynamicColumn, not the public IDynamicColumn: the write trusts invariants only that
    // class's constructor establishes (the type-name list matches the child-column count, and every discriminator
    // is a valid type index or the NULL marker). A caller's column goes to the converter layer, which infers and
    // validates per value.
    public bool CanWrite(IColumn column)
    {
        if (column is not DynamicColumn dense)
        {
            return false;
        }

        for (int i = 0; i < dense.TypeCount; i++)
        {
            if (!registry.Resolve(dense.TypeNames[i], in context).CanWrite(dense.GetTypeColumn(i)))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildDenseState((DynamicColumn)column, start, length);

    /// <inheritdoc/>
    // The version, the runtime type list, then each type's own prefix (empty for a leaf type).
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<DynamicWriteState>(TypeName);
        writer.WriteUInt64(FlattenedVersion);
        writer.WriteVarUInt((ulong)own.TypeNames.Length);
        foreach (string name in own.TypeNames)
        {
            writer.WriteString(name);
        }

        for (int i = 0; i < own.Children.Length; i++)
        {
            own.Children[i].WriteStatePrefix(writer, own.ChildColumns[i], own.ChildStart[i], own.ChildLength[i], own.ChildStates[i]);
        }
    }

    /// <inheritdoc/>
    // The discriminators, then each type's dense run in wire order.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<DynamicWriteState>(TypeName);
        WriteDiscriminators(writer, ((DynamicColumn)column).Discriminators.Slice(start, length), own.Width);
        for (int i = 0; i < own.Children.Length; i++)
        {
            own.Children[i].WriteColumn(writer, own.ChildColumns[i], own.ChildStart[i], own.ChildLength[i], own.ChildStates[i]);
        }
    }

    /// <summary>
    /// The discriminator width for <paramref name="typeCount"/> runtime types: the smallest unsigned integer that
    /// indexes the types plus the NULL slot (NULL is the value <paramref name="typeCount"/>). One byte up to 255
    /// types, then 2, then 4 — a count is capped at <see cref="MaxTypes"/> and held in an <see cref="int"/>, so it
    /// never needs the wire format's 8-byte width.
    /// </summary>
    /// <param name="typeCount">The number of runtime types.</param>
    /// <returns>The discriminator width in bytes (1, 2, or 4).</returns>
    internal static int DiscriminatorWidth(int typeCount)
        => typeCount <= byte.MaxValue ? 1
            : typeCount <= ushort.MaxValue ? 2
            : 4;

    /// <summary>Writes one discriminator for each row, in <paramref name="width"/> bytes each (<see cref="DiscriminatorWidth"/>).</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="discriminators">The discriminators.</param>
    /// <param name="width">The width of each discriminator in bytes: 1, 2 or 4.</param>
    internal static void WriteDiscriminators(ClickHouseBinaryWriter writer, ReadOnlySpan<int> discriminators, int width)
    {
        switch (width)
        {
            case 1:
                foreach (int d in discriminators)
                {
                    writer.WriteByte((byte)d);
                }

                break;
            case 2:
                foreach (int d in discriminators)
                {
                    writer.WriteUInt16((ushort)d);
                }

                break;
            default:
                foreach (int d in discriminators)
                {
                    writer.WriteUInt32((uint)d);
                }

                break;
        }
    }

    // The type list, discriminators, and per-type child columns already exist. Per type, find its child-column run
    // within the slice: the count before and within it, from the precomputed local indices, as the Variant codec
    // slices its alternatives.
    private DynamicWriteState BuildDenseState(DynamicColumn dense, int start, int length)
    {
        int typeCount = dense.TypeCount;
        var names = new string[typeCount];
        var children = new IColumnCodec[typeCount];
        for (int i = 0; i < typeCount; i++)
        {
            names[i] = dense.TypeNames[i];
            children[i] = registry.Resolve(names[i], in context);
        }

        // Child values are in row order, so a type's in-slice rows make one run. The local index of the first
        // in-slice row of a type is that type's count before the slice — where its run starts — and the loop
        // counts the run length. A NULL row fills no child slot.
        ReadOnlySpan<int> discriminators = dense.Discriminators;
        ReadOnlySpan<int> localIndices = dense.LocalIndices;
        var before = new int[typeCount];
        var within = new int[typeCount];
        for (int i = start; i < start + length; i++)
        {
            int d = discriminators[i];
            if (d == typeCount)
            {
                continue; // NULL
            }

            if (within[d] == 0)
            {
                before[d] = localIndices[i];
            }

            within[d]++;
        }

        var childColumns = new IColumn[typeCount];
        var childStates = new IColumnWriteState[typeCount];
        int statesBuilt = 0;
        try
        {
            for (int i = 0; i < typeCount; i++)
            {
                childColumns[i] = dense.GetTypeColumn(i);
                childStates[i] = children[i].BeginWrite(childColumns[i], before[i], within[i]);
                statesBuilt = i + 1;
            }
        }
        catch
        {
            // A child BeginWrite throwing mid-loop would otherwise leak the states already built (each may hold its
            // own rented buffers).
            for (int i = 0; i < statesBuilt; i++)
            {
                childStates[i]?.Dispose();
            }

            throw;
        }

        return new DynamicWriteState
        {
            TypeNames = names,
            Children = children,
            ChildColumns = childColumns,
            ChildStart = before,
            ChildLength = within,
            ChildStates = childStates,
            Width = DiscriminatorWidth(typeCount),
        };
    }

    // Reads rowCount discriminators of the given width into dest, widening each to int. Width 1 (the common case,
    // up to 255 runtime types) bulk-reads the bytes; the wider widths read per row.
    private static async ValueTask ReadDiscriminatorsAsync(ClickHouseBinaryReader reader, Memory<int> dest, int width, CancellationToken cancellationToken)
    {
        int rowCount = dest.Length;
        if (width == 1)
        {
            byte[] raw = ArrayPool<byte>.Shared.Rent(rowCount);
            try
            {
                await reader.ReadBytesAsync(raw.AsMemory(0, rowCount), cancellationToken).ConfigureAwait(false);
                Span<int> destination = dest.Span;
                for (int i = 0; i < rowCount; i++)
                {
                    destination[i] = raw[i];
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(raw);
            }

            return;
        }

        if (width == 2)
        {
            for (int i = 0; i < rowCount; i++)
            {
                ushort value = await reader.ReadUInt16Async(cancellationToken).ConfigureAwait(false);
                dest.Span[i] = value;
            }

            return;
        }

        for (int i = 0; i < rowCount; i++)
        {
            uint value = await reader.ReadUInt32Async(cancellationToken).ConfigureAwait(false);
            dest.Span[i] = checked((int)value);
        }
    }

    // Parses a max_types=N argument. Returns true (with the parsed N) for that form and false for anything else.
    private static bool TryParseMaxTypes(string argument, out int maxTypes)
    {
        maxTypes = 0;
        int equals = argument.IndexOf('=');
        if (equals < 0)
        {
            return false;
        }

        return string.Equals(argument.Substring(0, equals).Trim(), "max_types", StringComparison.Ordinal)
            && int.TryParse(argument.AsSpan(equals + 1).Trim(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out maxTypes);
    }

    // The write plan for one slice, computed once by BeginWrite and shared across the prefix and body phases: the
    // runtime type list, the discriminator width, and each type's child column plus the slice within it.
    private sealed class DynamicWriteState : IColumnWriteState
    {
        public string[] TypeNames;
        public IColumnCodec[] Children;
        public IColumn[] ChildColumns;
        public int[] ChildStart;
        public int[] ChildLength;
        public IColumnWriteState[] ChildStates;
        public int Width;

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
}
