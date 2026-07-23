using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
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
    // Builds a flat typed column from boxed values, one cached delegate per element type — the ergonomic write
    // path's per-runtime-type projection, mirroring the variant codec's.
    private static readonly ConcurrentDictionary<Type, Func<string, string, object[], int, IColumn>> FlatBuilders = new();

    private readonly ColumnCodecRegistry registry;
    private readonly ResolveContext context;

    // The runtime type list read by ReadStatePrefixAsync and consumed by the immediately following
    // ReadColumnAsync. A codec instance serves one column of one block (the registry builds a fresh one per
    // resolve), and reads on a connection are sequential, so carrying this between the two phases is safe.
    private string[] prefixTypeNames;
    private IColumnCodec[] prefixChildren;

    // Codecs resolved lazily for MeasureRowBytes (which has no shared write state), cached by type name so a
    // per-row measure does not re-resolve the same runtime type.
    private Dictionary<string, IColumnCodec> measureCodecs;

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

    // A Dynamic is never nested inside Nullable (the server rejects Nullable(Dynamic); NULL rides the
    // discriminator), so this placeholder is a formality the interface requires and is never written.
    /// <inheritdoc/>
    public object NullPlaceholder => null;

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
        if (version != DynamicWire.FlattenedVersion)
        {
            throw new ClickHouseProtocolException(
                $"Dynamic column '{TypeName}' uses serialization version {version}; this client supports only the flattened version {DynamicWire.FlattenedVersion}. " +
                "Enable it with the query setting output_format_native_use_flattened_dynamic_and_json_serialization=1.");
        }

        ulong rawTypeCount = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        if (rawTypeCount > DynamicWire.MaxTypes)
        {
            throw new ClickHouseProtocolException(
                $"Dynamic column '{TypeName}' declares {rawTypeCount} runtime types, exceeding the supported maximum of {DynamicWire.MaxTypes} (corrupt stream).");
        }

        int typeCount = (int)rawTypeCount;
        var names = new string[typeCount];
        var children = new IColumnCodec[typeCount];
        for (int i = 0; i < typeCount; i++)
        {
            names[i] = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
            children[i] = registry.Resolve(names[i], in context);
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
            // A zero-row block carries neither the version/type-list prefix (the block layer skips the prefix for
            // zero rows) nor any body; surface an empty column with no runtime types.
            return new DynamicColumn(columnName, columnType, Array.Empty<string>(), Array.Empty<int>(), Array.Empty<IColumn>(), 0, pooledDiscriminators: false, ownsColumns: true);
        }

        string[] names = prefixTypeNames;
        IColumnCodec[] children = prefixChildren;
        prefixTypeNames = null;
        prefixChildren = null;
        if (names is null || children is null)
        {
            throw new ClickHouseProtocolException(
                $"Dynamic column '{columnName}' ({columnType}) has {rowCount} row(s) but its state prefix was not read.");
        }

        int typeCount = children.Length;
        int width = DynamicWire.DiscriminatorWidth(typeCount);
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
                    throw new FormatException(
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
    // The dense DynamicColumn is the zero-copy source; a flat IColumn<object> is scattered by each value's
    // inferred type. A value whose CLR type has no inferred ClickHouse type throws at write time (inference
    // cannot be pre-validated here, since the type set is data-derived).
    public bool CanWrite(IColumn column) => column is IDynamicColumn or IColumn<object>;

    /// <inheritdoc/>
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildState(column, start, length);

    /// <inheritdoc/>
    // The state-free path recomputes the write plan for the prefix; the type list it derives is a deterministic
    // function of the data, so it matches the body's plan. The block layer avoids the recompute by threading the
    // BeginWrite state through the state-aware overloads below.
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using DynamicWriteState state = BuildState(column, start, length);
        WriteStatePrefixCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is DynamicWriteState dynamicState)
        {
            WriteStatePrefixCore(writer, dynamicState);
            return;
        }

        WriteStatePrefix(writer, column, start, length);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using DynamicWriteState state = BuildState(column, start, length);
        WriteBodyCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is DynamicWriteState dynamicState)
        {
            WriteBodyCore(writer, dynamicState);
            return;
        }

        WriteColumn(writer, column, start, length);
    }

    /// <inheritdoc/>
    public long MeasureRowBytes(IColumn column, int row)
    {
        if (column is IDynamicColumn dense)
        {
            int width = DynamicWire.DiscriminatorWidth(dense.TypeCount);
            int d = dense.Discriminators[row];
            if (d == dense.TypeCount)
            {
                return width;
            }

            IColumnCodec child = ResolveForMeasure(dense.TypeNames[d]);
            return width + child.MeasureRowBytes(dense.GetTypeColumn(d), dense.LocalIndices[row]);
        }

        object value = column.GetValue(row);
        if (value is null)
        {
            // Approximate the discriminator as one byte; the block's type count (hence the true width) is not
            // known per row, and a small mis-price only shifts where blocks split, never correctness.
            return 1;
        }

        (string typeName, object canonical) = DynamicTypeInference.Infer(value);
        IColumnCodec valueCodec = ResolveForMeasure(typeName);
        IColumn probe = FlatBuilderFor(valueCodec.ElementType)(column.Name, valueCodec.TypeName, new[] { canonical }, 1);

        // Densify the one-row probe before measuring it: the inferred value's codec measures only the dense wire
        // shape, and a composite value (an array, a tuple) arrives here in its ergonomic form. The probe is a
        // throwaway measured in place, so the freshly built column (if any) is not retained.
        return 1 + valueCodec.MeasureRowBytes(valueCodec.TryDensify(probe, out _), 0);
    }

    // Writes the state prefix from a computed write plan: version, the runtime type list, then each type's own
    // prefix (empty for a leaf type).
    private static void WriteStatePrefixCore(ClickHouseBinaryWriter writer, DynamicWriteState state)
    {
        writer.WriteUInt64(DynamicWire.FlattenedVersion);
        writer.WriteVarUInt((ulong)state.TypeNames.Length);
        foreach (string name in state.TypeNames)
        {
            writer.WriteString(name);
        }

        for (int i = 0; i < state.Children.Length; i++)
        {
            state.Children[i].WriteStatePrefix(writer, state.ChildColumns[i], state.ChildStart[i], state.ChildLength[i], state.ChildStates[i]);
        }
    }

    // Writes the body from a computed write plan: the discriminators, then each type's dense run in wire order.
    private static void WriteBodyCore(ClickHouseBinaryWriter writer, DynamicWriteState state)
    {
        WriteDiscriminators(writer, state.Discriminators.AsSpan(0, state.Length), state.Width);
        for (int i = 0; i < state.Children.Length; i++)
        {
            state.Children[i].WriteColumn(writer, state.ChildColumns[i], state.ChildStart[i], state.ChildLength[i], state.ChildStates[i]);
        }
    }

    private static void WriteDiscriminators(ClickHouseBinaryWriter writer, ReadOnlySpan<int> discriminators, int width)
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

    // Computes the shared write plan for rows [start, start + length): the runtime type list, the per-row
    // discriminators, and the per-type child column each type's run is written from.
    private DynamicWriteState BuildState(IColumn column, int start, int length)
        => column is IDynamicColumn dense ? BuildDenseState(dense, start, length) : BuildScatteredState(column, start, length);

    // The dense path: the type list, discriminators, and per-type child columns already exist. Copy the slice's
    // discriminators and, per type, find its child-column run within the slice (the count before and within it,
    // from the precomputed local indices — the same slicing the variant codec does).
    private DynamicWriteState BuildDenseState(IDynamicColumn dense, int start, int length)
    {
        int typeCount = dense.TypeCount;
        var names = new string[typeCount];
        var children = new IColumnCodec[typeCount];
        for (int i = 0; i < typeCount; i++)
        {
            names[i] = dense.TypeNames[i];
            children[i] = registry.Resolve(names[i], in context);
        }

        ReadOnlySpan<int> discriminators = dense.Discriminators;
        int[] slice = ArrayPool<int>.Shared.Rent(length);
        for (int i = 0; i < length; i++)
        {
            slice[i] = discriminators[start + i];
        }

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
            // A child BeginWrite throwing mid-loop would otherwise leak the rented slice and the states already
            // built (each may hold its own rented buffers).
            ArrayPool<int>.Shared.Return(slice);
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
            Discriminators = slice,
            Length = length,
            Width = DynamicWire.DiscriminatorWidth(typeCount),
        };
    }

    // The ergonomic path: infer each value's type, build the deterministic (name-sorted) type list, then scatter
    // the values into per-type buckets and project each into a typed child column.
    private DynamicWriteState BuildScatteredState(IColumn column, int start, int length)
    {
        string[] rowTypes = ArrayPool<string>.Shared.Rent(length);

        // The value coerced to its inferred codec's element type (e.g. a DateTimeOffset becomes a
        // ClickHouseDateTime64), so the per-type bucket holds what that codec writes.
        object[] rowValues = ArrayPool<object>.Shared.Rent(length);
        try
        {
            var distinct = new SortedSet<string>(StringComparer.Ordinal);
            for (int row = 0; row < length; row++)
            {
                object value = column.GetValue(start + row);
                if (value is null)
                {
                    rowTypes[row] = null;
                    continue;
                }

                (string typeName, object canonical) = DynamicTypeInference.Infer(value);
                rowTypes[row] = typeName;
                rowValues[row] = canonical;
                distinct.Add(typeName);
            }

            int typeCount = distinct.Count;
            var names = new string[typeCount];
            distinct.CopyTo(names);
            var typeIndex = new Dictionary<string, int>(typeCount, StringComparer.Ordinal);
            var children = new IColumnCodec[typeCount];
            for (int i = 0; i < typeCount; i++)
            {
                typeIndex[names[i]] = i;
                children[i] = registry.Resolve(names[i], in context);
            }

            var counts = new int[typeCount];
            for (int row = 0; row < length; row++)
            {
                if (rowTypes[row] is string t)
                {
                    counts[typeIndex[t]]++;
                }
            }

            int[] discriminators = ArrayPool<int>.Shared.Rent(length);
            var buckets = new object[typeCount][];
            var filled = new int[typeCount];
            for (int i = 0; i < typeCount; i++)
            {
                buckets[i] = ArrayPool<object>.Shared.Rent(counts[i]);
            }

            // Declared before the try so the catch can dispose the child states already built (each may hold its
            // own rented buffers) and return the discriminators buffer if a child BeginWrite throws mid-loop.
            var childStates = new IColumnWriteState[typeCount];
            int statesBuilt = 0;
            try
            {
                for (int row = 0; row < length; row++)
                {
                    string t = rowTypes[row];
                    if (t is null)
                    {
                        discriminators[row] = typeCount; // NULL
                        continue;
                    }

                    int d = typeIndex[t];
                    discriminators[row] = d;
                    buckets[d][filled[d]++] = rowValues[row];
                }

                var childColumns = new IColumn[typeCount];
                var childStart = new int[typeCount];
                var childLength = new int[typeCount];
                for (int i = 0; i < typeCount; i++)
                {
                    childLength[i] = filled[i];

                    // Densify each per-type bucket before writing it: the resolved codec writes only the dense wire
                    // shape, and a composite value (array, tuple) arrives in its ergonomic form. The densified column
                    // is what the body phase writes, so store it (not the ergonomic build) in childColumns.
                    IColumn built = FlatBuilderFor(children[i].ElementType)(column.Name, children[i].TypeName, buckets[i], filled[i]);
                    childColumns[i] = children[i].TryDensify(built, out _);
                    childStates[i] = children[i].BeginWrite(childColumns[i], 0, filled[i]);
                    statesBuilt = i + 1;
                }

                return new DynamicWriteState
                {
                    TypeNames = names,
                    Children = children,
                    ChildColumns = childColumns,
                    ChildStart = childStart,
                    ChildLength = childLength,
                    ChildStates = childStates,
                    Discriminators = discriminators,
                    Length = length,
                    Width = DynamicWire.DiscriminatorWidth(typeCount),
                };
            }
            catch
            {
                ArrayPool<int>.Shared.Return(discriminators);
                for (int i = 0; i < statesBuilt; i++)
                {
                    childStates[i]?.Dispose();
                }

                throw;
            }
            finally
            {
                for (int i = 0; i < buckets.Length; i++)
                {
                    ArrayPool<object>.Shared.Return(buckets[i], clearArray: true);
                }
            }
        }
        finally
        {
            ArrayPool<string>.Shared.Return(rowTypes, clearArray: true);
            ArrayPool<object>.Shared.Return(rowValues, clearArray: true);
        }
    }

    private IColumnCodec ResolveForMeasure(string typeName)
    {
        measureCodecs ??= new Dictionary<string, IColumnCodec>(StringComparer.Ordinal);
        if (!measureCodecs.TryGetValue(typeName, out IColumnCodec codec))
        {
            codec = registry.Resolve(typeName, in context);
            measureCodecs[typeName] = codec;
        }

        return codec;
    }

    private static Func<string, string, object[], int, IColumn> FlatBuilderFor(Type elementType)
        => FlatBuilders.GetOrAdd(elementType, static type => (Func<string, string, object[], int, IColumn>)
            typeof(DynamicColumnCodec)
                .GetMethod(nameof(BuildFlatColumn), BindingFlags.NonPublic | BindingFlags.Static)
                .MakeGenericMethod(type)
                .CreateDelegate(typeof(Func<string, string, object[], int, IColumn>)));

    private static IColumn BuildFlatColumn<T>(string name, string typeName, object[] boxed, int count)
    {
        var values = new T[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (T)boxed[i];
        }

        return new ArrayColumn<T>(name, typeName, values);
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
    // runtime type list, the per-row discriminators, and each type's child column plus the slice within it.
    private sealed class DynamicWriteState : IColumnWriteState
    {
        public string[] TypeNames;
        public IColumnCodec[] Children;
        public IColumn[] ChildColumns;
        public int[] ChildStart;
        public int[] ChildLength;
        public IColumnWriteState[] ChildStates;
        public int[] Discriminators;
        public int Length;
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

            if (Discriminators is not null)
            {
                ArrayPool<int>.Shared.Return(Discriminators);
                Discriminators = null;
            }
        }
    }
}
