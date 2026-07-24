using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for a ClickHouse <c>Nested(name1 T1, ..., namen Tn)</c> column carried as a single wire column — the
/// case a table declares under <c>flatten_nested = 0</c>. Its layout after the type string is byte-identical to
/// <c>Array(Tuple(T1, ..., Tn))</c>: the fields' serialization-state prefixes in order, then a per-row offsets
/// stream (<c>num_rows</c> little-endian <c>UInt64</c>, each the cumulative element end after that row), then each
/// field's encoding for every element concatenated end-to-end. Within a row every field carries the same element
/// count (the server enforces it), so the single offsets stream delimits all fields.
///
/// <para>
/// Unlike the other composites this codec is arity-agnostic and does not route through the tuple codec, so it is
/// not bound by the tuple's element-count cap — a <c>Nested</c> commonly has many fields. It owns one child codec
/// per field and loops them for every phase; the decoded <see cref="NestedColumn"/> keeps the fields as flat
/// columns plus offsets (the columnar, zero-copy shape), which is also the write source.
/// </para>
///
/// <para>
/// Under the server-default <c>flatten_nested = 1</c> there is no <c>Nested</c> wire type at all: the server
/// presents the column as N parallel <c>Array(T_i)</c> columns with dotted names, each a plain array handled by
/// the array codec. Only the single-column form reaches this codec.
/// </para>
/// </summary>
internal sealed class NestedColumnCodec : IColumnCodec
{
    private readonly IColumnCodec[] children;
    private readonly string[] fieldNames;

    private NestedColumnCodec(string typeName, IColumnCodec[] children, string[] fieldNames)
    {
        TypeName = typeName;
        this.children = children;
        this.fieldNames = fieldNames;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <summary>
    /// A <c>Nested</c> column surfaces as an array of records (<c>object[][]</c>). This is never consumed
    /// compositionally — ClickHouse does not allow <c>Nested</c> as an inner type of another type — but the
    /// interface requires an element type.
    /// </summary>
    public Type ElementType => typeof(object[][]);

    /// <summary>
    /// The placeholder for an absent <c>Nested</c> value is the empty row (no elements). A <c>Nested</c> is never
    /// wrapped in <c>Nullable</c> (the server rejects it), so this is a formality the interface requires.
    /// </summary>
    public object NullPlaceholder => Array.Empty<object[]>();

    /// <summary>Builds a <c>Nested(...)</c> codec, resolving each field's codec through the registry.</summary>
    /// <param name="node">The parsed <c>Nested</c> node; each argument is a named field (<c>name Type</c>).</param>
    /// <param name="context">The resolution context, forwarded to each field codec's factory.</param>
    /// <param name="registry">The registry used to resolve the field codecs.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The type has no fields, or a field is unnamed.</exception>
    /// <exception cref="NotSupportedException">A field type is unsupported.</exception>
    public static NestedColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        if (node.Arguments.Count == 0)
        {
            throw new FormatException($"Nested type '{node}' must have at least one field.");
        }

        // Every Nested field is named — an unnamed field is malformed (the server never emits one, and it could
        // not have created the column). NamedElementParser handles the shared 'name Type' element syntax: the
        // field name is glued to the type by the first whitespace, since the tokenizer only breaks on ',()'.
        (string Name, TypeNode Type)[] elements = NamedElementParser.Split(node);
        var childCodecs = new IColumnCodec[elements.Length];
        var names = new string[elements.Length];
        for (int i = 0; i < elements.Length; i++)
        {
            if (elements[i].Name is null)
            {
                throw new FormatException($"Nested type '{node}' has an unnamed field; every Nested field must be named.");
            }

            childCodecs[i] = registry.ResolveNode(elements[i].Type, in context);
            names[i] = elements[i].Name;
        }

        return new NestedColumnCodec(node.ToString(), childCodecs, names);
    }

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        // A Nested has no prefix of its own; it delegates the prefix phase to each field's serialization, in order,
        // matching the inner Tuple(...) it is byte-compatible with. Empty unless a field type is versioned.
        foreach (IColumnCodec child in children)
        {
            await child.ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            // An empty column writes no offsets and no field streams: read zero-row field columns and wrap them
            // with the single sentinel offset (offsets[0] = 0) every nested column carries.
            return await ReadFieldsAndWrapAsync(reader, columnName, columnType, new int[1], rowCount: 0, totalElements: 0, pooledOffsets: false, cancellationToken).ConfigureAwait(false);
        }

        long offsetBytes = (long)rowCount * sizeof(ulong);
        if (offsetBytes > Array.MaxLength)
        {
            throw new ClickHouseProtocolException(
                $"Nested column '{columnName}' declares {rowCount} rows, whose offsets stream exceeds the maximum this client can buffer.");
        }

        int[] offsets = ArrayPool<int>.Shared.Rent(rowCount + 1);
        byte[] scratch = ArrayPool<byte>.Shared.Rent((int)offsetBytes);
        try
        {
            await reader.ReadBytesAsync(scratch.AsMemory(0, (int)offsetBytes), cancellationToken).ConfigureAwait(false);
            DecodeOffsets(scratch.AsSpan(0, (int)offsetBytes), offsets, rowCount, columnName);
            return await ReadFieldsAndWrapAsync(reader, columnName, columnType, offsets, rowCount, offsets[rowCount], pooledOffsets: true, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ArrayPool<int>.Shared.Return(offsets);
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    /// <summary>
    /// Reads every field's flat column (each holding <paramref name="totalElements"/> values) and wraps them with
    /// <paramref name="offsets"/> into a <see cref="NestedColumn"/>. On any failure disposes whatever fields were
    /// read and rethrows; the <paramref name="offsets"/> lifetime is owned by the caller (returned by the caller's
    /// own failure handler when pooled, or by the constructed column on success), so this method never touches it.
    /// </summary>
    private async ValueTask<IColumn> ReadFieldsAndWrapAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int[] offsets, int rowCount, int totalElements, bool pooledOffsets, CancellationToken cancellationToken)
    {
        var fieldColumns = new IColumn[children.Length];
        int read = 0;
        try
        {
            for (int i = 0; i < children.Length; i++)
            {
                fieldColumns[i] = await children[i].ReadColumnAsync(reader, columnName, children[i].TypeName, totalElements, cancellationToken).ConfigureAwait(false);
                read = i + 1;
            }

            // The constructed column takes ownership of the field columns and the offsets.
            return new NestedColumn(columnName, columnType, fieldNames, fieldColumns, offsets, rowCount, pooledOffsets, ownsFields: true);
        }
        catch
        {
            for (int i = 0; i < read; i++)
            {
                fieldColumns[i].Dispose();
            }

            throw;
        }
    }

    /// <summary>
    /// Decodes the per-row offsets stream — the <paramref name="rowCount"/> little-endian <c>UInt64</c> cumulative
    /// element ends — into <paramref name="offsets"/>, prepending the <c>offsets[0] = 0</c> sentinel. Validates
    /// that the stream never runs backwards and never declares more elements than this client can address.
    /// </summary>
    private static void DecodeOffsets(ReadOnlySpan<byte> offsetBytes, Span<int> offsets, int rowCount, string columnName)
    {
        // Offsets are little-endian UInt64 (this client is little-endian only, like every fixed-width codec).
        ReadOnlySpan<ulong> wire = MemoryMarshal.Cast<byte, ulong>(offsetBytes);
        offsets[0] = 0;
        ulong previous = 0;
        for (int i = 0; i < rowCount; i++)
        {
            ulong end = wire[i];
            if (end < previous)
            {
                throw new ClickHouseProtocolException(
                    $"Nested column '{columnName}' has a non-monotonic offset at row {i} ({end} < {previous}); the stream is corrupt.");
            }

            if (end > int.MaxValue)
            {
                throw new ClickHouseProtocolException(
                    $"Nested column '{columnName}' declares {end} total elements, exceeding the maximum this client can address.");
            }

            offsets[i + 1] = (int)end;
            previous = end;
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column)
    {
        if (column is not NestedColumn nested || nested.FieldCount != children.Length)
        {
            return false;
        }

        for (int i = 0; i < children.Length; i++)
        {
            if (!children[i].CanWrite(nested.GetField(i)))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    // Project each field's flat column and slice once and create each field codec's own write state over it, so a
    // data-dependent field (Dynamic) sees its real values at prefix time and the projection spans both phases.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildState(column, start, length);

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using NestedWriteState state = BuildState(column, start, length);
        WriteStatePrefixCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is NestedWriteState nestedState)
        {
            WriteStatePrefixCore(writer, nestedState);
            return;
        }

        WriteStatePrefix(writer, column, start, length);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using NestedWriteState state = BuildState(column, start, length);
        WriteBodyCore(writer, column, start, length, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is NestedWriteState nestedState)
        {
            WriteBodyCore(writer, column, start, length, nestedState);
            return;
        }

        WriteColumn(writer, column, start, length);
    }

    private void WriteStatePrefixCore(ClickHouseBinaryWriter writer, NestedWriteState state)
    {
        for (int f = 0; f < children.Length; f++)
        {
            children[f].WriteStatePrefix(writer, state.FieldColumns[f], state.ElementBase, state.ElementCount, state.FieldStates[f]);
        }
    }

    private void WriteBodyCore(ClickHouseBinaryWriter writer, IColumn column, int start, int length, NestedWriteState state)
    {
        var nested = AsNested(column);
        ReadOnlySpan<int> offsets = nested.Offsets;

        // The wire offsets are relative to this slice's own field streams, so subtract the slice's first element
        // index from each cumulative end.
        int elementBase = offsets[start];
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt64((ulong)(offsets[start + i + 1] - elementBase));
        }

        for (int f = 0; f < children.Length; f++)
        {
            children[f].WriteColumn(writer, state.FieldColumns[f], state.ElementBase, state.ElementCount, state.FieldStates[f]);
        }
    }

    // Builds the per-field projection state for a slice: each field's flat column with the slice's element range,
    // plus each field codec's own write state over it.
    private NestedWriteState BuildState(IColumn column, int start, int length)
    {
        var nested = AsNested(column);
        ReadOnlySpan<int> offsets = nested.Offsets;
        int elementBase = offsets[start];
        int elementCount = offsets[start + length] - elementBase;

        var fieldColumns = new IColumn[children.Length];
        var fieldStates = new IColumnWriteState[children.Length];
        int built = 0;
        try
        {
            for (int f = 0; f < children.Length; f++)
            {
                fieldColumns[f] = nested.GetField(f);
                fieldStates[f] = children[f].BeginWrite(fieldColumns[f], elementBase, elementCount);
                built = f + 1;
            }
        }
        catch
        {
            // A later field's BeginWrite throwing must not leak the field states already built (each may hold
            // rented buffers).
            for (int f = 0; f < built; f++)
            {
                fieldStates[f]?.Dispose();
            }

            throw;
        }

        return new NestedWriteState { FieldColumns = fieldColumns, ElementBase = elementBase, ElementCount = elementCount, FieldStates = fieldStates };
    }

    // A Nested column has no jagged/row-oriented write form (that would need a per-row record type and reintroduce
    // an arity cap); the dense NestedColumn is the only write source, so anything else is a caller error.
    private NestedColumn AsNested(IColumn column)
    {
        if (column is NestedColumn nested && nested.FieldCount == children.Length)
        {
            return nested;
        }

        throw new ArgumentException(
            $"The '{TypeName}' codec writes a NestedColumn with {children.Length} field(s); got '{column?.GetType().Name ?? "null"}'.",
            nameof(column));
    }

    // The per-field projection of one slice, shared across the prefix and body phases: each field's (borrowed)
    // flat column, the shared element range, and each field codec's own state.
    private sealed class NestedWriteState : IColumnWriteState
    {
        public IColumn[] FieldColumns;
        public int ElementBase;
        public int ElementCount;
        public IColumnWriteState[] FieldStates;

        public void Dispose()
        {
            if (FieldStates is not null)
            {
                foreach (IColumnWriteState state in FieldStates)
                {
                    state?.Dispose();
                }
            }
        }
    }
}
