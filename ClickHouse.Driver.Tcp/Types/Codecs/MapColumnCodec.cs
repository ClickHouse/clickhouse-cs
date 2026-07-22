using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Map(K, V)</c> column. The wire layout is byte-identical to
/// <c>Array(Tuple(K, V))</c>: it delegates the serialization-state prefix to the key then the value codec, then
/// reads/writes a per-row offsets stream (<c>num_rows</c> little-endian <c>UInt64</c>, each the cumulative pair
/// end after that row) followed by two concatenated streams — every row's keys, then every row's values,
/// positionally aligned so pair <c>i</c> is <c>(keys[i], values[i])</c>. The decoded column surfaces each row as
/// a <see cref="KeyValuePair{TKey, TValue}"/>[]; a pair array (not a dictionary) is used so duplicate keys and
/// pair order round-trip intact.
///
/// <para>
/// The generic bridge from the non-generic key/value codecs to the right typed <see cref="MapColumn{TKey, TValue}"/>
/// lives in the cached per-type-pair <see cref="IMapShape"/>; the codec itself stays non-generic. On the write
/// path it accepts a column of <c>KeyValuePair&lt;K, V&gt;[]</c> (the dense <see cref="MapColumn{TKey, TValue}"/>,
/// written with no copy, or the ergonomic jagged form, flattened through pooled key/value buffers).
/// </para>
/// </summary>
internal sealed class MapColumnCodec : IColumnCodec
{
    private readonly IColumnCodec keyCodec;
    private readonly IColumnCodec valueCodec;
    private readonly IMapShape shape;
    private readonly bool childrenCanWrite;

    private MapColumnCodec(string typeName, IColumnCodec keyCodec, IColumnCodec valueCodec)
    {
        TypeName = typeName;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        shape = MapShapes.For(keyCodec.ElementType, valueCodec.ElementType);

        // Whether both the key and value codecs can write at all (e.g. Nothing cannot). Computed once so CanWrite
        // can reject a Map(non-writable, ...) column up front rather than letting the write fail mid-stream.
        childrenCanWrite = shape.CanInnerWrite(keyCodec, valueCodec);
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => shape.MapElementType;

    /// <summary>
    /// The placeholder for an absent <c>Map(K, V)</c> value is the empty pair array — a row whose offset advances
    /// by zero and contributes no pairs. Relevant only if a composite nests a <c>Map</c> and asks for its placeholder.
    /// </summary>
    public object NullPlaceholder => shape.EmptyMap;

    /// <summary>Builds a <c>Map(K, V)</c> codec, resolving the key and value types through the registry.</summary>
    /// <param name="node">The parsed <c>Map</c> type node; its two arguments are the key and value types.</param>
    /// <param name="context">The resolution context, forwarded to the key/value codec factories.</param>
    /// <param name="registry">The registry used to resolve the key and value codecs.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The type has other than two arguments.</exception>
    public static MapColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        if (node.Arguments.Count != 2)
        {
            throw new FormatException($"Map type '{node}' must have exactly two type arguments (a key type and a value type).");
        }

        IColumnCodec keyCodec = registry.ResolveNode(node.Arguments[0], in context);
        IColumnCodec valueCodec = registry.ResolveNode(node.Arguments[1], in context);
        return new MapColumnCodec(node.ToString(), keyCodec, valueCodec);
    }

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        // A Map has no prefix of its own; it delegates the prefix phase to its element serializations, key first
        // then value, matching the inner Tuple(K, V) it is byte-compatible with. Empty unless K or V is versioned.
        await keyCodec.ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
        await valueCodec.ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            // An empty column writes no offsets and no values: read zero-row key/value columns and wrap them with
            // the single sentinel offset (offsets[0] = 0) every map column carries.
            IColumn emptyKeys = await keyCodec.ReadColumnAsync(reader, columnName, keyCodec.TypeName, 0, cancellationToken).ConfigureAwait(false);
            IColumn emptyValues;
            try
            {
                emptyValues = await valueCodec.ReadColumnAsync(reader, columnName, valueCodec.TypeName, 0, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                emptyKeys.Dispose();
                throw;
            }

            try
            {
                return shape.Wrap(columnName, columnType, emptyKeys, emptyValues, new int[1], rowCount: 0, pooledOffsets: false);
            }
            catch
            {
                emptyKeys.Dispose();
                emptyValues.Dispose();
                throw;
            }
        }

        long offsetBytes = (long)rowCount * sizeof(ulong);
        if (offsetBytes > Array.MaxLength)
        {
            throw new ClickHouseProtocolException(
                $"Map column '{columnName}' declares {rowCount} rows, whose offsets stream exceeds the maximum this client can buffer.");
        }

        int[] offsets = ArrayPool<int>.Shared.Rent(rowCount + 1);
        byte[] scratch = ArrayPool<byte>.Shared.Rent((int)offsetBytes);
        IColumn keyColumn = null;
        IColumn valueColumn = null;
        try
        {
            await reader.ReadBytesAsync(scratch.AsMemory(0, (int)offsetBytes), cancellationToken).ConfigureAwait(false);

            // Offsets are little-endian UInt64 (this client is little-endian only, like every fixed-width codec).
            ReadOnlySpan<ulong> wire = MemoryMarshal.Cast<byte, ulong>(scratch.AsSpan(0, (int)offsetBytes));
            offsets[0] = 0;
            ulong previous = 0;
            for (int i = 0; i < rowCount; i++)
            {
                ulong end = wire[i];
                if (end < previous)
                {
                    throw new ClickHouseProtocolException(
                        $"Map column '{columnName}' has a non-monotonic offset at row {i} ({end} < {previous}); the stream is corrupt.");
                }

                if (end > int.MaxValue)
                {
                    throw new ClickHouseProtocolException(
                        $"Map column '{columnName}' declares {end} total pairs, exceeding the maximum this client can address.");
                }

                offsets[i + 1] = (int)end;
                previous = end;
            }

            int totalPairs = offsets[rowCount];
            keyColumn = await keyCodec.ReadColumnAsync(reader, columnName, keyCodec.TypeName, totalPairs, cancellationToken).ConfigureAwait(false);
            valueColumn = await valueCodec.ReadColumnAsync(reader, columnName, valueCodec.TypeName, totalPairs, cancellationToken).ConfigureAwait(false);

            // Wrap inside the try: only a successful Wrap takes ownership of the rented offsets and the inner
            // columns, so a throw (e.g. an element-type mismatch surfacing as a cast failure) leaks none of them.
            return shape.Wrap(columnName, columnType, keyColumn, valueColumn, offsets, rowCount, pooledOffsets: true);
        }
        catch
        {
            ArrayPool<int>.Shared.Return(offsets);
            keyColumn?.Dispose();
            valueColumn?.Dispose();
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => childrenCanWrite && shape.CanWrite(column);

    /// <inheritdoc/>
    // Flatten the slice's keys and values once and create the key/value codecs' own write states over them, so a
    // data-dependent value (Dynamic) sees its real values at prefix time and the flatten spans both phases.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length)
        => shape.BeginWrite(keyCodec, valueCodec, column, start, length);

    /// <inheritdoc/>
<<<<<<< HEAD
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (shape.CanWrite(column))
        {
            using IColumnWriteState state = shape.BeginWrite(keyCodec, valueCodec, column, start, length);
            shape.WriteStatePrefix(keyCodec, valueCodec, writer, state);
            return;
        }

        // An outer composite forwarded its own column (its children's prefixes are written from its own slice);
        // the key/value codecs ignore it, preserving the prior contract for a Map nested in Variant/Nested/Tuple.
        keyCodec.WriteStatePrefix(writer, column, start, length);
        valueCodec.WriteStatePrefix(writer, column, start, length);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is not null)
        {
            shape.WriteStatePrefix(keyCodec, valueCodec, writer, state);
            return;
        }

        WriteStatePrefix(writer, column, start, length);
=======
    // Every key/value type supported today has a data-independent state prefix, so the outer column/slice is
    // forwarded unchanged and ignored by the children. A data-dependent child (e.g. Dynamic) would need its
    // flattened element sub-slice projected here, landed with the prefix->data scratch work.
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        keyCodec.WriteStatePrefix(writer, column, start, length);
        valueCodec.WriteStatePrefix(writer, column, start, length);
>>>>>>> bc9e8fd (Widen IColumnCodec.WriteStatePrefix to receive the sliced column)
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using IColumnWriteState state = shape.BeginWrite(keyCodec, valueCodec, column, start, length);
        shape.WriteBody(keyCodec, valueCodec, writer, column, start, length, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is not null)
        {
            shape.WriteBody(keyCodec, valueCodec, writer, column, start, length, state);
            return;
        }

        WriteColumn(writer, column, start, length);
    }
}

/// <summary>
/// The generic bridge for one map key/value type pair: it knows how to build the typed
/// <see cref="MapColumn{TKey, TValue}"/>, test a writable column, drive the offsets-plus-two-streams write, and
/// price a row. One implementation covers every type pair (a <c>KeyValuePair&lt;K, V&gt;[]</c> is a reference
/// type, so unlike the nullable bridge there is no value/reference split); the concrete instance is chosen once
/// per type pair.
/// </summary>
internal interface IMapShape
{
    /// <summary>The CLR element type the wrapped column surfaces (<c>KeyValuePair&lt;K, V&gt;[]</c>).</summary>
    Type MapElementType { get; }

    /// <summary>The empty pair array — a map column's null/absent placeholder.</summary>
    object EmptyMap { get; }

    /// <summary>Wraps decoded flat key/value columns and their shared offsets into the typed map column.</summary>
    IColumn Wrap(string name, string typeName, IColumn keys, IColumn values, int[] offsets, int rowCount, bool pooledOffsets);

    /// <summary>Whether <paramref name="column"/> is a map column of this key/value type pair, writable by the codec.</summary>
    bool CanWrite(IColumn column);

    /// <summary>Whether both the key and value codecs can write their typed column at all (e.g. <c>Nothing</c> cannot).</summary>
    bool CanInnerWrite(IColumnCodec keyCodec, IColumnCodec valueCodec);

    /// <summary>
    /// Flattens the slice's keys and values into contiguous columns once and creates the key and value codecs'
    /// own write states over them, so a data-dependent inner (Dynamic) sees its real values at prefix time and the
    /// flatten is shared across the prefix and body phases.
    /// </summary>
    IColumnWriteState BeginWrite(IColumnCodec keyCodec, IColumnCodec valueCodec, IColumn column, int start, int length);

    /// <summary>Writes the key then value serialization-state prefixes from a computed <see cref="BeginWrite"/> state.</summary>
    void WriteStatePrefix(IColumnCodec keyCodec, IColumnCodec valueCodec, ClickHouseBinaryWriter writer, IColumnWriteState state);

    /// <summary>
    /// Writes the map body for rows [<paramref name="start"/>, start + length): the offsets stream (each offset
    /// relative to this slice's own pairs), then the flattened keys stream, then the flattened values stream, the
    /// latter two from the pre-flattened <paramref name="state"/>. A dense <see cref="MapColumn{TKey, TValue}"/> is
    /// written with no intermediate copy; the ergonomic jagged form uses the state's pooled key/value buffers.
    /// </summary>
    void WriteBody(IColumnCodec keyCodec, IColumnCodec valueCodec, ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state);
}

/// <summary>Resolves and caches the <see cref="IMapShape"/> for a given key/value element type pair.</summary>
internal static class MapShapes
{
    private static readonly ConcurrentDictionary<(Type Key, Type Value), IMapShape> Cache = new();

    /// <summary>Returns the shape for the (<paramref name="keyType"/>, <paramref name="valueType"/>) pair, building it once and caching it.</summary>
    /// <param name="keyType">The key codec's CLR element type.</param>
    /// <param name="valueType">The value codec's CLR element type.</param>
    /// <returns>The shape.</returns>
    public static IMapShape For(Type keyType, Type valueType) => Cache.GetOrAdd((keyType, valueType), Build);

    // nonPublic: true so the shape's (implicit, but internal-assembly) constructor is always reachable here.
    private static IMapShape Build((Type Key, Type Value) pair)
        => (IMapShape)Activator.CreateInstance(typeof(MapShape<,>).MakeGenericType(pair.Key, pair.Value), nonPublic: true);
}

/// <summary>The shape for a key type <typeparamref name="TKey"/> and value type <typeparamref name="TValue"/>: the map column surfaces <c>KeyValuePair&lt;TKey, TValue&gt;[]</c>.</summary>
/// <typeparam name="TKey">The key codec's element type.</typeparam>
/// <typeparam name="TValue">The value codec's element type.</typeparam>
internal sealed class MapShape<TKey, TValue> : IMapShape
{
    /// <inheritdoc/>
    public Type MapElementType => typeof(KeyValuePair<TKey, TValue>[]);

    /// <inheritdoc/>
    public object EmptyMap => Array.Empty<KeyValuePair<TKey, TValue>>();

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn keys, IColumn values, int[] offsets, int rowCount, bool pooledOffsets)
        => new MapColumn<TKey, TValue>(name, typeName, (IColumn<TKey>)keys, (IColumn<TValue>)values, offsets, rowCount, pooledOffsets);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<KeyValuePair<TKey, TValue>[]>;

    /// <inheritdoc/>
    public bool CanInnerWrite(IColumnCodec keyCodec, IColumnCodec valueCodec)
        => keyCodec.CanWrite(new ArrayColumn<TKey>(string.Empty, keyCodec.TypeName, Array.Empty<TKey>()))
        && valueCodec.CanWrite(new ArrayColumn<TValue>(string.Empty, valueCodec.TypeName, Array.Empty<TValue>()));

    /// <inheritdoc/>
    public IColumnWriteState BeginWrite(IColumnCodec keyCodec, IColumnCodec valueCodec, IColumn column, int start, int length)
    {
        if (column is MapColumn<TKey, TValue> dense)
        {
            ReadOnlySpan<int> offsets = dense.Offsets;
            int pairBase = offsets[start];
            int pairCount = offsets[start + length] - pairBase;
            IColumnWriteState denseKeyState = keyCodec.BeginWrite(dense.KeyColumn, pairBase, pairCount);
            IColumnWriteState denseValueState;
            try
            {
                denseValueState = valueCodec.BeginWrite(dense.ValueColumn, pairBase, pairCount);
            }
            catch
            {
                // The value codec throwing must not leak the key state (it may hold rented buffers).
                denseKeyState?.Dispose();
                throw;
            }

            return new MapWriteState((IColumn<TKey>)dense.KeyColumn, (IColumn<TValue>)dense.ValueColumn, pairBase, pairCount, denseKeyState, denseValueState, keyBuffer: null, valueBuffer: null);
        }

        // Ergonomic jagged form: flatten the pair arrays into pooled key and value buffers (copying references for
        // a composite inner, values for a leaf inner). Map(K, V) rows are themselves non-nullable, so a null row is
        // rejected rather than silently coerced to an empty map; callers pass Array.Empty<KeyValuePair<K, V>>() for
        // an empty row, or use Map(K, Nullable(V)) to carry null values.
        var source = (IColumn<KeyValuePair<TKey, TValue>[]>)column;
        ulong running = 0;
        for (int i = 0; i < length; i++)
        {
            KeyValuePair<TKey, TValue>[] row = source[start + i];
            if (row is null)
            {
                throw new ArgumentException(
                    $"Map column '{column.Name}' has a null value at row {start + i}; Map(K, V) rows are non-nullable. Use Array.Empty<KeyValuePair<K, V>>() for an empty row, or Map(K, Nullable(V)) to carry null values.",
                    nameof(column));
            }

            running += (ulong)row.Length;
        }

        // The flat buffers are addressed with an int length, so a slice whose pairs sum past Array.MaxLength cannot
        // be buffered — reject it cleanly rather than truncate the cast and corrupt the streams.
        if (running > (ulong)Array.MaxLength)
        {
            throw new NotSupportedException(
                $"Map column '{column.Name}' holds {running} pairs in one block, exceeding the maximum ({Array.MaxLength}) this client can buffer.");
        }

        int total = (int)running;
        TKey[] flatKeys = ArrayPool<TKey>.Shared.Rent(total);
        TValue[] flatValues = ArrayPool<TValue>.Shared.Rent(total);
        IColumnWriteState keyState = null;
        try
        {
            int pos = 0;
            for (int i = 0; i < length; i++)
            {
                KeyValuePair<TKey, TValue>[] row = source[start + i];
                for (int p = 0; p < row.Length; p++)
                {
                    flatKeys[pos] = row[p].Key;
                    flatValues[pos] = row[p].Value;
                    pos++;
                }
            }

            var keyColumn = ArrayColumn<TKey>.OverBuffer(column.Name, keyCodec.TypeName, flatKeys, total);
            var valueColumn = ArrayColumn<TValue>.OverBuffer(column.Name, valueCodec.TypeName, flatValues, total);
            keyState = keyCodec.BeginWrite(keyColumn, 0, total);
            IColumnWriteState valueState = valueCodec.BeginWrite(valueColumn, 0, total);
            return new MapWriteState(keyColumn, valueColumn, pairBase: 0, total, keyState, valueState, flatKeys, flatValues);
        }
        catch
        {
            // Dispose a key state already created (the value codec may have thrown) before returning the buffers.
            keyState?.Dispose();
            ArrayPool<TKey>.Shared.Return(flatKeys, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
            ArrayPool<TValue>.Shared.Return(flatValues, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TValue>());
            throw;
        }
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(IColumnCodec keyCodec, IColumnCodec valueCodec, ClickHouseBinaryWriter writer, IColumnWriteState state)
    {
        var mapState = (MapWriteState)state;
        keyCodec.WriteStatePrefix(writer, mapState.FlatKeys, mapState.PairBase, mapState.PairCount, mapState.KeyState);
        valueCodec.WriteStatePrefix(writer, mapState.FlatValues, mapState.PairBase, mapState.PairCount, mapState.ValueState);
    }

    /// <inheritdoc/>
    public void WriteBody(IColumnCodec keyCodec, IColumnCodec valueCodec, ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var mapState = (MapWriteState)state;

        // Offsets, relative to this slice's own pair streams: from the dense column's offsets, or each jagged row's
        // pair count. The key/value bodies come from the pre-flattened state.
        if (column is MapColumn<TKey, TValue> dense)
        {
            ReadOnlySpan<int> offsets = dense.Offsets;
            int pairBase = offsets[start];
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)(offsets[start + i + 1] - pairBase));
            }
        }
        else
        {
            var source = (IColumn<KeyValuePair<TKey, TValue>[]>)column;
            ulong running = 0;
            for (int i = 0; i < length; i++)
            {
                running += (ulong)source[start + i].Length;
                writer.WriteUInt64(running);
            }
        }

        keyCodec.WriteColumn(writer, mapState.FlatKeys, mapState.PairBase, mapState.PairCount, mapState.KeyState);
        valueCodec.WriteColumn(writer, mapState.FlatValues, mapState.PairBase, mapState.PairCount, mapState.ValueState);
    }

    // The flatten of one slice's keys and values, shared across the prefix and body phases. For the ergonomic
    // jagged form the columns are backed by pooled buffers returned on dispose; for the dense form they are the
    // borrowed key/value columns (no buffers to return).
    private sealed class MapWriteState : IColumnWriteState
    {
        private readonly TKey[] keyBuffer;
        private readonly TValue[] valueBuffer;

        public MapWriteState(IColumn<TKey> flatKeys, IColumn<TValue> flatValues, int pairBase, int pairCount, IColumnWriteState keyState, IColumnWriteState valueState, TKey[] keyBuffer, TValue[] valueBuffer)
        {
            FlatKeys = flatKeys;
            FlatValues = flatValues;
            PairBase = pairBase;
            PairCount = pairCount;
            KeyState = keyState;
            ValueState = valueState;
            this.keyBuffer = keyBuffer;
            this.valueBuffer = valueBuffer;
        }

        public IColumn<TKey> FlatKeys { get; }

        public IColumn<TValue> FlatValues { get; }

        public int PairBase { get; }

        public int PairCount { get; }

        public IColumnWriteState KeyState { get; }

        public IColumnWriteState ValueState { get; }

        public void Dispose()
        {
            KeyState?.Dispose();
            ValueState?.Dispose();
            if (keyBuffer is not null)
            {
                ArrayPool<TKey>.Shared.Return(keyBuffer, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
            }

            if (valueBuffer is not null)
            {
                ArrayPool<TValue>.Shared.Return(valueBuffer, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TValue>());
            }
        }
    }
}
