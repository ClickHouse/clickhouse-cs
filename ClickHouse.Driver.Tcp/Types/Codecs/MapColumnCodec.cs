using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
/// lives in the cached per-type-pair <see cref="IMapShape"/>; the codec itself stays non-generic. The codec writes a
/// decoded column only (<see cref="CanWrite"/>): its offsets, rebased to the slice, then its key and value columns
/// through the key and value codecs, with no copy. The converter layer writes every other column.
/// </para>
/// </summary>
internal sealed class MapColumnCodec : IColumnCodec
{
    private readonly IColumnCodec keyCodec;
    private readonly IColumnCodec valueCodec;
    private readonly IMapShape shape;

    private MapColumnCodec(string typeName, IColumnCodec keyCodec, IColumnCodec valueCodec)
    {
        TypeName = typeName;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        shape = MapShapes.For(keyCodec.ElementType, valueCodec.ElementType);
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => shape.MapElementType;

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
            throw new ClickHouseTcpProtocolException(
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
                    throw new ClickHouseTcpProtocolException(
                        $"Map column '{columnName}' has a non-monotonic offset at row {i} ({end} < {previous}); the stream is corrupt.");
                }

                if (end > int.MaxValue)
                {
                    throw new ClickHouseTcpProtocolException(
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
    // A decoded Map column whose key and value columns the key and value codecs write.
    public bool CanWrite(IColumn column)
        => shape.IsDense(column)
            && column is IMapColumn dense
            && keyCodec.CanWrite(dense.KeyColumn)
            && valueCodec.CanWrite(dense.ValueColumn);

    /// <inheritdoc/>
    // The range of the pairs of the slice, and the states of the key and value writes.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length)
    {
        var dense = (IMapColumn)column;
        ReadOnlySpan<int> offsets = dense.Offsets;
        int pairBase = offsets[start];
        int pairCount = offsets[start + length] - pairBase;
        IColumnWriteState keyState = keyCodec.BeginWrite(dense.KeyColumn, pairBase, pairCount);
        try
        {
            return new MapWriteState(pairBase, pairCount, keyState, valueCodec.BeginWrite(dense.ValueColumn, pairBase, pairCount));
        }
        catch
        {
            // The value codec throwing must not leak the key state (it may hold rented buffers).
            keyState?.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<MapWriteState>(TypeName);
        var dense = (IMapColumn)column;
        keyCodec.WriteStatePrefix(writer, dense.KeyColumn, own.PairBase, own.PairCount, own.KeyState);
        valueCodec.WriteStatePrefix(writer, dense.ValueColumn, own.PairBase, own.PairCount, own.ValueState);
    }

    /// <inheritdoc/>
    // The stored offsets, rebased to the slice, then the keys and the values of the slice.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<MapWriteState>(TypeName);
        var dense = (IMapColumn)column;
        ReadOnlySpan<int> offsets = dense.Offsets;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt64((ulong)(offsets[start + i + 1] - own.PairBase));
        }

        keyCodec.WriteColumn(writer, dense.KeyColumn, own.PairBase, own.PairCount, own.KeyState);
        valueCodec.WriteColumn(writer, dense.ValueColumn, own.PairBase, own.PairCount, own.ValueState);
    }

    // The range of the pairs of one slice and the states of the two child writes, shared by the prefix and the body.
    private sealed class MapWriteState : IColumnWriteState
    {
        public MapWriteState(int pairBase, int pairCount, IColumnWriteState keyState, IColumnWriteState valueState)
        {
            PairBase = pairBase;
            PairCount = pairCount;
            KeyState = keyState;
            ValueState = valueState;
        }

        public int PairBase { get; }

        public int PairCount { get; }

        public IColumnWriteState KeyState { get; }

        public IColumnWriteState ValueState { get; }

        public void Dispose()
        {
            KeyState?.Dispose();
            ValueState?.Dispose();
        }
    }
}

/// <summary>Handles one CLR key/value shape: the typed column of a read, and the decoded column that the codec writes.</summary>
internal interface IMapShape
{
    /// <summary>The CLR element type the wrapped column surfaces (<c>KeyValuePair&lt;K, V&gt;[]</c>).</summary>
    Type MapElementType { get; }

    /// <summary>Wraps decoded flat key/value columns and their shared offsets into the typed map column.</summary>
    IColumn Wrap(string name, string typeName, IColumn keys, IColumn values, int[] offsets, int rowCount, bool pooledOffsets);

    /// <summary>Whether the column is a dense Map column of this shape, whose key and value columns are written as they are.</summary>
    /// <param name="column">The column.</param>
    /// <returns>Whether the column is dense.</returns>
    bool IsDense(IColumn column);
}

/// <summary>Resolves and caches the <see cref="IMapShape"/> for a given key/value element type pair.</summary>
internal static class MapShapes
{
    private static readonly ConcurrentDictionary<(Type Key, Type Value), IMapShape> Cache = new();

    /// <summary>Returns the cached shape for the CLR key and value types.</summary>
    public static IMapShape For(Type keyType, Type valueType) => Cache.GetOrAdd((keyType, valueType), Build);

    private static IMapShape Build((Type Key, Type Value) pair)
        => (IMapShape)Activator.CreateInstance(typeof(MapShape<,>).MakeGenericType(pair.Key, pair.Value), nonPublic: true);
}

/// <summary>Handles map rows surfaced as <c>KeyValuePair&lt;TKey, TValue&gt;[]</c>.</summary>
internal sealed class MapShape<TKey, TValue> : IMapShape
{
    /// <inheritdoc/>
    public Type MapElementType => typeof(KeyValuePair<TKey, TValue>[]);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn keys, IColumn values, int[] offsets, int rowCount, bool pooledOffsets)
        => new MapColumn<TKey, TValue>(name, typeName, (IColumn<TKey>)keys, (IColumn<TValue>)values, offsets, rowCount, pooledOffsets);

    /// <inheritdoc/>
    public bool IsDense(IColumn column) => column is MapColumn<TKey, TValue>;
}
