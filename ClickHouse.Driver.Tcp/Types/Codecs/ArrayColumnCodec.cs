using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The non-generic entry point for building an <c>Array(T)</c> codec. The codec proper is the generic
/// <see cref="ArrayColumnCodec{TElement}"/>; this closes it over the inner codec's runtime element type. The
/// crossing from a runtime <see cref="Type"/> to the closed generic needs one reflective instantiation, so it is
/// done once per element type and cached as a constructor delegate — every later resolution of that array type
/// just invokes the delegate with no further reflection.
/// </summary>
internal static class ArrayColumnCodec
{
    private static readonly ConcurrentDictionary<Type, Func<string, IColumnCodec, IColumnCodec>> Factories = new();

    /// <summary>Builds an <c>Array(T)</c> codec, resolving the inner type <c>T</c> through the registry.</summary>
    /// <param name="node">The parsed <c>Array</c> type node; its single argument is the inner type.</param>
    /// <param name="context">The resolution context, forwarded to the inner codec's factory.</param>
    /// <param name="registry">The registry used to resolve the inner type's codec.</param>
    /// <param name="typeName">The name to report as the codec's <see cref="IColumnCodec.TypeName"/>, or null to use
    /// <paramref name="node"/>'s own. An alias whose structure is an array (<c>Ring</c>, <c>Polygon</c>) passes its
    /// own name so diagnostics name the type the server sent rather than the structure it stands for.</param>
    /// <returns>The codec, closed over the inner codec's element type.</returns>
    /// <exception cref="FormatException">The type has other than one argument.</exception>
    public static IColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry, string typeName = null)
    {
        if (node.Arguments.Count != 1)
        {
            throw new FormatException($"Array type '{node}' must have exactly one inner type argument.");
        }

        IColumnCodec inner = registry.ResolveNode(node.Arguments[0], in context);
        return Factories.GetOrAdd(inner.ElementType, BuildFactory)(typeName ?? node.ToString(), inner);
    }

    // Closes ArrayColumnCodec<T> over elementType once — via a generic helper invoked reflectively — and returns a
    // delegate that constructs instances with no further reflection.
    private static Func<string, IColumnCodec, IColumnCodec> BuildFactory(Type elementType)
    {
        MethodInfo make = typeof(ArrayColumnCodec).GetMethod(nameof(MakeFactory), BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"{nameof(MakeFactory)} not found; the {nameof(ArrayColumnCodec)} factory was likely trimmed.");
        return (Func<string, IColumnCodec, IColumnCodec>)make.MakeGenericMethod(elementType).Invoke(null, null);
    }

    private static Func<string, IColumnCodec, IColumnCodec> MakeFactory<T>()
        => static (typeName, inner) => new ArrayColumnCodec<T>(typeName, inner);
}

/// <summary>
/// A codec for the ClickHouse <c>Array(T)</c> column. It owns no bytes of its own beyond the offsets: it
/// delegates the serialization-state prefix to the inner codec, then reads/writes a per-row offsets stream
/// (<c>num_rows</c> little-endian <c>UInt64</c>, each the cumulative element end after that row) followed by the
/// inner type's encoding for every element of every row concatenated end-to-end. The decoded column surfaces
/// each row as the inner CLR value array: <c>Array(UInt32)</c> as <c>uint[]</c>, <c>Array(String)</c> as
/// <c>string[]</c>, <c>Array(Array(UInt8))</c> as <c>byte[][]</c>.
///
/// <para>
/// The codec is generic over the inner element type <typeparamref name="TElement"/> so it can build the typed
/// <see cref="ArrayValueColumn{TElement}"/> and slice inner values without boxing; the registry pipeline is
/// non-generic, so <see cref="ArrayColumnCodec"/> closes this over the inner codec's runtime element type. The
/// inner codec stays non-generic (<see cref="IColumnCodec"/>), so its column is cast to <c>IColumn&lt;TElement&gt;</c>
/// once at the read boundary. The codec writes a dense column only (<see cref="CanWrite"/>): its offsets, rebased to
/// the slice, then its inner column through the inner codec, with no copy. The converter layer writes every other
/// column.
/// </para>
/// </summary>
/// <typeparam name="TElement">The inner codec's CLR element type; each row surfaces as <typeparamref name="TElement"/>[].</typeparam>
internal sealed class ArrayColumnCodec<TElement> : IColumnCodec
{
    private readonly IColumnCodec inner;

    internal ArrayColumnCodec(string typeName, IColumnCodec inner)
    {
        TypeName = typeName;
        this.inner = inner;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(TElement[]);

    /// <inheritdoc/>
    public ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
        => inner.ReadStatePrefixAsync(reader, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            // An empty column writes no offsets and no values: read a zero-row inner column and wrap it with the
            // single sentinel offset (offsets[0] = 0) every array column carries.
            IColumn emptyInner = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, 0, cancellationToken).ConfigureAwait(false);
            try
            {
                // Cast inside the try so a mismatched inner element type surfacing as a cast failure disposes the
                // inner column rather than leaking it, matching the non-empty path below.
                return new ArrayValueColumn<TElement>(columnName, columnType, (IColumn<TElement>)emptyInner, new int[1], rowCount: 0, pooledOffsets: false);
            }
            catch
            {
                emptyInner.Dispose();
                throw;
            }
        }

        long offsetBytes = (long)rowCount * sizeof(ulong);
        if (offsetBytes > Array.MaxLength)
        {
            throw new ClickHouseTcpProtocolException(
                $"Array column '{columnName}' declares {rowCount} rows, whose offsets stream exceeds the maximum this client can buffer.");
        }

        int[] offsets = ArrayPool<int>.Shared.Rent(rowCount + 1);
        byte[] scratch = ArrayPool<byte>.Shared.Rent((int)offsetBytes);
        IColumn innerColumn = null;
        try
        {
            await reader.ReadBytesAsync(scratch.AsMemory(0, (int)offsetBytes), cancellationToken).ConfigureAwait(false);
            DecodeOffsets(scratch.AsSpan(0, (int)offsetBytes), offsets, rowCount, columnName);

            innerColumn = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, offsets[rowCount], cancellationToken).ConfigureAwait(false);

            // Cast and wrap inside the try: only a successful wrap takes ownership of the rented offsets and the
            // inner column, so an element-type mismatch surfacing as a cast failure leaks neither.
            return new ArrayValueColumn<TElement>(columnName, columnType, (IColumn<TElement>)innerColumn, offsets, rowCount, pooledOffsets: true);
        }
        catch
        {
            ArrayPool<int>.Shared.Return(offsets);
            innerColumn?.Dispose();
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    /// <summary>
    /// Decodes the per-row offsets stream — the <paramref name="rowCount"/> little-endian <c>UInt64</c> cumulative
    /// element ends read into <paramref name="offsetBytes"/> — into <paramref name="offsets"/>, prepending the
    /// <c>offsets[0] = 0</c> sentinel every array column carries. Validates as it goes that the stream never runs
    /// backwards and never declares more elements than this client can address.
    /// </summary>
    /// <param name="offsetBytes">The raw offsets stream: exactly <paramref name="rowCount"/> little-endian <c>UInt64</c>.</param>
    /// <param name="offsets">The destination, sized for <paramref name="rowCount"/> + 1 entries.</param>
    /// <param name="rowCount">The number of rows (and offsets on the wire).</param>
    /// <param name="columnName">The column name, for diagnostics.</param>
    /// <exception cref="ClickHouseTcpProtocolException">An offset goes backwards, or exceeds <see cref="int.MaxValue"/>.</exception>
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
                throw new ClickHouseTcpProtocolException(
                    $"Array column '{columnName}' has a non-monotonic offset at row {i} ({end} < {previous}); the stream is corrupt.");
            }

            if (end > int.MaxValue)
            {
                throw new ClickHouseTcpProtocolException(
                    $"Array column '{columnName}' declares {end} total elements, exceeding the maximum this client can address.");
            }

            offsets[i + 1] = (int)end;
            previous = end;
        }
    }

    /// <inheritdoc/>
    // A dense array whose inner column the inner codec writes.
    public bool CanWrite(IColumn column) => column is IDenseArrayColumn dense && inner.CanWrite(dense.Inner);

    /// <inheritdoc/>
    // The range of the elements of the slice, and the state of the inner write.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length)
    {
        var dense = (IDenseArrayColumn)column;
        ReadOnlySpan<int> offsets = dense.Offsets;
        int elementBase = offsets[start];
        int elementCount = offsets[start + length] - elementBase;
        return new ArrayWriteState(elementBase, elementCount, inner.BeginWrite(dense.Inner, elementBase, elementCount));
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<ArrayWriteState>(TypeName);
        inner.WriteStatePrefix(writer, ((IDenseArrayColumn)column).Inner, own.ElementBase, own.ElementCount, own.InnerState);
    }

    /// <inheritdoc/>
    // The stored offsets, rebased to the slice, then the elements of the slice.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var own = state.Expect<ArrayWriteState>(TypeName);
        var dense = (IDenseArrayColumn)column;
        ReadOnlySpan<int> offsets = dense.Offsets;
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt64((ulong)(offsets[start + i + 1] - own.ElementBase));
        }

        inner.WriteColumn(writer, dense.Inner, own.ElementBase, own.ElementCount, own.InnerState);
    }

    // State shared by the prefix and body writes for one slice.
    private sealed class ArrayWriteState : IColumnWriteState
    {
        public ArrayWriteState(int elementBase, int elementCount, IColumnWriteState innerState)
        {
            ElementBase = elementBase;
            ElementCount = elementCount;
            InnerState = innerState;
        }

        public int ElementBase { get; }

        public int ElementCount { get; }

        public IColumnWriteState InnerState { get; }

        public void Dispose() => InnerState?.Dispose();
    }
}
