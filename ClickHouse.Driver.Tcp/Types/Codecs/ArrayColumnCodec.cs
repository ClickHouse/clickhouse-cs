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
    /// <returns>The codec, closed over the inner codec's element type.</returns>
    /// <exception cref="FormatException">The type has other than one argument.</exception>
    public static IColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        if (node.Arguments.Count != 1)
        {
            throw new FormatException($"Array type '{node}' must have exactly one inner type argument.");
        }

        IColumnCodec inner = registry.ResolveNode(node.Arguments[0], in context);
        return Factories.GetOrAdd(inner.ElementType, BuildFactory)(node.ToString(), inner);
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
/// each row as the inner CLR value array — <c>Array(UInt32)</c> as <c>uint[]</c>, <c>Array(String)</c> as
/// <c>string[]</c>, <c>Array(Array(UInt8))</c> as <c>byte[][]</c>.
///
/// <para>
/// The codec is generic over the inner element type <typeparamref name="TElement"/> so it can build the typed
/// <see cref="ArrayValueColumn{TElement}"/> and slice inner values without boxing; the registry pipeline is
/// non-generic, so <see cref="ArrayColumnCodec"/> closes this over the inner codec's runtime element type. The
/// inner codec stays non-generic (<see cref="IColumnCodec"/>), so its column is cast to <c>IColumn&lt;TElement&gt;</c>
/// once at the read boundary. On the write path the column is always the dense
/// <see cref="ArrayValueColumn{TElement}"/> (the wire's own shape, written with no copy) — the ergonomic jagged
/// form (<c>TElement[]</c> per row) is projected into it by <see cref="TryDensify"/> before the write.
/// </para>
/// </summary>
/// <typeparam name="TElement">The inner codec's CLR element type; each row surfaces as <typeparamref name="TElement"/>[].</typeparam>
internal sealed class ArrayColumnCodec<TElement> : IColumnCodec
{
    private readonly IColumnCodec inner;
    private readonly bool innerCanWrite;

    internal ArrayColumnCodec(string typeName, IColumnCodec inner)
    {
        TypeName = typeName;
        this.inner = inner;

        // Whether the inner codec can write at all (e.g. Nothing cannot). Computed once so CanWrite can reject an
        // Array(non-writable) column up front rather than letting the write fail mid-stream.
        innerCanWrite = inner.CanWrite(new ArrayColumn<TElement>(string.Empty, inner.TypeName, Array.Empty<TElement>()));
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(TElement[]);

    /// <summary>
    /// The placeholder for an absent <c>Array(T)</c> value is the empty array — a row whose offset advances by
    /// zero and contributes no elements. Relevant only if a composite nests an <c>Array</c> and asks for its
    /// placeholder.
    /// </summary>
    public object NullPlaceholder => Array.Empty<TElement>();

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
            return new ArrayValueColumn<TElement>(columnName, columnType, (IColumn<TElement>)emptyInner, new int[1], rowCount: 0, pooledOffsets: false);
        }

        long offsetBytes = (long)rowCount * sizeof(ulong);
        if (offsetBytes > Array.MaxLength)
        {
            throw new ClickHouseProtocolException(
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
    /// <exception cref="ClickHouseProtocolException">An offset goes backwards, or exceeds <see cref="int.MaxValue"/>.</exception>
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
                    $"Array column '{columnName}' has a non-monotonic offset at row {i} ({end} < {previous}); the stream is corrupt.");
            }

            if (end > int.MaxValue)
            {
                throw new ClickHouseProtocolException(
                    $"Array column '{columnName}' declares {end} total elements, exceeding the maximum this client can address.");
            }

            offsets[i + 1] = (int)end;
            previous = end;
        }
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => innerCanWrite && column is IColumn<TElement[]>;

    /// <inheritdoc/>
    // Builds the dense wire shape (a per-row offsets array plus a single flat inner column holding every row's
    // elements end-to-end) from the ergonomic jagged form once, so a later measure/write indexes/slices it with no
    // re-projection. The flat inner run is itself densified through the inner codec — a no-op for a leaf inner, but
    // for e.g. Array(Nullable(T)) it turns the concatenated T? run into the dense (inner column + null-map) shape.
    // An already-dense column is returned unchanged (built = false) when its inner is already dense.
    public IColumn TryDensify(IColumn column, out bool built)
    {
        if (column is ArrayValueColumn<TElement> dense)
        {
            IColumn densifiedInner = inner.TryDensify(dense.Inner, out bool innerBuilt);
            if (!innerBuilt)
            {
                built = false;
                return column;
            }

            built = true;
            return new ArrayValueColumn<TElement>(dense.Name, dense.TypeName, (IColumn<TElement>)densifiedInner, dense.Offsets.ToArray(), dense.RowCount, pooledOffsets: false);
        }

        var source = (IColumn<TElement[]>)column;
        int rowCount = source.RowCount;
        int total = SumElementCount(source, source.Name, 0, rowCount);

        var offsets = new int[rowCount + 1];
        var flat = new TElement[total];
        int pos = 0;
        for (int i = 0; i < rowCount; i++)
        {
            TElement[] row = source[i];
            if (row.Length > 0)
            {
                Array.Copy(row, 0, flat, pos, row.Length);
                pos += row.Length;
            }

            offsets[i + 1] = pos;
        }

        IColumn innerColumn = inner.TryDensify(new ArrayColumn<TElement>(source.Name, inner.TypeName, flat), out _);
        built = true;
        return new ArrayValueColumn<TElement>(source.Name, source.TypeName, (IColumn<TElement>)innerColumn, offsets, rowCount, pooledOffsets: false);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer) => inner.WriteStatePrefix(writer);

    /// <inheritdoc/>
    // Every column is densified before the write, so the body is always the dense wire shape: the offsets already
    // exist and the inner column already holds every element, so both are written directly. The wire offsets are
    // relative to this slice's values stream, so subtract the slice's first element index from each. The ergonomic
    // jagged form was flattened into this shape by TryDensify.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var dense = (ArrayValueColumn<TElement>)column;
        ReadOnlySpan<int> offsets = dense.Offsets;
        int elementBase = offsets[start];
        for (int i = 0; i < length; i++)
        {
            writer.WriteUInt64((ulong)(offsets[start + i + 1] - elementBase));
        }

        inner.WriteColumn(writer, dense.Inner, elementBase, offsets[start + length] - elementBase);
    }

    /// <summary>
    /// Sums the column's element count, rejecting null rows, and guards that the flat run fits a single array —
    /// the preflight <see cref="TryDensify"/> runs before flattening the jagged rows into one contiguous inner column.
    /// </summary>
    /// <exception cref="ArgumentException">A row is null; <c>Array(T)</c> rows are non-nullable.</exception>
    /// <exception cref="NotSupportedException">The elements sum past <see cref="Array.MaxLength"/>.</exception>
    private static int SumElementCount(IColumn<TElement[]> column, string columnName, int start, int length)
    {
        ulong total64 = 0;
        for (int i = 0; i < length; i++)
        {
            TElement[] row = column[start + i];
            if (row is null)
            {
                throw new ArgumentException(
                    $"Array column '{columnName}' has a null value at row {start + i}; Array(T) rows are non-nullable. Use Array.Empty<T>() for an empty row, or Array(Nullable(T)) to carry null elements.",
                    nameof(column));
            }

            total64 += (ulong)row.Length;
        }

        if (total64 > (ulong)Array.MaxLength)
        {
            throw new NotSupportedException(
                $"Array column '{columnName}' holds {total64} elements in one block, exceeding the maximum ({Array.MaxLength}) this client can buffer.");
        }

        return (int)total64;
    }

    /// <inheritdoc/>
    // One UInt64 offset plus the inner bytes of this row's elements. The column is always the dense wire shape (the
    // pipeline densifies before measuring), so the inner column already holds every element and this row's elements
    // are just its offset range.
    public long MeasureRowBytes(IColumn column, int row)
    {
        var dense = (ArrayValueColumn<TElement>)column;
        ReadOnlySpan<int> offsets = dense.Offsets;
        int elementStart = offsets[row];
        int elementEnd = offsets[row + 1];
        return sizeof(ulong) + MeasureInnerRun(dense.Inner, elementStart, elementEnd - elementStart);
    }

    // The inner bytes for the contiguous run of count elements starting at start in innerColumn: a fixed-width
    // inner prices in O(1); a variable-width inner is walked element by element through the inner codec.
    private long MeasureInnerRun(IColumn innerColumn, int start, int count)
    {
        if (inner.FixedRowByteSize is int width)
        {
            return (long)count * width;
        }

        long bytes = 0;
        int end = start + count;
        for (int e = start; e < end; e++)
        {
            bytes += inner.MeasureRowBytes(innerColumn, e);
        }

        return bytes;
    }
}
