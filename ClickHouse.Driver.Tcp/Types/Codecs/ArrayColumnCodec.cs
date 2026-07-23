using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
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
/// once at the read boundary. On the write path a column is accepted as either the dense
/// <see cref="ArrayValueColumn{TElement}"/> (the wire's own shape, written with no copy) or the ergonomic jagged
/// form (<c>TElement[]</c> per row, flattened through a pooled buffer).
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
    // An already-dense column is returned unchanged when its inner is already dense.
    public IColumn Densify(IColumn column)
    {
        if (column is ArrayValueColumn<TElement> dense)
        {
            IColumn densifiedInner = inner.Densify(dense.Inner);
            return ReferenceEquals(densifiedInner, dense.Inner)
                ? column
                : new ArrayValueColumn<TElement>(dense.Name, dense.TypeName, (IColumn<TElement>)densifiedInner, dense.Offsets.ToArray(), dense.RowCount, pooledOffsets: false);
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

        IColumn innerColumn = inner.Densify(new ArrayColumn<TElement>(source.Name, inner.TypeName, flat));
        return new ArrayValueColumn<TElement>(source.Name, source.TypeName, (IColumn<TElement>)innerColumn, offsets, rowCount, pooledOffsets: false);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer) => inner.WriteStatePrefix(writer);

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (column is ArrayValueColumn<TElement> dense)
        {
            // Dense form (the wire's own layout): the offsets already exist and the inner column already holds
            // every element, so write both directly. The wire offsets are relative to this slice's values stream,
            // so subtract the slice's first element index from each.
            ReadOnlySpan<int> offsets = dense.Offsets;
            int elementBase = offsets[start];
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)(offsets[start + i + 1] - elementBase));
            }

            inner.WriteColumn(writer, dense.Inner, elementBase, offsets[start + length] - elementBase);
            return;
        }

        // Ergonomic jagged form: preflight the whole slice (element count, null-row rejection, size guard) before
        // writing anything, then emit the offsets stream, then flatten the element arrays into a pooled inner-typed
        // buffer (copying array references for a composite inner, values for a leaf inner) and hand it to the inner
        // codec as one contiguous column.
        //
        // The flatten is required for correctness, not just to save calls: the inner codec must see every element
        // at once. A sectioned inner encoding — Nullable's null-map, LowCardinality's dictionary, a nested Array's
        // offsets — emits its section(s) once spanning the whole element run and only then the values. Writing the
        // inner codec one row at a time would restart that layout per row (e.g. null-map/values/null-map/values
        // interleaved) and corrupt the stream, so the elements are gathered contiguous first. A leaf inner that is
        // a pure per-element concatenation (fixed-width, String) would tolerate per-row writes, but the write path
        // stays uniform across inner types rather than branching on their layout.
        //
        // Array(T) rows are themselves non-nullable, so a null row is rejected rather than silently coerced to an
        // empty array; callers pass Array.Empty<T>() for an empty row, or use Array(Nullable(T)) to carry null
        // elements. (ClickHouse rejects Nullable(Array(T)), so an array row itself cannot be null.)
        var source = (IColumn<TElement[]>)column;

        int total = SumElementCount(source, column.Name, start, length);

        // Offsets stream: each row's cumulative element end, relative to this slice's own values stream.
        ulong running = 0;
        for (int i = 0; i < length; i++)
        {
            running += (ulong)source[start + i].Length;
            writer.WriteUInt64(running);
        }

        var flat = ArrayPool<TElement>.Shared.Rent(total);
        try
        {
            int pos = 0;
            for (int i = 0; i < length; i++)
            {
                TElement[] row = source[start + i];
                if (row.Length > 0)
                {
                    Array.Copy(row, 0, flat, pos, row.Length);
                    pos += row.Length;
                }
            }

            inner.WriteColumn(writer, ArrayColumn<TElement>.OverBuffer(column.Name, inner.TypeName, flat, total), 0, total);
        }
        finally
        {
            ArrayPool<TElement>.Shared.Return(flat, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TElement>());
        }
    }

    /// <summary>
    /// Preflight for the jagged write path: sums the slice's element count, rejecting null rows, before a single
    /// byte is written. The flat buffer in <see cref="WriteColumn"/> is int-addressed, so a slice whose elements
    /// sum past <see cref="Array.MaxLength"/> cannot be buffered — catching that here (rather than after the offsets
    /// are already on the wire) keeps a failed write from leaving a half-written, corrupt column behind.
    /// </summary>
    /// <exception cref="ArgumentException">A row in the slice is null; <c>Array(T)</c> rows are non-nullable.</exception>
    /// <exception cref="NotSupportedException">The slice's elements sum past <see cref="Array.MaxLength"/>.</exception>
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
    // One UInt64 offset plus the inner bytes of this row's elements, measured through whichever form the column
    // takes — the wire-shaped dense column or the ergonomic jagged one.
    public long MeasureRowBytes(IColumn column, int row)
        => column is ArrayValueColumn<TElement> dense
            ? MeasureDenseRow(dense, row)
            : MeasureJaggedRow(column, row);

    // Dense form: the inner column already holds every element, so this row's elements are just the offset range.
    private long MeasureDenseRow(ArrayValueColumn<TElement> dense, int row)
    {
        ReadOnlySpan<int> offsets = dense.Offsets;
        int elementStart = offsets[row];
        int elementEnd = offsets[row + 1];
        return sizeof(ulong) + MeasureInnerRun(dense.Inner, elementStart, elementEnd - elementStart);
    }

    // Ergonomic jagged form: this row is its own element array, wrapped (only when non-empty, to skip the wrapper
    // allocation for an empty row) so the inner codec can price its elements.
    private long MeasureJaggedRow(IColumn column, int row)
    {
        TElement[] value = ((IColumn<TElement[]>)column)[row];
        if (value is null)
        {
            throw new ArgumentException(
                $"Array column '{column.Name}' has a null value at row {row}; Array(T) rows are non-nullable. Use Array.Empty<T>() for an empty row, or Array(Nullable(T)) to carry null elements.",
                nameof(column));
        }

        if (value.Length == 0)
        {
            return sizeof(ulong);
        }

        var wrapped = ArrayColumn<TElement>.OverBuffer(column.Name, inner.TypeName, value, value.Length);
        return sizeof(ulong) + MeasureInnerRun(wrapped, start: 0, value.Length);
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
