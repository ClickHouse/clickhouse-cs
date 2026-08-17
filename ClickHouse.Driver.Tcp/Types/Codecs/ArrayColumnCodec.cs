using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Linq.Expressions;
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
/// once at the read boundary. The write path takes either shape as it comes: the dense
/// <see cref="ArrayValueColumn{TElement}"/> is the wire's own layout and is written with no copy, while the
/// ergonomic jagged form (<c>TElement[]</c> per row) is written from its rows without its elements being copied
/// into a flat buffer first when the inner codec accepts that projected shape. An inner such as <c>Nested</c>,
/// whose only write source is its dense named-field column, therefore requires the dense outer form too.
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
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, typeof(TElement[]), TypeName);

        if (targetType == typeof(TElement[]))
        {
            projected = value;
            return true;
        }

        projected = null;

        // Only an array can hold this surface's rows, and its element type is the one reading the inner codec is
        // asked for. Nothing about the inner is inferred from the outer shape: a rank-2 or jagged target is refused
        // here rather than being read as if it were T[].
        if (!CompositeElementProjections.TryGetArrayElement(targetType, out Type targetElement))
        {
            return false;
        }

        // Ask the inner codec before building anything. The element variable has to exist first so the inner can
        // project from it, but if the inner declines there is no tree to unwind.
        ParameterExpression element = Expression.Variable(typeof(TElement), "element");
        if (!inner.TryProjectRead(element, targetElement, out Expression elementProjection))
        {
            return false;
        }

        projected = CompositeElementProjections.ProjectArray(value, element, elementProjection);
        return true;
    }

    /// <inheritdoc/>
    public bool CanWriteElementType(Type elementType)
        => CompositeElementProjections.TryGetArrayElement(elementType, out Type sourceElement)
            && inner.CanWriteElementType(sourceElement);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column)
        => column is ArrayValueColumn<TElement> dense
            ? inner.CanWrite(dense.Inner)
            : ResolveWriteShape(column) is not null;

    // The shape for the CLR type this column's rows hold their elements in, or null when the inner codec cannot encode
    // them. The row's own element type decides it, so an Array(DateTime) column takes a uint[] row through the shape
    // for uint and a DateTime[] row through the shape for DateTime -- the inner codec converting either as it writes.
    private IArrayWriteShape ResolveWriteShape(IColumn column)
    {
        if (!CompositeElementProjections.TryGetArrayElement(column.ElementType, out Type sourceElement)
            || !inner.CanWriteElementType(sourceElement))
        {
            return null;
        }

        return ArrayWriteShapes.For(sourceElement);
    }

    /// <inheritdoc/>
    // Computed once per slice and handed to both the prefix and body phases; see BuildState for what it holds.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildState(column, start, length);

    /// <inheritdoc/>
    // The Array's own state prefix is the inner codec's, written once over every element of the slice (a leaf inner
    // has none). Callers that already hold the slice's state use the overload below; this one builds its own.
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using ArrayWriteState state = BuildState(column, start, length);
        WriteStatePrefixCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        WriteStatePrefixCore(writer, state.Expect<ArrayWriteState>(TypeName));
    }

    private void WriteStatePrefixCore(ClickHouseBinaryWriter writer, ArrayWriteState state)
    {
        if (state.Elements is not null)
        {
            inner.WriteStatePrefix(writer, state.Elements, state.ElementBase, state.ElementCount, state.InnerState);
        }
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using ArrayWriteState state = BuildState(column, start, length);
        WriteBody(writer, column, start, length, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        WriteBody(writer, column, start, length, state.Expect<ArrayWriteState>(TypeName));
    }

    // Writes this slice's offsets stream — one UInt64 per row, the cumulative element end after that row — then its
    // elements.
    private void WriteBody(ClickHouseBinaryWriter writer, IColumn column, int start, int length, ArrayWriteState state)
    {
        if (column is ArrayValueColumn<TElement> dense)
        {
            // A dense column already carries offsets, but they count from the start of the whole column; rebase them
            // on this slice's first element so each block's stream starts from zero.
            ReadOnlySpan<int> offsets = dense.Offsets;
            int elementBase = offsets[start];
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)(offsets[start + i + 1] - elementBase));
            }
        }
        else
        {
            // An ergonomic column has no offsets of its own; BuildState summed its per-row lengths into
            // slice-relative ones.
            int[] sliceOffsets = state.SliceOffsets;
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)sliceOffsets[i + 1]);
            }
        }

        if (state.Elements is not null)
        {
            // Every element of the slice handed over as one column: the dense column's own flat inner (borrowed, so a
            // re-insert copies nothing), or a flattening view over the ergonomic rows. A sectioned inner needs this —
            // it emits each section (Nullable's null-map, LowCardinality's dictionary, a nested Array's offsets,
            // Dynamic's discriminators) once spanning the whole run, so driving it a row at a time would interleave
            // those sections and corrupt the stream.
            inner.WriteColumn(writer, state.Elements, state.ElementBase, state.ElementCount, state.InnerState);
            return;
        }

        // A leaf inner (see ISpanWritableCodec) encodes as a flat per-element stream with no sections, so each row's
        // array goes out as its own contiguous run — a bulk blit for a fixed-width inner — read straight from the
        // ergonomic column with no flattened view or buffer in between.
        state.Shape.WriteRuns(inner, writer, column, start, length);
    }

    // Builds the scratch the prefix and body phases of one slice share: which elements the slice covers, plus
    // whatever the inner codec needs to write them.
    private ArrayWriteState BuildState(IColumn column, int start, int length)
    {
        if (column is ArrayValueColumn<TElement> dense)
        {
            // A dense column is already the wire's shape: its flat inner is the element column, and the slice's
            // element range is the gap between the offsets bracketing the rows. Nothing is summed, and the inner
            // column is borrowed rather than copied.
            ReadOnlySpan<int> offsets = dense.Offsets;
            int elementBase = offsets[start];
            int elementCount = offsets[start + length] - elementBase;
            IColumnWriteState innerState = inner.BeginWrite(dense.Inner, elementBase, elementCount);
            return new ArrayWriteState(dense.Inner, elementBase, elementCount, innerState, sliceOffsets: null, shape: null);
        }

        // An ergonomic column has to have its offsets derived from the per-row lengths; doing it here means neither
        // phase re-walks the rows. The shape is resolved from the row's own element type, which may be a type the inner
        // codec converts rather than its canonical one.
        IArrayWriteShape shape = ResolveWriteShape(column)
            ?? throw new ArgumentException(
                $"A {TypeName} column must hold rows of a CLR type its element codec accepts, not {column.GetType()}.",
                nameof(column));

        int[] sliceOffsets = shape.ComputeOffsets(column, start, length);
        int total = sliceOffsets[length];
        if (shape.InnerWritesSpans(inner))
        {
            // A leaf inner emits no state prefix and needs no element column — the body writes each row as its own
            // run straight from the source arrays — so the offsets are the whole of the scratch.
            return new ArrayWriteState(elements: null, elementBase: 0, total, innerState: null, sliceOffsets, shape);
        }

        // A sectioned inner has to see every element as one column, so give it a lazy flattening view over the rows
        // instead of copying the elements into a flat buffer.
        IColumn view = shape.CreateFlatteningView(inner.TypeName, column, start, sliceOffsets, total);
        IColumnWriteState viewState = inner.BeginWrite(view, 0, total);
        return new ArrayWriteState(view, elementBase: 0, total, viewState, sliceOffsets, shape);
    }

    // The write scratch of one slice, shared across the prefix and body phases; BuildState decides what goes in it.
    // Elements is the slice's element column, null only for a leaf inner written as runs; SliceOffsets is the
    // ergonomic slice's cumulative element ends, null for a dense column, which reads its own. Nothing here is
    // pooled; disposing releases the inner state.
    private sealed class ArrayWriteState : IColumnWriteState
    {
        public ArrayWriteState(IColumn elements, int elementBase, int elementCount, IColumnWriteState innerState, int[] sliceOffsets, IArrayWriteShape shape)
        {
            Elements = elements;
            ElementBase = elementBase;
            ElementCount = elementCount;
            InnerState = innerState;
            SliceOffsets = sliceOffsets;
            Shape = shape;
        }

        public IColumn Elements { get; }

        public int ElementBase { get; }

        public int ElementCount { get; }

        public IColumnWriteState InnerState { get; }

        public int[] SliceOffsets { get; }

        // The ergonomic slice's write shape, null for a dense column, which needs none.
        public IArrayWriteShape Shape { get; }

        public void Dispose() => InnerState?.Dispose();
    }
}
