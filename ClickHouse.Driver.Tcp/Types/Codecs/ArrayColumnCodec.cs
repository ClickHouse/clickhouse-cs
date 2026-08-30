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
    private static readonly MethodInfo ProjectArrayMethod =
        typeof(ArrayColumnCodec<TElement>).GetMethod(nameof(ProjectArray), BindingFlags.NonPublic | BindingFlags.Static);

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
    public object NullPlaceholderAs(Type writeType)
    {
        if (!CompositeElementProjections.TryGetArrayElement(writeType, out Type sourceElement)
            || !inner.CanWriteElementType(sourceElement))
        {
            throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
        }

        return writeType == ElementType ? NullPlaceholder : Array.CreateInstance(sourceElement, 0);
    }

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
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, typeof(TElement[]), TypeName);

        if (targetType == typeof(TElement[]))
        {
            projected = value;
            return true;
        }

        projected = null;

        // Only T[] has the required row shape.
        if (!CompositeElementProjections.TryGetArrayElement(targetType, out Type targetElement))
        {
            return false;
        }

        ParameterExpression element = Expression.Variable(typeof(TElement), "element");
        if (!inner.TryProjectRead(element, targetElement, out Expression elementProjection))
        {
            return false;
        }

        projected = CompositeElementProjections.ProjectArray(value, element, elementProjection);
        return true;
    }

    /// <summary>
    /// Forwards a column-level reading to the element codec over the flat element column, then reslices it per row.
    /// Offered only where the element codec has one of its own: an element whose values convert one at a time is
    /// cheaper projected into the row array <see cref="TryProjectRead"/> already builds.
    /// </summary>
    public bool TryProjectColumnRead(Type targetType, out ColumnReadProjection projection)
    {
        projection = null;

        if (targetType == ElementType || !CompositeElementProjections.TryGetArrayElement(targetType, out Type targetElement))
        {
            return false;
        }

        if (!inner.TryProjectColumnRead(targetElement, out ColumnReadProjection elementProjection))
        {
            return false;
        }

        projection = ColumnProjection.Close(ProjectArrayMethod, elementProjection, targetElement);
        return true;
    }

    /// <summary>
    /// Builds the view over one decoded column: the flat element column projected once, then addressed per row
    /// through the offsets this column already holds.
    /// </summary>
    /// <typeparam name="T">The projected element type; the view's element type is <c>T[]</c>.</typeparam>
    /// <param name="source">The decoded <c>Array(T)</c> column.</param>
    /// <param name="elementProjection">The element codec's projection of the flat element column.</param>
    /// <returns>The view.</returns>
    private static IColumn ProjectArray<T>(IColumn source, ColumnReadProjection elementProjection)
    {
        IArrayColumn array = ColumnProjection.Surface<IArrayColumn>(source);
        var elements = (IColumn<T>)elementProjection(array.Inner);
        return new ProjectedReadColumn<T[]>(source, (column, row) => Row(((IArrayColumn)column).Offsets, elements, row));
    }

    /// <summary>Reads one row's slice of the projected element column into a new array.</summary>
    // Read through the indexer, not Values: an element belongs to exactly one row, so there is nothing for the rows
    // to share, and materializing the whole element column to copy one slice out of it would convert every other
    // row's elements as well.
    private static T[] Row<T>(ReadOnlySpan<int> offsets, IColumn<T> elements, int row)
    {
        int start = offsets[row];
        int length = offsets[row + 1] - start;
        if (length == 0)
        {
            return Array.Empty<T>();
        }

        var projected = new T[length];
        for (int i = 0; i < length; i++)
        {
            projected[i] = elements[start + i];
        }

        return projected;
    }

    /// <inheritdoc/>
    public bool CanWriteElementType(Type elementType)
        => CompositeElementProjections.TryGetArrayElement(elementType, out Type sourceElement)
            && inner.CanWriteElementType(sourceElement);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column)
        => TryDense(column, out _) || ResolveWriteShape(column) is not null;

    /// <summary>
    /// Recognizes a column already in the wire's layout whose elements the inner codec takes as they stand, so the
    /// offsets and the element column are re-emitted with nothing rebuilt.
    ///
    /// <para>
    /// The element type is not required to be this codec's own <typeparamref name="TElement"/>: a caller building
    /// the dense shape names the CLR type it holds, which for a convenience type differs from the canonical one an
    /// <c>Array(DateTime)</c> decodes to. Matching only the canonical type would send those columns down the jagged
    /// path, where the flattening view indexes the outer column per element and each access materializes the whole
    /// row again — quadratic in the row's length for a shape that needed no work at all.
    /// </para>
    /// </summary>
    private bool TryDense(IColumn column, out IDenseArrayColumn dense)
    {
        dense = column as IDenseArrayColumn;
        if (dense is not null && inner.CanWrite(dense.Inner))
        {
            return true;
        }

        dense = null;
        return false;
    }

    // Resolve the shape from the row's CLR element type.
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
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildState(column, start, length);

    /// <inheritdoc/>
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

    // Write cumulative offsets, then the flattened elements.
    private void WriteBody(ClickHouseBinaryWriter writer, IColumn column, int start, int length, ArrayWriteState state)
    {
        if (TryDense(column, out IDenseArrayColumn dense))
        {
            // Rebase the stored offsets to this slice.
            ReadOnlySpan<int> offsets = dense.Offsets;
            int elementBase = offsets[start];
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)(offsets[start + i + 1] - elementBase));
            }
        }
        else
        {
            int[] sliceOffsets = state.SliceOffsets;
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)sliceOffsets[i + 1]);
            }
        }

        if (state.Elements is not null)
        {
            // Sectioned codecs must see the whole flattened element column.
            inner.WriteColumn(writer, state.Elements, state.ElementBase, state.ElementCount, state.InnerState);
            return;
        }

        // Span-writable leaves can write each source row directly.
        state.Shape.WriteRuns(inner, writer, column, start, length);
    }

    // Prepare the element range and the inner codec's state once per slice.
    private ArrayWriteState BuildState(IColumn column, int start, int length)
    {
        if (TryDense(column, out IDenseArrayColumn dense))
        {
            // Dense columns already contain the flattened element column.
            ReadOnlySpan<int> offsets = dense.Offsets;
            int elementBase = offsets[start];
            int elementCount = offsets[start + length] - elementBase;
            IColumnWriteState innerState = inner.BeginWrite(dense.Inner, elementBase, elementCount);
            return new ArrayWriteState(dense.Inner, elementBase, elementCount, innerState, sliceOffsets: null, shape: null);
        }

        // Compute ergonomic offsets once for both write phases.
        IArrayWriteShape shape = ResolveWriteShape(column)
            ?? throw new ArgumentException(
                $"A {TypeName} column must hold rows of a CLR type its element codec accepts, not {column.GetType()}.",
                nameof(column));

        int[] sliceOffsets = shape.ComputeOffsets(column, start, length);
        int total = sliceOffsets[length];
        if (shape.InnerWritesSpans(inner))
        {
            return new ArrayWriteState(elements: null, elementBase: 0, total, innerState: null, sliceOffsets, shape);
        }

        // Give sectioned codecs a lazy flattened view.
        IColumn view = shape.CreateFlatteningView(inner.TypeName, column, start, sliceOffsets, total);
        IColumnWriteState viewState = inner.BeginWrite(view, 0, total);
        return new ArrayWriteState(view, elementBase: 0, total, viewState, sliceOffsets, shape);
    }

    // State shared by the prefix and body writes for one slice.
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

        public IArrayWriteShape Shape { get; }

        public void Dispose() => InnerState?.Dispose();
    }
}
