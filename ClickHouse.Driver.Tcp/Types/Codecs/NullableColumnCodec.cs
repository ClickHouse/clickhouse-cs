using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Nullable(T)</c> column. It owns no bytes of its own beyond the null-map: it
/// delegates the serialization-state prefix to the inner codec, then reads/writes a per-row null-map (one
/// <c>UInt8</c> each: non-zero means NULL) followed by the inner type's encoding for <em>all</em> rows —
/// placeholders included at the null positions. The decoded column surfaces each row as the inner CLR value or
/// <see langword="null"/>: a value type as <c>T?</c> (<see cref="NullableValueColumn{T}"/>), a reference type as the
/// nullable reference (<see cref="NullableReferenceColumn{T}"/>).
///
/// <para>
/// The codec itself stays non-generic; the generic work — building the typed wrapper column, and reading and
/// filling a caller's column — is delegated to a cached, per-element-type <see cref="INullableShape"/>.
/// </para>
///
/// <para>
/// On the write path a Nullable column may be supplied in any of the CLR write types the inner codec accepts
/// (<see cref="IColumnCodec.WritableElementTypes"/>), each made nullable — so <c>Nullable(DateTime)</c> takes
/// either <c>DateTimeOffset?</c> or <c>DateTime?</c>. One <see cref="INullableShape"/> is built per write type;
/// the supplied column picks its shape, which fills the placeholder buffer in that same write type via the inner
/// codec's <see cref="IColumnCodec.NullPlaceholderAs"/>. Reads always produce the canonical
/// <see cref="IColumnCodec.ElementType"/> made nullable.
/// </para>
/// </summary>
internal sealed class NullableColumnCodec : IColumnCodec
{
    private readonly IColumnCodec inner;
    private readonly INullableShape canonicalShape;
    private readonly (Type Spelling, INullableShape Shape)[] writeShapes;
    private readonly bool innerCanWrite;

    // Built lazily to avoid write-shape allocations when the codec is used only for reads. Races are harmless.
    private Type[] writableElementTypes;

    private NullableColumnCodec(string typeName, IColumnCodec inner)
    {
        TypeName = typeName;
        this.inner = inner;

        // The canonical shape drives reads and the read-back element type: reads always surface the inner's
        // canonical ElementType made nullable.
        canonicalShape = NullableShapes.For(inner.ElementType);

        // Preserve the inner codec's write-type order; its canonical type remains preferred.
        IReadOnlyList<Type> writeTypes = inner.WritableElementTypes;
        writeShapes = new (Type, INullableShape)[writeTypes.Count];
        for (int i = 0; i < writeTypes.Count; i++)
        {
            writeShapes[i] = (writeTypes[i], NullableShapes.For(writeTypes[i]));
        }

        // Reject a non-writable inner codec before streaming starts.
        innerCanWrite = canonicalShape.CanInnerWrite(inner);
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => canonicalShape.NullableElementType;

    /// <summary>
    /// The inner codec's readings, each made nullable through the same shape rule reads use — so
    /// <c>Nullable(DateTime)</c> reports <c>uint?</c>, <c>DateTimeOffset?</c> and <c>DateTime?</c>. Diagnostics only,
    /// and only ever read on a failure path, so it is built per call rather than cached.
    /// </summary>
    public IReadOnlyList<Type> ReadableElementTypes
    {
        get
        {
            IReadOnlyList<Type> innerTypes = inner.ReadableElementTypes;
            var lifted = new Type[innerTypes.Count];
            for (int i = 0; i < innerTypes.Count; i++)
            {
                lifted[i] = NullableShapes.For(innerTypes[i]).NullableElementType;
            }

            return lifted;
        }
    }

    /// <summary>
    /// The inner codec's writable CLR types, each made nullable.
    /// </summary>
    public IReadOnlyList<Type> WritableElementTypes => EnsureWritableElementTypes();

    /// <summary>
    /// The placeholder for an absent value.
    /// </summary>
    public object NullPlaceholder => null;

    /// <summary>
    /// Returns <see langword="null"/> for any writable CLR type.
    /// </summary>
    /// <param name="writeType">The CLR write type to express the placeholder in.</param>
    /// <returns><see langword="null"/>.</returns>
    /// <exception cref="NotSupportedException"><paramref name="writeType"/> is not a writable element type.</exception>
    public object NullPlaceholderAs(Type writeType)
    {
        foreach (Type writable in EnsureWritableElementTypes())
        {
            if (writable == writeType)
            {
                return null;
            }
        }

        throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
    }

    /// <summary>Builds a <c>Nullable(T)</c> codec, resolving the inner type <c>T</c> through the registry.</summary>
    /// <param name="node">The parsed <c>Nullable</c> type node; its single argument is the inner type.</param>
    /// <param name="context">The resolution context, forwarded to the inner codec's factory.</param>
    /// <param name="registry">The registry used to resolve the inner type's codec.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The type has other than one argument, or the inner is itself <c>Nullable</c>.</exception>
    public static NullableColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        if (node.Arguments.Count != 1)
        {
            throw new FormatException($"Nullable type '{node}' must have exactly one inner type argument.");
        }

        TypeNode innerNode = node.Arguments[0];
        if (innerNode.Name == "Nullable")
        {
            throw new FormatException($"Nullable cannot be nested: '{node}'.");
        }

        IColumnCodec inner = registry.ResolveNode(innerNode, in context);
        return new NullableColumnCodec(node.ToString(), inner);
    }

    /// <summary>
    /// Wraps an inner codec directly, bypassing the registry. Exists so a test can build this wrapper over a stand-in
    /// inner whose read surface no registered type has — the read lifting rule is written for shapes the registry
    /// cannot yet produce, and this is the only way to reach them.
    /// </summary>
    /// <param name="inner">The inner codec to wrap.</param>
    /// <returns>The codec.</returns>
    internal static NullableColumnCodec Over(IColumnCodec inner)
        => new($"Nullable({inner.TypeName})", inner);

    /// <inheritdoc/>
    public ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
        => inner.ReadStatePrefixAsync(reader, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            IColumn emptyInner = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, 0, cancellationToken).ConfigureAwait(false);
            return canonicalShape.Wrap(columnName, columnType, emptyInner, Array.Empty<byte>(), pooledMap: false);
        }

        byte[] nullMap = ArrayPool<byte>.Shared.Rent(rowCount);
        IColumn innerColumn = null;
        try
        {
            await reader.ReadBytesAsync(nullMap.AsMemory(0, rowCount), cancellationToken).ConfigureAwait(false);
            innerColumn = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, rowCount, cancellationToken).ConfigureAwait(false);

            // Wrap pairs the null-map with the inner column (which holds a real inner value at every row — a
            // placeholder at the null positions) into the typed nullable column that surfaces each null row as
            // null; the inner column's row count becomes the wrapper's. Wrap inside the try: only a successful Wrap
            // takes ownership of the rented map and the inner column, so if it throws (e.g. an element-type mismatch
            // surfacing as a cast failure) neither is leaked.
            return canonicalShape.Wrap(columnName, columnType, innerColumn, nullMap, pooledMap: true);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(nullMap);
            innerColumn?.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, ElementType, TypeName);

        if (targetType == ElementType)
        {
            projected = value;
            return true;
        }

        projected = null;

        // Undo this surface's wrap on the target to recover the inner's spelling. The wrap is invertible, so the
        // target alone decides it — see ColumnValueProjections.TryLiftOverAbsent for why nothing may be inferred from
        // the inner codec's canonical type instead.
        Type innerTarget = Nullable.GetUnderlyingType(targetType);
        if (innerTarget is null)
        {
            // A bare value-typed target has nowhere to put a null row, so this surface cannot offer it — that is what
            // stops Nullable(Int64) from claiming it can produce a plain long.
            if (targetType.IsValueType)
            {
                return false;
            }

            // A reference-typed target holds the null itself, and the surface left it unwrapped.
            innerTarget = targetType;
        }

        return ColumnValueProjections.TryLiftOverAbsent(value, inner, innerTarget, targetType, out projected);
    }

    /// <summary>Builds the nullable write-type list on first use.</summary>
    /// <returns>The write types, each the inner's spelling made nullable.</returns>
    private Type[] EnsureWritableElementTypes()
    {
        Type[] surface = writableElementTypes;
        if (surface is not null)
        {
            return surface;
        }

        surface = new Type[writeShapes.Length];
        for (int i = 0; i < writeShapes.Length; i++)
        {
            surface[i] = writeShapes[i].Shape.NullableElementType;
        }

        writableElementTypes = surface;
        return surface;
    }

    /// <inheritdoc/>
    // Gated on innerCanWrite as CanWrite is, not left to the interface default: an inner that cannot be written at all
    // (Nothing) still reports a surface element type, so membership alone would accept Nullable(Nothing) and the write
    // would fault part-way through a block instead of being refused before any byte went out.
    public bool CanWriteElementType(Type elementType)
    {
        if (!innerCanWrite)
        {
            return false;
        }

        foreach (Type surface in EnsureWritableElementTypes())
        {
            if (surface == elementType)
            {
                return true;
            }
        }

        return false;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => innerCanWrite && ResolveWriteShape(column) is not null;

    /// <inheritdoc/>
    // Every inner type supported today has a data-independent state prefix, so the outer column/slice is
    // forwarded unchanged and ignored by the inner. A future data-dependent inner (e.g. Dynamic) will need the
    // inner's own sliced value column projected here, landed with the prefix->data scratch work.
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
        => inner.WriteStatePrefix(writer, column, start, length);

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        INullableShape shape = ResolveWriteShape(column)
            ?? throw new ArgumentException(
                $"A {TypeName} column must hold one of [{string.Join(", ", Array.ConvertAll(writeShapes, w => w.Spelling.Name))}] made nullable, not {column.GetType()}.",
                nameof(column));

        shape.WriteBody(inner, writer, column, start, length);
    }

    // The shape for the CLR write type the supplied column uses, or null if none of the inner's writable types
    // match. The canonical write type leads writeShapes, so it is preferred when a column matches more than one.
    private INullableShape ResolveWriteShape(IColumn column)
    {
        foreach ((Type _, INullableShape shape) in writeShapes)
        {
            if (shape.CanWrite(column))
            {
                return shape;
            }
        }

        return null;
    }
}
