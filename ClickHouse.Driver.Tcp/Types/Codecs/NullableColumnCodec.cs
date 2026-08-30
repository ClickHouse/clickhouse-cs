using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
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
    private static readonly MethodInfo IsNullAtMethod =
        typeof(NullableColumnCodec).GetMethod(nameof(IsNullAt), BindingFlags.Public | BindingFlags.Static);

    private static readonly MethodInfo NullMappedMethod =
        typeof(NullableColumnCodec).GetMethod(nameof(NullMapped), BindingFlags.Public | BindingFlags.Static);

    private readonly IColumnCodec inner;
    private readonly INullableShape canonicalShape;

    // Built lazily because only diagnostics and POCO planning enumerate it. Races are harmless.
    private Type[] writableElementTypes;

    private NullableColumnCodec(string typeName, IColumnCodec inner)
    {
        TypeName = typeName;
        this.inner = inner;

        // The canonical shape drives reads and the read-back element type: reads always surface the inner's
        // canonical ElementType made nullable.
        canonicalShape = NullableShapes.For(inner.ElementType);

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
        if (TryInnerWriteType(writeType, out Type innerType) && inner.CanWriteElementType(innerType))
        {
            return null;
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

    /// <summary>
    /// Forwards a storage reading to the inner codec over the dense inner column, with the null-map deciding
    /// whether a row is read at all — the lifting rule <see cref="TryProjectRead"/> applies to a value, applied to
    /// a reading taken off the column instead. It is how a <c>Nullable(String)</c> reads as a <c>byte[]</c>.
    /// </summary>
    public bool TryProjectColumnRead(Expression column, Expression row, Type targetType, out Expression projected)
    {
        projected = null;

        // Only a reference-typed target has room for this surface's null. A Nullable<U> one would need the inner to
        // offer U off its storage and then be lifted, which no reading needs today.
        if (targetType.IsValueType || targetType == ElementType)
        {
            return false;
        }

        // Both are spliced into the condition, so bind them once: the column behind a cast and the row behind
        // whatever arithmetic the caller passed.
        ParameterExpression nullable = Expression.Variable(typeof(INullableColumn), "nullableColumn");
        ParameterExpression index = Expression.Variable(typeof(int), "nullableRow");
        if (!inner.TryProjectColumnRead(Expression.Property(nullable, nameof(INullableColumn.Inner)), index, targetType, out Expression innerProjection))
        {
            return false;
        }

        // The inner column holds a decoded value at every row, the null positions included, so its content there is
        // a meaningless placeholder and the map has to be consulted first.
        projected = Expression.Block(
            new[] { nullable, index },
            Expression.Assign(nullable, Expression.Call(NullMappedMethod, column)),
            Expression.Assign(index, row),
            Expression.Condition(
                Expression.Call(IsNullAtMethod, nullable, index),
                Expression.Default(targetType),
                innerProjection));
        return true;
    }

    /// <summary>Whether the row is NULL, read through the column's null-map.</summary>
    /// <param name="column">The decoded nullable column.</param>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>Whether the row is NULL.</returns>
    /// <exception cref="IndexOutOfRangeException"><paramref name="row"/> is negative or not less than the row count.</exception>
    // A method rather than an inline span index: ReadOnlySpan is a ref struct, which an expression tree cannot hold.
    public static bool IsNullAt(INullableColumn column, int row) => column.NullMap[row] != 0;

    /// <summary>The column's null-map surface, which a storage reading needs to know which rows to read.</summary>
    /// <param name="column">The decoded column.</param>
    /// <returns>The same column, as its nullable surface.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="column"/> exposes no null-map.</exception>
    // Named here for the same reason StringColumnCodec.RowBytes names its own: a column built by a caller and
    // labelled Nullable(T) need not carry a null-map, and a bare cast error would name neither it nor the reading.
    public static INullableColumn NullMapped(IColumn column) => column as INullableColumn
        ?? throw new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) was read as {column.GetType()}, which exposes no null-map through INullableColumn, " +
            $"so a reading taken off its storage cannot tell which rows are NULL. Only a Nullable column decoded from a server response does.");

    /// <summary>Builds the nullable write-type list on first use.</summary>
    /// <returns>The write types, each the inner's spelling made nullable.</returns>
    private Type[] EnsureWritableElementTypes()
    {
        Type[] surface = writableElementTypes;
        if (surface is not null)
        {
            return surface;
        }

        IReadOnlyList<Type> innerTypes = inner.WritableElementTypes;
        surface = new Type[innerTypes.Count];
        for (int i = 0; i < innerTypes.Count; i++)
        {
            surface[i] = NullableShapes.For(innerTypes[i]).NullableElementType;
        }

        writableElementTypes = surface;
        return surface;
    }

    /// <inheritdoc/>
    public bool CanWriteElementType(Type elementType)
        => TryInnerWriteType(elementType, out Type innerType) && inner.CanWriteElementType(innerType);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => ResolveWriteShape(column) is not null;

    private static bool TryInnerWriteType(Type elementType, out Type innerType)
    {
        innerType = Nullable.GetUnderlyingType(elementType);
        if (innerType is not null)
        {
            return true;
        }

        if (elementType.IsValueType)
        {
            return false;
        }

        innerType = elementType;
        return true;
    }

    /// <inheritdoc/>
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildState(column, start, length);

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using NullableWriteState state = BuildState(column, start, length);
        WriteStatePrefixCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => WriteStatePrefixCore(writer, state.Expect<NullableWriteState>(TypeName));

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using NullableWriteState state = BuildState(column, start, length);
        WriteBody(writer, column, start, length, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => WriteBody(writer, column, start, length, state.Expect<NullableWriteState>(TypeName));

    private INullableShape ResolveWriteShape(IColumn column)
    {
        if (!TryInnerWriteType(column.ElementType, out Type innerType))
        {
            return null;
        }

        INullableShape shape = NullableShapes.For(innerType);
        return shape.CanWrite(inner, column) ? shape : null;
    }

    private NullableWriteState BuildState(IColumn column, int start, int length)
    {
        INullableShape shape = ResolveWriteShape(column)
            ?? throw new ArgumentException(
                $"A {TypeName} column must hold a nullable CLR type its inner codec accepts, not {column.GetType()}.",
                nameof(column));

        IColumn innerColumn = shape.GetInnerColumn(inner, column);
        IColumnWriteState innerState = inner.BeginWrite(innerColumn, start, length);
        return new NullableWriteState
        {
            Shape = shape,
            InnerColumn = innerColumn,
            InnerState = innerState,
            Start = start,
            Length = length,
        };
    }

    private void WriteStatePrefixCore(ClickHouseBinaryWriter writer, NullableWriteState state)
        => inner.WriteStatePrefix(writer, state.InnerColumn, state.Start, state.Length, state.InnerState);

    private void WriteBody(ClickHouseBinaryWriter writer, IColumn column, int start, int length, NullableWriteState state)
    {
        state.Shape.WriteNullMap(writer, column, start, length);
        inner.WriteColumn(writer, state.InnerColumn, state.Start, state.Length, state.InnerState);
    }

    private sealed class NullableWriteState : IColumnWriteState
    {
        public INullableShape Shape;
        public IColumn InnerColumn;
        public IColumnWriteState InnerState;
        public int Start;
        public int Length;

        public void Dispose() => InnerState?.Dispose();
    }
}
