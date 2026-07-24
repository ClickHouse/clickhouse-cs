using System;
using System.Buffers;
using System.Collections.Generic;
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

    private NullableColumnCodec(string typeName, IColumnCodec inner)
    {
        TypeName = typeName;
        this.inner = inner;

        // The canonical shape drives reads and the read-back element type: reads always surface the inner's
        // canonical ElementType made nullable.
        canonicalShape = NullableShapes.For(inner.ElementType);

        // One shape per CLR write type the inner codec accepts, so a Nullable column can be supplied in any form
        // the bare inner accepts (e.g. Nullable(DateTime) as DateTimeOffset? or DateTime?). The canonical
        // ElementType leads the list, so it wins when a column would match more than one write type.
        IReadOnlyList<Type> writeTypes = inner.WritableElementTypes;
        writeShapes = new (Type, INullableShape)[writeTypes.Count];
        for (int i = 0; i < writeTypes.Count; i++)
        {
            writeShapes[i] = (writeTypes[i], NullableShapes.For(writeTypes[i]));
        }

        // Whether the inner codec can write at all (e.g. Nothing cannot). Computed once so CanWrite can reject a
        // Nullable(non-writable) column up front rather than letting the write fail mid-stream.
        innerCanWrite = canonicalShape.CanInnerWrite(inner);
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => canonicalShape.NullableElementType;

    /// <summary>
    /// The placeholder for an absent <c>Nullable(T)</c> value is <see langword="null"/> itself — a null-marked
    /// row. Relevant only if a future composite nests a <c>Nullable</c> and asks for its placeholder.
    /// </summary>
    public object NullPlaceholder => null;

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

    /// <inheritdoc/>
    public ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
        => inner.ReadStatePrefixAsync(reader, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            IColumn emptyInner = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, 0, cancellationToken).ConfigureAwait(false);
            return canonicalShape.Wrap(columnName, columnType, emptyInner, Array.Empty<byte>(), rowCount: 0, pooledMap: false);
        }

        byte[] nullMap = ArrayPool<byte>.Shared.Rent(rowCount);
        IColumn innerColumn = null;
        try
        {
            await reader.ReadBytesAsync(nullMap.AsMemory(0, rowCount), cancellationToken).ConfigureAwait(false);
            innerColumn = await inner.ReadColumnAsync(reader, columnName, inner.TypeName, rowCount, cancellationToken).ConfigureAwait(false);

            // Wrap pairs the null-map with the inner column (which holds a real inner value at every row — a
            // placeholder at the null positions) into the typed nullable column that surfaces each null row as
            // null. Wrap inside the try: only a successful Wrap takes ownership of the rented map and the inner
            // column, so if it throws (e.g. an element-type mismatch surfacing as a cast failure) neither is leaked.
            return canonicalShape.Wrap(columnName, columnType, innerColumn, nullMap, rowCount, pooledMap: true);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(nullMap);
            innerColumn?.Dispose();
            throw;
        }
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
