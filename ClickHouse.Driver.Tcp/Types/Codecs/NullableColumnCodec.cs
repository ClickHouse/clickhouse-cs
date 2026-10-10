using System;
using System.Buffers;
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
/// The codec itself stays non-generic; the generic work, building the typed wrapper column, is delegated to a
/// cached, per-element-type <see cref="INullableShape"/>.
/// </para>
///
/// <para>
/// The codec writes the decoded column only (<see cref="CanWrite"/>): its null map, then its inner column through the
/// inner codec. The converter layer writes every other column.
/// </para>
/// </summary>
internal sealed class NullableColumnCodec : IColumnCodec
{
    private readonly IColumnCodec inner;
    private readonly INullableShape canonicalShape;

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
    // A decoded Nullable column: the null map and the inner column that the inner codec writes.
    public bool CanWrite(IColumn column)
    {
        Type type = column.GetType();
        return type.IsGenericType
            && (type.GetGenericTypeDefinition() == typeof(NullableValueColumn<>) || type.GetGenericTypeDefinition() == typeof(NullableReferenceColumn<>))
            && inner.CanWrite(((INullableColumn)column).Inner);
    }

    /// <inheritdoc/>
    // The state is the state of the inner column.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length)
        => inner.BeginWrite(((INullableColumn)column).Inner, start, length);

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => inner.WriteStatePrefix(writer, ((INullableColumn)column).Inner, start, length, state);

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        var nullable = (INullableColumn)column;
        writer.WriteBytes(nullable.NullMap.Slice(start, length));
        inner.WriteColumn(writer, nullable.Inner, start, length, state);
    }
}
