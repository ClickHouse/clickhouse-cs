using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A lazy write-path view over a nullable value column that presents a real value at every row — the source
/// value where present, a supplied placeholder where null — so an inner value-type codec can write the
/// <c>Nullable(T)</c> values stream straight from the ergonomic <c>T?</c> column without a materialized copy.
/// The view is read per element (it does not expose a contiguous span), matching the per-element write the
/// scattered null positions force. It borrows the source: disposing it does nothing.
/// </summary>
/// <typeparam name="T">The inner value type.</typeparam>
internal sealed class SubstituteValueColumn<T> : IColumn<T>
    where T : struct
{
    private readonly IColumn<T?> source;
    private readonly T placeholder;

    /// <summary>Initializes a substitute view over <paramref name="source"/>, filling nulls with <paramref name="placeholder"/>.</summary>
    /// <param name="typeName">The inner type name the view reports (the codec's own type).</param>
    /// <param name="source">The ergonomic nullable column.</param>
    /// <param name="placeholder">The value written where a row is null.</param>
    public SubstituteValueColumn(string typeName, IColumn<T?> source, T placeholder)
    {
        TypeName = typeName;
        this.source = source;
        this.placeholder = placeholder;
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <inheritdoc/>
    public T this[int row] => source[row].GetValueOrDefault(placeholder);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Not supported: the view is scattered over the source and computes each value per index.</summary>
    /// <exception cref="NotSupportedException">Always — read the view through the indexer.</exception>
    public ReadOnlySpan<T> Values => throw new NotSupportedException($"{nameof(SubstituteValueColumn<T>)} has no contiguous span; read it per element.");

    /// <inheritdoc/>
    public void Dispose()
    {
    }
}

/// <summary>
/// The reference-type counterpart of <see cref="SubstituteValueColumn{T}"/>: a lazy write-path view over a
/// nullable-reference column that presents the source reference where non-null and a supplied placeholder where
/// null, so an inner reference-type codec (e.g. <c>String</c>) can write the <c>Nullable(T)</c> values stream
/// straight from the ergonomic column. Borrows the source: disposing it does nothing.
/// </summary>
/// <typeparam name="T">The inner reference type.</typeparam>
internal sealed class SubstituteReferenceColumn<T> : IColumn<T>
    where T : class
{
    private readonly IColumn<T> source;
    private readonly T placeholder;

    /// <summary>Initializes a substitute view over <paramref name="source"/>, filling nulls with <paramref name="placeholder"/>.</summary>
    /// <param name="typeName">The inner type name the view reports (the codec's own type).</param>
    /// <param name="source">The ergonomic nullable-reference column.</param>
    /// <param name="placeholder">The value written where a row is null.</param>
    public SubstituteReferenceColumn(string typeName, IColumn<T> source, T placeholder)
    {
        TypeName = typeName;
        this.source = source;
        this.placeholder = placeholder;
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <inheritdoc/>
    public T this[int row] => source[row] ?? placeholder;

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>Not supported: the view is scattered over the source and computes each value per index.</summary>
    /// <exception cref="NotSupportedException">Always — read the view through the indexer.</exception>
    public ReadOnlySpan<T> Values => throw new NotSupportedException($"{nameof(SubstituteReferenceColumn<T>)} has no contiguous span; read it per element.");

    /// <inheritdoc/>
    public void Dispose()
    {
    }
}
