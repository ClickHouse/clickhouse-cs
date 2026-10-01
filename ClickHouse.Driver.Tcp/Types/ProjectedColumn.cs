using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>A borrowed write-path view that converts each source value on access.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
internal sealed class ProjectedColumn<TSource, TResult> : IColumn<TResult>
{
    private readonly IColumn<TSource> source;
    private readonly Func<TSource, TResult> project;

    /// <summary>Initializes a projected view over <paramref name="source"/>.</summary>
    public ProjectedColumn(string typeName, IColumn<TSource> source, Func<TSource, TResult> project)
    {
        TypeName = typeName;
        this.source = source;
        this.project = project;
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <inheritdoc/>
    public TResult this[int row] => project(source[row]);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>The values are computed on access and have no contiguous storage.</summary>
    public ReadOnlySpan<TResult> Values => throw new NotSupportedException(
        $"{nameof(ProjectedColumn<TSource, TResult>)} has no contiguous span; read it per element.");

    /// <inheritdoc/>
    public void Dispose()
    {
    }
}
