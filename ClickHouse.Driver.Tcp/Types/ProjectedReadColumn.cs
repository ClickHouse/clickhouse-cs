using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Converts another column on access. The source remains owned by its block; this view owns only the array that
/// <see cref="Values"/> materializes.
/// </summary>
/// <typeparam name="T">The projected element type.</typeparam>
internal sealed class ProjectedReadColumn<T> : IColumn<T>
{
    private readonly IColumn source;
    private readonly Func<IColumn, int, T> read;
    private T[] materialized;

    /// <summary>Initializes a projected view over <paramref name="source"/>.</summary>
    /// <param name="source">The decoded column to convert from.</param>
    /// <param name="read">Reads one row of <paramref name="source"/> as <typeparamref name="T"/>.</param>
    public ProjectedReadColumn(IColumn source, Func<IColumn, int, T> read)
    {
        this.source = source ?? throw new ArgumentNullException(nameof(source));
        this.read = read ?? throw new ArgumentNullException(nameof(read));
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName => source.TypeName;

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <summary>
    /// The projected values, converted once and cached. Read them before the source block is disposed.
    /// </summary>
    public ReadOnlySpan<T> Values
    {
        get
        {
            if (materialized is null)
            {
                var values = new T[source.RowCount];
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = read(source, i);
                }

                materialized = values;
            }

            return materialized;
        }
    }

    /// <inheritdoc/>
    public T this[int row] => materialized is not null ? materialized[row] : read(source, row);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <summary>
    /// Releases nothing because the source column belongs to its block.
    /// </summary>
    public void Dispose()
    {
    }
}
