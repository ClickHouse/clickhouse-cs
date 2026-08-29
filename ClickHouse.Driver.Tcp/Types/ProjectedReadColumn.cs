using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A read-side view that converts each value of another column on access — what
/// <see cref="Block.ReadAs{T}(string)"/> returns when the requested type is not the column's own. The source
/// column is borrowed and stays the block's to dispose; this view owns nothing but the array
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
    /// The projected values, converted once and cached. The array is this view's own, not a pooled buffer, so it
    /// is not returned on <see cref="Dispose"/> — but it is only as valid as the source column it was read from,
    /// so build it while the owning block is alive.
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
    /// Releases nothing: the source column belongs to the block that produced it, and emptying it here would take
    /// the block's data away from every other reader of it.
    /// </summary>
    public void Dispose()
    {
    }
}
