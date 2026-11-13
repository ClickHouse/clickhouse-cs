using System;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The children of a tuple column after the first <c>offset</c>, as a tuple column of their own. The reader of a tuple of
/// more than seven elements reads its eighth field (<c>TRest</c>) from this view with the reader of a tuple of the
/// remaining elements. The view borrows the children: disposing it does nothing. It has no value of its own: read its
/// children.
/// </summary>
internal sealed class TupleTailColumn : ITupleColumn
{
    private readonly ITupleColumn source;

    /// <summary>Initializes the view.</summary>
    /// <param name="source">The tuple column.</param>
    /// <param name="offset">The number of children before the view, less than the number of children.</param>
    public TupleTailColumn(ITupleColumn source, int offset)
    {
        this.source = source;
        Children = source.Children.Skip(offset).ToArray();
        FieldNames = source.FieldNames.Count == 0 ? Array.Empty<string>() : source.FieldNames.Skip(offset).ToArray();
    }

    /// <inheritdoc/>
    public string Name => source.Name;

    /// <inheritdoc/>
    public string TypeName => source.TypeName;

    /// <inheritdoc/>
    public int RowCount => source.RowCount;

    /// <inheritdoc/>
    public IReadOnlyList<IColumn> Children { get; }

    /// <inheritdoc/>
    public IReadOnlyList<string> FieldNames { get; }

    /// <summary>Not supported: the view has no value of its own.</summary>
    /// <param name="row">The row.</param>
    /// <returns>Never returns.</returns>
    /// <exception cref="NotSupportedException">Always.</exception>
    public object GetValue(int row)
        => throw new NotSupportedException($"{nameof(TupleTailColumn)} has no value of its own; read its children.");

    /// <inheritdoc/>
    public void Dispose()
    {
    }
}
