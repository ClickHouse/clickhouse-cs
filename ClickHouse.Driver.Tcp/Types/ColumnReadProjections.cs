using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The columnar read tier of <see cref="Block.ReadAs{T}(string)"/> over the codecs of one registry: the column itself
/// when it already reads as the type, else a view over the reader that the converter derivation gives
/// (<see cref="ColumnProjection"/>). The derivation caches its readers by type name, session timezone and CLR type.
/// </summary>
internal sealed class ColumnReadProjections
{
    private readonly ColumnCodecRegistry registry;

    /// <summary>Initializes the tier over the codecs of one registry.</summary>
    /// <param name="registry">The registry whose codecs decoded the columns, and whose derivation reads them.</param>
    public ColumnReadProjections(ColumnCodecRegistry registry) => this.registry = registry;

    /// <summary>Reads <paramref name="column"/> as <typeparamref name="T"/>, with a view that no block owns.</summary>
    /// <typeparam name="T">The CLR type to read the values as.</typeparam>
    /// <param name="column">The decoded column, which the result borrows.</param>
    /// <param name="context">The context the column's codec was resolved with.</param>
    /// <returns>The column itself when it already reads as <typeparamref name="T"/>, otherwise a converting view over it.</returns>
    /// <exception cref="InvalidCastException">The column's type offers no reading as <typeparamref name="T"/>.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public IColumn<T> ReadAs<T>(IColumn column, in ResolveContext context) => ReadAs<T>(column, in context, views: null);

    /// <summary>Reads <paramref name="column"/> as <typeparamref name="T"/>, with the view that <paramref name="views"/> keeps.</summary>
    /// <typeparam name="T">The CLR type to read the values as.</typeparam>
    /// <param name="column">The decoded column, which the result borrows.</param>
    /// <param name="context">The context the column's codec was resolved with.</param>
    /// <param name="views">The views of the column's block, or null for a view that no block owns.</param>
    /// <returns>The column itself when it already reads as <typeparamref name="T"/>, otherwise a converting view over it.</returns>
    /// <exception cref="InvalidCastException">The column's type offers no reading as <typeparamref name="T"/>.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public IColumn<T> ReadAs<T>(IColumn column, in ResolveContext context, DerivedViews views)
    {
        if (column is IColumn<T> already)
        {
            return already;
        }

        if (column.TypeName is null)
        {
            throw new InvalidCastException(
                $"Column '{column.Name}' carries no ClickHouse type (it was built by a caller, not decoded), so it offers no reading other than {column.ElementType}.");
        }

        if (views is not null && views.TryGet(column, out IColumn<T> kept))
        {
            return kept;
        }

        IColumn<T> view = ColumnProjection.For<T>(column, registry.Converters, in context) ?? throw NoSuchReading<T>(column, in context);
        return views is null ? view : views.Add(column, (DerivedColumn<T>)view);
    }

    private InvalidCastException NoSuchReading<T>(IColumn column, in ResolveContext context)
    {
        // Only on the failure path, so re-resolving the codec to name its readings costs nothing that matters.
        IReadOnlyList<Type> readable = registry.Resolve(column.TypeName, in context).ReadableElementTypes;
        return new InvalidCastException(
            $"Column '{column.Name}' has type '{column.TypeName}', whose values cannot be read as {typeof(T)}. It reads as: {string.Join(", ", readable)}.");
    }
}
