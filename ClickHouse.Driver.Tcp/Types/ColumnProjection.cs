using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The reading of a column that <see cref="Block.ReadAs{T}(string)"/> gives: the column itself when it already is an
/// <see cref="IColumn{T}"/>, else a <see cref="DerivedColumn{T}"/> over the reader that the converter derivation gives
/// for the column's type (decision D1).
/// </summary>
internal static class ColumnProjection
{
    /// <summary>Reads <paramref name="column"/> as <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">The CLR type to read the values as.</typeparam>
    /// <param name="column">A decoded column, which carries its ClickHouse type.</param>
    /// <param name="derivation">The derivation of the registry that decoded the column.</param>
    /// <param name="context">The context that the column's codec was resolved with.</param>
    /// <returns>The column, a view over it, or null when the column's type cannot be read as <typeparamref name="T"/>.</returns>
    /// <exception cref="InvalidOperationException">The column does not have the decoded shape that the reader needs.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public static IColumn<T> For<T>(IColumn column, ConverterDerivation derivation, in ResolveContext context)
    {
        if (column is IColumn<T> already)
        {
            return already;
        }

        Derivation derived = derivation.Derive(column.TypeName, in context, typeof(T), ConversionDirection.Read);
        if (!derived.Succeeded)
        {
            return null;
        }

        // Bind now, so a column without the decoded shape fails here. The view converts on its first access.
        return new DerivedColumn<T>(column, ((ColumnReader<T>)derived.Converter).Bind(column));
    }
}

/// <summary>
/// The views of <see cref="Block.ReadAs{T}(string)"/> that one block owns: one for each (column, CLR type), so a second
/// call gives the view of the first, with its converted values. When the block is disposed, every view gives its values
/// back to the pool. Safe for concurrent use.
/// </summary>
internal sealed class DerivedViews
{
    private readonly object gate = new();
    private List<Entry> entries;
    private bool released;

    /// <summary>A set that is already released: a view added to it is released at once.</summary>
    public static DerivedViews Released { get; } = new() { released = true };

    /// <summary>The view of <paramref name="source"/> as <typeparamref name="T"/> that the block keeps.</summary>
    /// <typeparam name="T">The CLR type of the view.</typeparam>
    /// <param name="source">The decoded column.</param>
    /// <param name="view">The view, or null when the block keeps none.</param>
    /// <returns>Whether the block keeps a view.</returns>
    public bool TryGet<T>(IColumn source, out IColumn<T> view)
    {
        lock (gate)
        {
            view = Find<T>(source);
            return view is not null;
        }
    }

    /// <summary>
    /// Keeps <paramref name="view"/> for <paramref name="source"/>, or gives the view that another thread kept first.
    /// A view added after <see cref="Release"/> is released at once, so its first access throws.
    /// </summary>
    /// <typeparam name="T">The CLR type of the view.</typeparam>
    /// <param name="source">The decoded column.</param>
    /// <param name="view">A view that has not converted any value yet.</param>
    /// <returns>The view that the block keeps.</returns>
    public IColumn<T> Add<T>(IColumn source, DerivedColumn<T> view)
    {
        lock (gate)
        {
            if (released)
            {
                view.Release();
                return view;
            }

            IColumn<T> kept = Find<T>(source);
            if (kept is not null)
            {
                return kept;
            }

            (entries ??= new List<Entry>()).Add(new Entry(source, typeof(T), view));
            return view;
        }
    }

    /// <summary>Gives the values of every view back to the pool. A later access to a view throws <see cref="ObjectDisposedException"/>.</summary>
    public void Release()
    {
        List<Entry> views;
        lock (gate)
        {
            released = true;
            views = entries;
            entries = null;
        }

        if (views is null)
        {
            return;
        }

        foreach (Entry entry in views)
        {
            entry.View.Release();
        }
    }

    private IColumn<T> Find<T>(IColumn source)
    {
        if (entries is not null)
        {
            foreach (Entry entry in entries)
            {
                if (entry.Is(source, typeof(T)))
                {
                    return (IColumn<T>)entry.View;
                }
            }
        }

        return null;
    }

    private sealed class Entry
    {
        private readonly IColumn source;
        private readonly Type target;

        public Entry(IColumn source, Type target, IDerivedColumn view)
        {
            this.source = source;
            this.target = target;
            View = view;
        }

        public IDerivedColumn View { get; }

        public bool Is(IColumn column, Type type) => ReferenceEquals(source, column) && target == type;
    }
}
