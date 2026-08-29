using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Compiles and caches the per-row readers behind <see cref="Block.ReadAs{T}(string)"/>: the codec decides which
/// readings a type offers (<see cref="IColumnCodec.TryProjectRead"/>), and this turns the expression it hands back
/// into a delegate over the decoded column.
///
/// <para>
/// One entry per column type, resolution context and target type. The key is the type string rather than the codec
/// instance because a parameterized type (<c>Enum8(...)</c>, <c>DateTime64(3)</c>, anything composing them) builds
/// a fresh codec per block, so a cache keyed on the instance would compile again for every block. The context is
/// part of the key for the same reason it is part of a codec's identity: a timezone-less <c>DateTime</c> resolves
/// its offset from the session timezone, which is baked into the compiled reader.
/// </para>
/// </summary>
internal sealed class ColumnReadProjections
{
    // Distinguishes "no reading offered" from "not compiled yet", so a refused target is not recompiled per call.
    private static readonly object NoReading = new();

    private readonly ColumnCodecRegistry registry;

    private readonly ConcurrentDictionary<(string TypeName, string Context, Type Target), object> readers = new();

    /// <summary>Initializes a cache over the codecs of one registry.</summary>
    /// <param name="registry">The registry whose codecs decide the readings.</param>
    public ColumnReadProjections(ColumnCodecRegistry registry) => this.registry = registry;

    /// <summary>Reads <paramref name="column"/> as <typeparamref name="T"/>, projecting if it is not already that.</summary>
    /// <typeparam name="T">The CLR type to read the values as.</typeparam>
    /// <param name="column">The decoded column, which the result borrows.</param>
    /// <param name="context">The context the column's codec was resolved with.</param>
    /// <returns>The column itself when it already reads as <typeparamref name="T"/>, otherwise a converting view over it.</returns>
    /// <exception cref="InvalidCastException">The column's type offers no reading as <typeparamref name="T"/>.</exception>
    public IColumn<T> ReadAs<T>(IColumn column, in ResolveContext context)
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

        Func<IColumn, int, T> read = Reader<T>(column.TypeName, in context);
        if (read is null)
        {
            throw NoSuchReading<T>(column, in context);
        }

        return new ProjectedReadColumn<T>(column, read);
    }

    private Func<IColumn, int, T> Reader<T>(string typeName, in ResolveContext context)
    {
        var key = (typeName, PocoBlockSignature.ContextKey(in context), typeof(T));
        if (readers.TryGetValue(key, out object cached))
        {
            return cached == NoReading ? null : (Func<IColumn, int, T>)cached;
        }

        // Not GetOrAdd: the factory would have to capture the context by value anyway, and a lost race just
        // compiles an equivalent delegate that is then dropped.
        Func<IColumn, int, T> read = Compile<T>(registry.Resolve(typeName, in context));
        readers[key] = read ?? NoReading;
        return read;
    }

    /// <summary>Builds <c>(column, row) => project(((IColumn&lt;source&gt;)column)[row])</c>, or null when the codec offers no such reading.</summary>
    private static Func<IColumn, int, T> Compile<T>(IColumnCodec codec)
    {
        ParameterExpression column = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression row = Expression.Parameter(typeof(int), "row");

        Type typedColumn = typeof(IColumn<>).MakeGenericType(codec.ElementType);
        PropertyInfo indexer = typedColumn.GetProperty("Item")
            ?? throw new InvalidOperationException($"{typedColumn} has no indexer; the {nameof(ColumnReadProjections)} reader cannot be built.");

        Expression value = Expression.MakeIndex(Expression.Convert(column, typedColumn), indexer, new Expression[] { row });
        return codec.TryProjectRead(value, typeof(T), out Expression projected)
            ? Expression.Lambda<Func<IColumn, int, T>>(projected, column, row).Compile()
            : null;
    }

    private InvalidCastException NoSuchReading<T>(IColumn column, in ResolveContext context)
    {
        // Only on the failure path, so re-resolving the codec to name its readings costs nothing that matters.
        IReadOnlyList<Type> readable = registry.Resolve(column.TypeName, in context).ReadableElementTypes;
        return new InvalidCastException(
            $"Column '{column.Name}' has type '{column.TypeName}', whose values cannot be read as {typeof(T)}. It reads as: {string.Join(", ", readable)}.");
    }
}
