using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Caches <see cref="Block.ReadAs{T}(string)"/> projections by type name, resolution context, and target type.
/// The context preserves session-dependent conversions such as timezone-less <c>DateTime</c>.
/// </summary>
internal sealed class ColumnReadProjections
{
    // Distinguishes "no reading offered" from "not resolved yet", so a refused target is not re-resolved per call.
    private static readonly object NoReading = new();

    // Limit long-lived compiled projections when callers use many session timezones. The lock-free count check
    // may overshoot slightly under concurrency; uncached projections remain correct.
    private const int MaxCachedReaders = 1024;

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

        ColumnReadProjection projection = Projection(column.TypeName, typeof(T), in context);
        if (projection is null)
        {
            throw NoSuchReading<T>(column, in context);
        }

        return (IColumn<T>)projection(column);
    }

    private ColumnReadProjection Projection(string typeName, Type target, in ResolveContext context)
    {
        var key = (typeName, PocoBlockSignature.ContextKey(in context), target);
        if (readers.TryGetValue(key, out object cached))
        {
            return cached == NoReading ? null : (ColumnReadProjection)cached;
        }

        // Not GetOrAdd: the factory would have to capture the context by value anyway, and a lost race just
        // resolves an equivalent projection that is then dropped.
        ColumnReadProjection projection = ColumnProjection.For(registry.Resolve(typeName, in context), target);
        if (readers.Count < MaxCachedReaders)
        {
            readers[key] = projection ?? NoReading;
        }

        return projection;
    }

    private InvalidCastException NoSuchReading<T>(IColumn column, in ResolveContext context)
    {
        // Only on the failure path, so re-resolving the codec to name its readings costs nothing that matters.
        IReadOnlyList<Type> readable = registry.Resolve(column.TypeName, in context).ReadableElementTypes;
        return new InvalidCastException(
            $"Column '{column.Name}' has type '{column.TypeName}', whose values cannot be read as {typeof(T)}. It reads as: {string.Join(", ", readable)}.");
    }
}
