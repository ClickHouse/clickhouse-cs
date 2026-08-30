using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Compiles and caches the per-row readers behind <see cref="Block.ReadAs{T}(string)"/>: the codec decides which
/// readings a type offers (<see cref="IColumnCodec.TryProjectColumnRead"/> off the column's storage, then
/// <see cref="IColumnCodec.TryProjectRead"/> off its decoded value), and this turns the expression it hands back
/// into a delegate over the decoded column.
///
/// <para>
/// A <c>LowCardinality</c> column is read per dictionary entry instead of per row. Its rows are keys into a
/// dictionary of distinct values, so a reading of the column is a reading of each entry, and a row resolves to
/// the entry its key names. That is the same answer the per-row reader would give, at the cost the type exists
/// for: a million rows over a five-entry dictionary convert five values, not a million.
/// </para>
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

    // A ceiling on the cache, which lives as long as the registry. The key includes the session timezone, so an
    // application setting one per request over many types could otherwise accumulate compiled delegates without
    // bound. Past the ceiling a reader is compiled per call: slower, and still correct.
    //
    // Approximate, not exact: the count is read and the entry added as two steps, so first-time compilations
    // racing each other at the ceiling can each see room and all insert, overshooting by however many raced. A
    // lock would make it exact at the cost of contention on a path that runs once per type; the point here is to
    // bound growth, and an overshoot of a few entries does not affect that.
    private const int MaxCachedReaders = 1024;

    private readonly ColumnCodecRegistry registry;

    // PerEntry is part of the key, not a detail: an entry reader is keyed on the dictionary's type, which is also
    // a column type in its own right, and the two disagree about the same target. Reading a DateTime column as a
    // DateTimeOffset? is refused, while reading one dictionary entry of a LowCardinality(Nullable(DateTime)) as one
    // is exactly how that column's rows are read — same type string, same target, different answer.
    private readonly ConcurrentDictionary<(string TypeName, string Context, Type Target, bool PerEntry), object> readers = new();

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

        // Asked first, so which readings exist stays the codec's answer alone — the same one ClickHouseTcpTypes.CanRead
        // gives. The dictionary below only changes how a reading that already exists is computed, never whether.
        Func<IColumn, int, T> read = Reader<T>(column.TypeName, in context);
        if (read is null)
        {
            throw NoSuchReading<T>(column, in context);
        }

        if (column is ILowCardinalityColumn dictionaryColumn)
        {
            IColumn<T> perEntry = ReadPerDictionaryEntry<T>(dictionaryColumn, in context);
            if (perEntry is not null)
            {
                return perEntry;
            }
        }

        return new ProjectedReadColumn<T>(column, read);
    }

    /// <summary>
    /// Builds a view that converts <paramref name="column"/>'s dictionary once per entry, or null when no reading
    /// of an entry as <typeparamref name="T"/> can be built — in which case the caller falls back to the per-row
    /// reader it already holds, which answers the same values.
    /// </summary>
    private IColumn<T> ReadPerDictionaryEntry<T>(ILowCardinalityColumn column, in ResolveContext context)
    {
        // The dictionary holds the bare inner type's values and carries its type string, so the entry reader comes
        // from the inner codec. A caller-built column has no type string to resolve.
        string dictionaryType = column.Dictionary.TypeName;
        if (dictionaryType is null)
        {
            return null;
        }

        // A nullable LowCardinality spells its NULL in the target, not in the dictionary: the entries themselves
        // are never absent, so an entry is read as the target's underlying type and lifted, exactly as the per-row
        // reader lifts over an absent surface value. A non-nullable value target has nowhere to put the NULL; the
        // per-row reader refuses it for the same reason, so returning null here only keeps this path from
        // outliving that rule if it ever changes.
        Type entryTarget = typeof(T);
        if (column.ReservedSlotCount == 2)
        {
            Type underlying = Nullable.GetUnderlyingType(entryTarget);
            if (underlying is not null)
            {
                entryTarget = underlying;
            }
            else if (entryTarget.IsValueType)
            {
                return null;
            }
        }

        Func<IColumn, int, T> readEntry = EntryReader<T>(dictionaryType, entryTarget, in context);
        return readEntry is null ? null : new ProjectedLowCardinalityColumn<T>(column, readEntry);
    }

    private Func<IColumn, int, T> Reader<T>(string typeName, in ResolveContext context)
    {
        var key = (typeName, PocoBlockSignature.ContextKey(in context), typeof(T), PerEntry: false);
        if (readers.TryGetValue(key, out object cached))
        {
            return cached == NoReading ? null : (Func<IColumn, int, T>)cached;
        }

        // Not GetOrAdd: the factory would have to capture the context by value anyway, and a lost race just
        // compiles an equivalent delegate that is then dropped.
        Func<IColumn, int, T> read = Compile<T>(registry.Resolve(typeName, in context));
        if (readers.Count < MaxCachedReaders)
        {
            readers[key] = read ?? NoReading;
        }

        return read;
    }

    private Func<IColumn, int, T> EntryReader<T>(string dictionaryType, Type entryTarget, in ResolveContext context)
    {
        var key = (dictionaryType, PocoBlockSignature.ContextKey(in context), typeof(T), PerEntry: true);
        if (readers.TryGetValue(key, out object cached))
        {
            return cached == NoReading ? null : (Func<IColumn, int, T>)cached;
        }

        Func<IColumn, int, T> read = CompileEntry<T>(registry.Resolve(dictionaryType, in context), entryTarget);
        if (readers.Count < MaxCachedReaders)
        {
            readers[key] = read ?? NoReading;
        }

        return read;
    }

    /// <summary>
    /// Builds <c>(column, row) => project(((IColumn&lt;source&gt;)column)[row])</c>, or the codec's own reading off
    /// the column where it offers one, or null when it offers neither.
    /// </summary>
    private static Func<IColumn, int, T> Compile<T>(IColumnCodec codec)
    {
        ParameterExpression column = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression row = Expression.Parameter(typeof(int), "row");

        // The storage reading is asked for first: it exists precisely where the canonical value has already lost
        // what the caller wants, so projecting from that value would answer with damaged data instead of failing.
        if (codec.TryProjectColumnRead(column, row, typeof(T), out Expression fromStorage))
        {
            return Expression.Lambda<Func<IColumn, int, T>>(fromStorage, column, row).Compile();
        }

        Type typedColumn = typeof(IColumn<>).MakeGenericType(codec.ElementType);
        PropertyInfo indexer = typedColumn.GetProperty("Item")
            ?? throw new InvalidOperationException($"{typedColumn} has no indexer; the {nameof(ColumnReadProjections)} reader cannot be built.");

        Expression value = Expression.MakeIndex(Expression.Convert(column, typedColumn), indexer, new Expression[] { row });
        return codec.TryProjectRead(value, typeof(T), out Expression projected)
            ? Expression.Lambda<Func<IColumn, int, T>>(projected, column, row).Compile()
            : null;
    }

    /// <summary>
    /// Builds a reader over one dictionary entry: the inner codec's reading of the entry as
    /// <paramref name="entryTarget"/>, converted to <typeparamref name="T"/> where the two differ because the
    /// target is nullable and the entry is not. Null when the inner codec offers no such reading.
    /// </summary>
    private static Func<IColumn, int, T> CompileEntry<T>(IColumnCodec inner, Type entryTarget)
    {
        ParameterExpression dictionary = Expression.Parameter(typeof(IColumn), "dictionary");
        ParameterExpression slot = Expression.Parameter(typeof(int), "slot");

        // Same order as the per-row reader: the storage reading exists where the canonical value has already lost
        // what the caller wants, so projecting from that value would answer with damaged data instead of failing.
        if (!inner.TryProjectColumnRead(dictionary, slot, entryTarget, out Expression projected))
        {
            Type typedDictionary = typeof(IColumn<>).MakeGenericType(inner.ElementType);
            PropertyInfo indexer = typedDictionary.GetProperty("Item")
                ?? throw new InvalidOperationException($"{typedDictionary} has no indexer; the {nameof(ColumnReadProjections)} entry reader cannot be built.");

            Expression entry = Expression.MakeIndex(Expression.Convert(dictionary, typedDictionary), indexer, new Expression[] { slot });
            if (entryTarget == inner.ElementType)
            {
                projected = entry;
            }
            else if (!inner.TryProjectRead(entry, entryTarget, out projected))
            {
                return null;
            }
        }

        if (projected.Type != typeof(T))
        {
            projected = Expression.Convert(projected, typeof(T));
        }

        return Expression.Lambda<Func<IColumn, int, T>>(projected, dictionary, slot).Compile();
    }

    private InvalidCastException NoSuchReading<T>(IColumn column, in ResolveContext context)
    {
        // Only on the failure path, so re-resolving the codec to name its readings costs nothing that matters.
        IReadOnlyList<Type> readable = registry.Resolve(column.TypeName, in context).ReadableElementTypes;
        return new InvalidCastException(
            $"Column '{column.Name}' has type '{column.TypeName}', whose values cannot be read as {typeof(T)}. It reads as: {string.Join(", ", readable)}.");
    }
}
