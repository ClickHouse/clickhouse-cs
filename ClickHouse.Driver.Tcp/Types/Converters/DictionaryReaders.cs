using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>LowCardinality(X)</c> read as <typeparamref name="T"/>, and <c>LowCardinality(Nullable(X))</c> read as a
/// reference type. The child converts the dictionary entries one time for each column, and each row gives the entry of
/// its key, so the rows that share a key share one value.
/// </summary>
/// <remarks>
/// <para>
/// The dictionary of <c>LowCardinality(Nullable(X))</c> is a column of the bare <c>X</c>, and its slot 0 is the NULL
/// slot (<see cref="ILowCardinalityColumn.ReservedSlotCount"/> is 2). The child does not convert that slot, because it
/// holds a placeholder, which a child conversion can refuse. Its entry is <c>default</c>, so a NULL row gives null.
/// </para>
/// <para>
/// An entry that the child cannot convert does not fail the conversion of the dictionary: a read throws its failure
/// only at a row whose key is that entry. An entry that no row of the read uses does not fail the read.
/// </para>
/// <para>
/// The reader keeps the converted entries of each column (<see cref="DictionaryEntryCache{TEntry}"/>), so all the reads
/// of one column convert its dictionary once. A POCO read runs the setup of <see cref="Emit"/> for each window of rows,
/// and that setup also binds the element reader of an <c>Array</c> or a <c>Map</c> again.
/// </para>
/// </remarks>
/// <typeparam name="T">The CLR type of one value.</typeparam>
internal sealed class DictionaryReader<T> : ColumnReader<T>
{
    private static readonly MethodInfo EntriesMethod =
        typeof(DictionaryEntryCache<T>).GetMethod(nameof(DictionaryEntryCache<T>.For));

    private static readonly MethodInfo ValueAtMethod =
        typeof(DictionaryEntries<T>).GetMethod(nameof(DictionaryEntries<T>.ValueAt));

    private readonly DictionaryEntryCache<T> entries;

    /// <summary>Initializes the reader over the reader of the dictionary type.</summary>
    /// <param name="inner">Reads the dictionary column.</param>
    public DictionaryReader(ColumnReader<T> inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        entries = new DictionaryEntryCache<T>(column => DictionaryEntries<T>.Convert(inner, column, static values => values));
    }

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column) => new Bound(ColumnSurface.Of<ILowCardinalityColumn>(column), entries);

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression lowCardinality = scope.Local(typeof(ILowCardinalityColumn), "lowCardinality", EmitScope.Surface<ILowCardinalityColumn>(column));
        ParameterExpression converted = scope.Local(
            typeof(DictionaryEntries<T>),
            "entries",
            Expression.Call(Expression.Constant(entries), EntriesMethod, lowCardinality));
        ParameterExpression keys = scope.Local(typeof(ReadOnlySpan<int>), "keys", Expression.Property(lowCardinality, nameof(ILowCardinalityColumn.Keys)));
        return Expression.Call(converted, ValueAtMethod, EmitScope.ElementAt(keys, row));
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly ILowCardinalityColumn column;
        private readonly DictionaryEntryCache<T> cache;
        private DictionaryEntries<T> entries;

        public Bound(ILowCardinalityColumn column, DictionaryEntryCache<T> cache)
        {
            this.column = column;
            this.cache = cache;
        }

        public override void Fill(int start, Span<T> destination)
            => DictionaryEntries<T>.Once(ref entries, cache, column, static (c, l) => c.For(l)).Fill(column.Keys.Slice(start, destination.Length), destination);
    }
}

/// <summary>
/// <c>LowCardinality(Nullable(X))</c> read as <c>T?</c>, for a value type <typeparamref name="T"/>: the dictionary is a
/// column of the bare <c>X</c>, so the child reads <typeparamref name="T"/>, and the entries are lifted to
/// <c>T?</c>. The NULL slot is not converted and gives null, and an entry that the child cannot convert fails a read
/// only at a row, as in <see cref="DictionaryReader{T}"/>. The reader keeps the converted entries of each column, as
/// <see cref="DictionaryReader{T}"/> does.
/// </summary>
/// <typeparam name="T">The CLR type that the child gives for one entry.</typeparam>
internal sealed class LiftingDictionaryReader<T> : ColumnReader<T?>
    where T : struct
{
    private static readonly MethodInfo EntriesMethod =
        typeof(DictionaryEntryCache<T?>).GetMethod(nameof(DictionaryEntryCache<T?>.For));

    private static readonly MethodInfo ValueAtMethod =
        typeof(DictionaryEntries<T?>).GetMethod(nameof(DictionaryEntries<T?>.ValueAt));

    private readonly DictionaryEntryCache<T?> entries;

    /// <summary>Initializes the reader over the reader of the dictionary type.</summary>
    /// <param name="inner">Reads the dictionary column.</param>
    public LiftingDictionaryReader(ColumnReader<T> inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        entries = new DictionaryEntryCache<T?>(column => DictionaryEntries<T?>.Convert(inner, column, Lift));
    }

    /// <inheritdoc/>
    public override BoundReader<T?> Bind(IColumn column) => new Bound(ColumnSurface.Of<ILowCardinalityColumn>(column), entries);

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression lowCardinality = scope.Local(typeof(ILowCardinalityColumn), "lowCardinality", EmitScope.Surface<ILowCardinalityColumn>(column));
        ParameterExpression converted = scope.Local(
            typeof(DictionaryEntries<T?>),
            "entries",
            Expression.Call(Expression.Constant(entries), EntriesMethod, lowCardinality));
        ParameterExpression keys = scope.Local(typeof(ReadOnlySpan<int>), "keys", Expression.Property(lowCardinality, nameof(ILowCardinalityColumn.Keys)));
        return Expression.Call(converted, ValueAtMethod, EmitScope.ElementAt(keys, row));
    }

    private static T?[] Lift(T[] values)
    {
        var lifted = new T?[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            lifted[i] = values[i];
        }

        return lifted;
    }

    private sealed class Bound : BoundReader<T?>, INullableBound
    {
        private readonly ILowCardinalityColumn column;
        private readonly DictionaryEntryCache<T?> cache;
        private DictionaryEntries<T?> entries;

        public Bound(ILowCardinalityColumn column, DictionaryEntryCache<T?> cache)
        {
            this.column = column;
            this.cache = cache;
        }

        public override void Fill(int start, Span<T?> destination)
            => DictionaryEntries<T?>.Once(ref entries, cache, column, static (c, l) => c.For(l)).Fill(column.Keys.Slice(start, destination.Length), destination);

        public int FindNull(int start, int length)
        {
            if (DictionarySlots.NullKey(column) < 0)
            {
                return -1;
            }

            int at = column.Keys.Slice(start, length).IndexOf(0);
            return at < 0 ? -1 : start + at;
        }
    }
}

/// <summary>The converted entries of one dictionary, and the failure of each entry that the child cannot convert.</summary>
/// <typeparam name="TEntry">The type of one entry.</typeparam>
internal sealed class DictionaryEntries<TEntry>
{
    private readonly TEntry[] entries;
    private readonly Exception[] failures;

    private DictionaryEntries(TEntry[] entries, Exception[] failures)
    {
        this.entries = entries;
        this.failures = failures;
    }

    /// <summary>
    /// Converts the entries of a dictionary with one bulk read. When that read fails, converts each entry alone and keeps
    /// the failure of each entry that fails, so that a read throws it only at a row that uses the entry. Never throws a
    /// failure of the child.
    /// </summary>
    /// <typeparam name="TInner">The type that the child gives for one entry.</typeparam>
    /// <param name="inner">The child reader.</param>
    /// <param name="column">The LowCardinality column.</param>
    /// <param name="wrap">Makes the entries from the values of the child (for example, lifts them to a nullable type).</param>
    /// <returns>The entries.</returns>
    public static DictionaryEntries<TEntry> Convert<TInner>(ColumnReader<TInner> inner, ILowCardinalityColumn column, Func<TInner[], TEntry[]> wrap)
    {
        IColumn dictionary = column.Dictionary;
        var values = new TInner[dictionary.RowCount];
        int nullKey = DictionarySlots.NullKey(column);
        int first = Math.Min(nullKey + 1, values.Length);
        if (first == values.Length)
        {
            return new(Null(wrap(values), nullKey), null);
        }

        BoundReader<TInner> bound = inner.Bind(dictionary);
        try
        {
            bound.Fill(first, values.AsSpan(first));
            return new(Null(wrap(values), nullKey), null);
        }
        catch (Exception)
        {
            var failures = new Exception[values.Length];
            for (int slot = first; slot < values.Length; slot++)
            {
                try
                {
                    bound.Fill(slot, values.AsSpan(slot, 1));
                }
                catch (Exception failure)
                {
                    failures[slot] = failure;
                    values[slot] = default;
                }
            }

            return new(Null(wrap(values), nullKey), failures);
        }
    }

    /// <summary>
    /// The entries of a bound column, made on the first call. Two threads that make them at the same time make equal
    /// entries, and both callers get the ones that are kept.
    /// </summary>
    /// <typeparam name="TReader">The reader that makes the entries.</typeparam>
    /// <param name="entries">The field that keeps the entries.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="column">The LowCardinality column.</param>
    /// <param name="convert">Makes the entries.</param>
    /// <returns>The entries.</returns>
    public static DictionaryEntries<TEntry> Once<TReader>(ref DictionaryEntries<TEntry> entries, TReader reader, ILowCardinalityColumn column, Func<TReader, ILowCardinalityColumn, DictionaryEntries<TEntry>> convert)
    {
        DictionaryEntries<TEntry> current = Volatile.Read(ref entries);
        if (current is not null)
        {
            return current;
        }

        DictionaryEntries<TEntry> made = convert(reader, column);
        return Interlocked.CompareExchange(ref entries, made, null) ?? made;
    }

    /// <summary>The value of a row with <paramref name="key"/>, or the failure of that entry.</summary>
    /// <param name="key">The key of the row.</param>
    /// <returns>The entry of the key.</returns>
    public TEntry ValueAt(int key) => failures is null ? entries[key] : Checked(key);

    /// <summary>Gives the values of the rows with <paramref name="keys"/>, or throws the failure of an entry that a row uses.</summary>
    /// <param name="keys">The keys of the rows.</param>
    /// <param name="destination">Receives the values.</param>
    public void Fill(ReadOnlySpan<int> keys, Span<TEntry> destination)
    {
        if (failures is null)
        {
            for (int i = 0; i < keys.Length; i++)
            {
                destination[i] = entries[keys[i]];
            }

            return;
        }

        for (int i = 0; i < keys.Length; i++)
        {
            destination[i] = Checked(keys[i]);
        }
    }

    // The NULL slot keeps default: the child does not convert it.
    private static TEntry[] Null(TEntry[] values, int nullKey)
    {
        if (nullKey >= 0 && values.Length > nullKey)
        {
            values[nullKey] = default;
        }

        return values;
    }

    // The NULL slot has no failure: the child does not convert it.
    private TEntry Checked(int key)
    {
        if (failures[key] is Exception failure)
        {
            ExceptionDispatchInfo.Throw(failure);
        }

        return entries[key];
    }
}

/// <summary>
/// The converted entries of each LowCardinality column that a dictionary reader reads, so the reads of one column
/// convert its dictionary once. A reader tree is cached and shared, so the cache is safe for concurrent use. It keys on
/// the column object and does not keep the column alive: an entry goes when its column is collected.
/// </summary>
/// <typeparam name="TEntry">The type of one entry.</typeparam>
internal sealed class DictionaryEntryCache<TEntry>
{
    private readonly ConditionalWeakTable<ILowCardinalityColumn, DictionaryEntries<TEntry>> converted = new();
    private readonly ConditionalWeakTable<ILowCardinalityColumn, DictionaryEntries<TEntry>>.CreateValueCallback convert;

    /// <summary>Initializes an empty cache.</summary>
    /// <param name="convert">Converts the entries of one column. It must not throw a failure of the child reader.</param>
    public DictionaryEntryCache(Func<ILowCardinalityColumn, DictionaryEntries<TEntry>> convert)
    {
        ArgumentNullException.ThrowIfNull(convert);
        this.convert = column => convert(column);
    }

    /// <summary>
    /// The entries of <paramref name="column"/>, converted on the first call for that column. Two threads that convert
    /// the same column at the same time make equal entries, and both get the ones that the cache keeps.
    /// </summary>
    /// <param name="column">The LowCardinality column.</param>
    /// <returns>The entries.</returns>
    public DictionaryEntries<TEntry> For(ILowCardinalityColumn column) => converted.GetValue(column, convert);
}

/// <summary>The reserved slots of a LowCardinality dictionary.</summary>
internal static class DictionarySlots
{
    /// <summary>The key of the NULL slot of a column, or -1 when the dictionary has none.</summary>
    /// <param name="column">The LowCardinality column.</param>
    /// <returns>The key.</returns>
    public static int NullKey(ILowCardinalityColumn column) => column.ReservedSlotCount == 2 ? 0 : -1;
}
