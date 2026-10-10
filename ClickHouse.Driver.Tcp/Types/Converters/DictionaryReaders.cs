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
/// when it reaches a row that is not NULL, as <see cref="DictionaryOrder"/> says.
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
    /// <param name="order">Which failure a read throws when an entry cannot be converted.</param>
    public DictionaryReader(ColumnReader<T> inner, DictionaryOrder order)
    {
        ArgumentNullException.ThrowIfNull(inner);
        entries = new DictionaryEntryCache<T>(column => DictionaryEntries<T>.Convert(inner, column, order, static values => values));
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
    /// <param name="order">Which failure a read throws when an entry cannot be converted.</param>
    public LiftingDictionaryReader(ColumnReader<T> inner, DictionaryOrder order)
    {
        ArgumentNullException.ThrowIfNull(inner);
        entries = new DictionaryEntryCache<T?>(column => DictionaryEntries<T?>.Convert(inner, column, order, Lift));
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

/// <summary>Which failure a dictionary read throws when the child cannot convert an entry.</summary>
internal enum DictionaryOrder
{
    /// <summary>
    /// The first row that is not NULL throws the failure of the first entry, in slot order, that cannot be converted, also
    /// when no row refers to it. This is the order of a reading of the whole column, which converts every entry before
    /// it gives the first value.
    /// </summary>
    FirstEntry,

    /// <summary>
    /// A row throws the failure of its own entry, so a read fails at the first row, in row order, whose entry cannot be
    /// converted. This is the order of a reading that converts each value alone, as the read rules of D6 do.
    /// </summary>
    Row,
}

/// <summary>The converted entries of one dictionary, and the failure of each entry that the child cannot convert.</summary>
/// <typeparam name="TEntry">The type of one entry.</typeparam>
internal sealed class DictionaryEntries<TEntry>
{
    private readonly TEntry[] entries;
    private readonly Exception[] failures;
    private readonly int firstFailure;
    private readonly int nullKey;
    private readonly DictionaryOrder order;

    private DictionaryEntries(TEntry[] entries, Exception[] failures, int firstFailure, int nullKey, DictionaryOrder order)
    {
        this.entries = entries;
        this.failures = failures;
        this.firstFailure = firstFailure;
        this.nullKey = nullKey;
        this.order = order;
    }

    /// <summary>
    /// Converts the entries of a dictionary with one bulk read. When that read fails, converts each entry alone and keeps
    /// the failure of each entry that fails. Never throws a failure of the child.
    /// </summary>
    /// <typeparam name="TInner">The type that the child gives for one entry.</typeparam>
    /// <param name="inner">The child reader.</param>
    /// <param name="column">The LowCardinality column.</param>
    /// <param name="order">Which failure a read throws.</param>
    /// <param name="wrap">Makes the entries from the values of the child (for example, lifts them to a nullable type).</param>
    /// <returns>The entries.</returns>
    public static DictionaryEntries<TEntry> Convert<TInner>(ColumnReader<TInner> inner, ILowCardinalityColumn column, DictionaryOrder order, Func<TInner[], TEntry[]> wrap)
    {
        IColumn dictionary = column.Dictionary;
        var values = new TInner[dictionary.RowCount];
        int nullKey = DictionarySlots.NullKey(column);
        int first = Math.Min(nullKey + 1, values.Length);
        if (first == values.Length)
        {
            return new(wrap(values), null, -1, nullKey, order);
        }

        BoundReader<TInner> bound = inner.Bind(dictionary);
        try
        {
            bound.Fill(first, values.AsSpan(first));
            return new(Null(wrap(values), nullKey), null, -1, nullKey, order);
        }
        catch (Exception)
        {
            var failures = new Exception[values.Length];
            int firstFailure = -1;
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
                    firstFailure = firstFailure < 0 ? slot : firstFailure;
                }
            }

            return new(Null(wrap(values), nullKey), firstFailure < 0 ? null : failures, firstFailure, nullKey, order);
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

    /// <summary>The value of a row with <paramref name="key"/>, or the failure of the dictionary order.</summary>
    /// <param name="key">The key of the row.</param>
    /// <returns>The entry of the key.</returns>
    public TEntry ValueAt(int key) => failures is null ? entries[key] : Checked(key);

    /// <summary>Gives the values of the rows with <paramref name="keys"/>, in row order, with the failures of the dictionary order.</summary>
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

    private TEntry Checked(int key)
    {
        if (key == nullKey)
        {
            return entries[key];
        }

        Exception failure = order == DictionaryOrder.Row ? failures[key] : failures[firstFailure];
        if (failure is not null)
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
