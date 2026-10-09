using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>LowCardinality(X)</c> read as <typeparamref name="T"/>, and <c>LowCardinality(Nullable(X))</c> read as a
/// reference type. The child converts each dictionary entry one time, on the first read of a bound column. Each row
/// then gives the entry of its key, so the rows that share a key share one value.
/// </summary>
/// <remarks>
/// The dictionary of <c>LowCardinality(Nullable(X))</c> is a column of the bare <c>X</c>, and its slot 0 is the NULL
/// slot (<see cref="ILowCardinalityColumn.ReservedSlotCount"/> is 2). The child does not convert that slot, because it
/// holds a placeholder, which a child conversion can refuse. Its entry is <c>default</c>, so a NULL row gives null.
/// </remarks>
/// <typeparam name="T">The CLR type of one value.</typeparam>
internal sealed class DictionaryReader<T> : ColumnReader<T>
{
    private static readonly MethodInfo EntriesMethod =
        typeof(DictionaryReader<T>).GetMethod(nameof(Entries), BindingFlags.NonPublic | BindingFlags.Static);

    private readonly ColumnReader<T> inner;

    /// <summary>Initializes the reader over the reader of the dictionary type.</summary>
    /// <param name="inner">Reads the dictionary column.</param>
    public DictionaryReader(ColumnReader<T> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column) => new Bound(ColumnSurface.Of<ILowCardinalityColumn>(column), inner);

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression lowCardinality = scope.Local(typeof(ILowCardinalityColumn), "lowCardinality", EmitScope.Surface<ILowCardinalityColumn>(column));
        ParameterExpression entries = scope.Local(
            typeof(T[]),
            "entries",
            Expression.Call(EntriesMethod, Expression.Constant(inner, typeof(ColumnReader<T>)), lowCardinality));
        ParameterExpression keys = scope.Local(typeof(ReadOnlySpan<int>), "keys", Expression.Property(lowCardinality, nameof(ILowCardinalityColumn.Keys)));
        return Expression.ArrayIndex(entries, EmitScope.ElementAt(keys, row));
    }

    // Converts every entry of the dictionary except the NULL slot, which keeps default.
    private static T[] Entries(ColumnReader<T> inner, ILowCardinalityColumn column)
    {
        IColumn dictionary = column.Dictionary;
        var entries = new T[dictionary.RowCount];
        int first = Math.Min(DictionarySlots.FirstConverted(column), entries.Length);
        if (first < entries.Length)
        {
            inner.Bind(dictionary).Fill(first, entries.AsSpan(first));
        }

        return entries;
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly ILowCardinalityColumn column;
        private readonly ColumnReader<T> inner;
        private T[] entries;

        public Bound(ILowCardinalityColumn column, ColumnReader<T> inner)
        {
            this.column = column;
            this.inner = inner;
        }

        public override void Fill(int start, Span<T> destination)
        {
            ReadOnlySpan<int> keys = column.Keys.Slice(start, destination.Length);
            T[] converted = DictionarySlots.Once(ref entries, inner, column, Entries);
            for (int i = 0; i < keys.Length; i++)
            {
                destination[i] = converted[keys[i]];
            }
        }
    }
}

/// <summary>
/// <c>LowCardinality(Nullable(X))</c> read as <c>T?</c>, for a value type <typeparamref name="T"/>: the dictionary is a
/// column of the bare <c>X</c>, so the child reads <typeparamref name="T"/>, and the entries are lifted to
/// <c>T?</c>. The NULL slot is not converted and gives null (see <see cref="DictionaryReader{T}"/>).
/// </summary>
/// <typeparam name="T">The CLR type that the child gives for one entry.</typeparam>
internal sealed class LiftingDictionaryReader<T> : ColumnReader<T?>
    where T : struct
{
    private static readonly MethodInfo EntriesMethod =
        typeof(LiftingDictionaryReader<T>).GetMethod(nameof(Entries), BindingFlags.NonPublic | BindingFlags.Static);

    private readonly ColumnReader<T> inner;

    /// <summary>Initializes the reader over the reader of the dictionary type.</summary>
    /// <param name="inner">Reads the dictionary column.</param>
    public LiftingDictionaryReader(ColumnReader<T> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<T?> Bind(IColumn column) => new Bound(ColumnSurface.Of<ILowCardinalityColumn>(column), inner);

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression lowCardinality = scope.Local(typeof(ILowCardinalityColumn), "lowCardinality", EmitScope.Surface<ILowCardinalityColumn>(column));
        ParameterExpression entries = scope.Local(
            typeof(T?[]),
            "entries",
            Expression.Call(EntriesMethod, Expression.Constant(inner, typeof(ColumnReader<T>)), lowCardinality));
        ParameterExpression keys = scope.Local(typeof(ReadOnlySpan<int>), "keys", Expression.Property(lowCardinality, nameof(ILowCardinalityColumn.Keys)));
        return Expression.ArrayIndex(entries, EmitScope.ElementAt(keys, row));
    }

    private static T?[] Entries(ColumnReader<T> inner, ILowCardinalityColumn column)
    {
        IColumn dictionary = column.Dictionary;
        var entries = new T?[dictionary.RowCount];
        int first = Math.Min(DictionarySlots.FirstConverted(column), entries.Length);
        if (first < entries.Length)
        {
            var values = new T[entries.Length - first];
            inner.Bind(dictionary).Fill(first, values);
            for (int i = 0; i < values.Length; i++)
            {
                entries[first + i] = values[i];
            }
        }

        return entries;
    }

    private sealed class Bound : BoundReader<T?>, INullableBound
    {
        private readonly ILowCardinalityColumn column;
        private readonly ColumnReader<T> inner;
        private T?[] entries;

        public Bound(ILowCardinalityColumn column, ColumnReader<T> inner)
        {
            this.column = column;
            this.inner = inner;
        }

        public override void Fill(int start, Span<T?> destination)
        {
            ReadOnlySpan<int> keys = column.Keys.Slice(start, destination.Length);
            T?[] converted = DictionarySlots.Once(ref entries, inner, column, Entries);
            for (int i = 0; i < keys.Length; i++)
            {
                destination[i] = converted[keys[i]];
            }
        }

        public int FindNull(int start, int length)
        {
            if (DictionarySlots.FirstConverted(column) == 0)
            {
                return -1;
            }

            int at = column.Keys.Slice(start, length).IndexOf(0);
            return at < 0 ? -1 : start + at;
        }
    }
}

/// <summary>The rules of the reserved dictionary slots, which the dictionary readers share.</summary>
internal static class DictionarySlots
{
    /// <summary>The first dictionary slot that holds a value: 1 when slot 0 is the NULL slot, else 0.</summary>
    /// <param name="column">The LowCardinality column.</param>
    /// <returns>The slot.</returns>
    public static int FirstConverted(ILowCardinalityColumn column) => column.ReservedSlotCount == 2 ? 1 : 0;

    /// <summary>
    /// The converted entries of a bound column, made on the first call. Two threads that make them at the same time
    /// make equal arrays, and both callers get the one that is kept.
    /// </summary>
    /// <typeparam name="TEntry">The type of one entry.</typeparam>
    /// <typeparam name="TInner">The type that the child reader gives.</typeparam>
    /// <param name="entries">The field that keeps the entries.</param>
    /// <param name="inner">The child reader.</param>
    /// <param name="column">The LowCardinality column.</param>
    /// <param name="convert">Makes the entries.</param>
    /// <returns>The entries.</returns>
    public static TEntry[] Once<TEntry, TInner>(ref TEntry[] entries, ColumnReader<TInner> inner, ILowCardinalityColumn column, Func<ColumnReader<TInner>, ILowCardinalityColumn, TEntry[]> convert)
    {
        TEntry[] current = Volatile.Read(ref entries);
        if (current is not null)
        {
            return current;
        }

        TEntry[] made = convert(inner, column);
        return Interlocked.CompareExchange(ref entries, made, null) ?? made;
    }
}
