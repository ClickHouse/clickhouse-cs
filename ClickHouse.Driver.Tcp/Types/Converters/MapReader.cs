using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Map(K, V)</c> read as <c>KeyValuePair&lt;TKey, TValue&gt;[]</c>. The key and the value child each read their
/// flat entry column, and each row gets a new array of pairs. An empty row gives <see cref="Array.Empty{T}"/>.
/// </summary>
/// <typeparam name="TKey">The CLR type of one key.</typeparam>
/// <typeparam name="TValue">The CLR type of one value.</typeparam>
internal sealed class MapReader<TKey, TValue> : ColumnReader<KeyValuePair<TKey, TValue>[]>
{
    private static readonly MethodInfo RowAtMethod =
        typeof(MapReader<TKey, TValue>).GetMethod(nameof(RowAt), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo BindKeyMethod = typeof(ColumnReader<TKey>).GetMethod(nameof(ColumnReader<TKey>.Bind));

    private static readonly MethodInfo BindValueMethod = typeof(ColumnReader<TValue>).GetMethod(nameof(ColumnReader<TValue>.Bind));

    private readonly ColumnReader<TKey> key;
    private readonly ColumnReader<TValue> value;

    /// <summary>Initializes the reader over the readers of the key type and the value type.</summary>
    /// <param name="key">Reads the flat key column.</param>
    /// <param name="value">Reads the flat value column.</param>
    public MapReader(ColumnReader<TKey> key, ColumnReader<TValue> value)
    {
        this.key = key ?? throw new ArgumentNullException(nameof(key));
        this.value = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <inheritdoc/>
    public override BoundReader<KeyValuePair<TKey, TValue>[]> Bind(IColumn column)
    {
        IMapColumn map = ColumnSurface.Of<IMapColumn>(column);
        return new Bound(map, key.Bind(map.KeyColumn), value.Bind(map.ValueColumn));
    }

    /// <inheritdoc/>
    // The setup binds the children to the flat entry columns, and each row is one bulk read of each child.
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression map = scope.Local(typeof(IMapColumn), "map", EmitScope.Surface<IMapColumn>(column));
        ParameterExpression keys = scope.Local(
            typeof(BoundReader<TKey>),
            "keys",
            Expression.Call(Expression.Constant(key, typeof(ColumnReader<TKey>)), BindKeyMethod, Expression.Property(map, nameof(IMapColumn.KeyColumn))));
        ParameterExpression values = scope.Local(
            typeof(BoundReader<TValue>),
            "values",
            Expression.Call(Expression.Constant(value, typeof(ColumnReader<TValue>)), BindValueMethod, Expression.Property(map, nameof(IMapColumn.ValueColumn))));
        ParameterExpression offsets = scope.Local(typeof(ReadOnlySpan<int>), "offsets", Expression.Property(map, nameof(IMapColumn.Offsets)));
        return Expression.Call(RowAtMethod, keys, values, offsets, row);
    }

    // The offsets span has one more entry than the column has rows: row r holds [offsets[r], offsets[r + 1]).
    private static KeyValuePair<TKey, TValue>[] RowAt(BoundReader<TKey> keys, BoundReader<TValue> values, ReadOnlySpan<int> offsets, int row)
    {
        int from = offsets[row];
        int length = offsets[row + 1] - from;
        if (length == 0)
        {
            return Array.Empty<KeyValuePair<TKey, TValue>>();
        }

        TKey[] keyScratch = ArrayPool<TKey>.Shared.Rent(length);
        TValue[] valueScratch = ArrayPool<TValue>.Shared.Rent(length);
        try
        {
            keys.Fill(from, keyScratch.AsSpan(0, length));
            values.Fill(from, valueScratch.AsSpan(0, length));
            return Pairs(keyScratch, valueScratch, 0, length);
        }
        finally
        {
            ArrayPool<TKey>.Shared.Return(keyScratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
            ArrayPool<TValue>.Shared.Return(valueScratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TValue>());
        }
    }

    private static KeyValuePair<TKey, TValue>[] Pairs(TKey[] keys, TValue[] values, int from, int length)
    {
        if (length == 0)
        {
            return Array.Empty<KeyValuePair<TKey, TValue>>();
        }

        var pairs = new KeyValuePair<TKey, TValue>[length];
        for (int i = 0; i < length; i++)
        {
            pairs[i] = new KeyValuePair<TKey, TValue>(keys[from + i], values[from + i]);
        }

        return pairs;
    }

    private sealed class Bound : BoundReader<KeyValuePair<TKey, TValue>[]>
    {
        private readonly IMapColumn column;
        private readonly BoundReader<TKey> keys;
        private readonly BoundReader<TValue> values;

        public Bound(IMapColumn column, BoundReader<TKey> keys, BoundReader<TValue> values)
        {
            this.column = column;
            this.keys = keys;
            this.values = values;
        }

        // Reads the entries of all the rows with one bulk read of each child, then pairs them row by row.
        public override void Fill(int start, Span<KeyValuePair<TKey, TValue>[]> destination)
        {
            ReadOnlySpan<int> offsets = column.Offsets.Slice(start, destination.Length + 1);
            int first = offsets[0];
            int count = offsets[destination.Length] - first;
            TKey[] keyScratch = ArrayPool<TKey>.Shared.Rent(count);
            TValue[] valueScratch = ArrayPool<TValue>.Shared.Rent(count);
            try
            {
                keys.Fill(first, keyScratch.AsSpan(0, count));
                values.Fill(first, valueScratch.AsSpan(0, count));
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = Pairs(keyScratch, valueScratch, offsets[i] - first, offsets[i + 1] - offsets[i]);
                }
            }
            finally
            {
                ArrayPool<TKey>.Shared.Return(keyScratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
                ArrayPool<TValue>.Shared.Return(valueScratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TValue>());
            }
        }
    }
}
