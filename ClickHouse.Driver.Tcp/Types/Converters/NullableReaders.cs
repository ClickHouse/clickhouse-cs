using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>A bound reader whose rows can hold NULL. It finds the first NULL row of a range without a conversion.</summary>
internal interface INullableBound
{
    /// <summary>The first row of <c>[start, start + length)</c> that holds NULL.</summary>
    /// <param name="start">The zero-based first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <returns>The row, or -1 when no row of the range holds NULL.</returns>
    int FindNull(int start, int length);
}

/// <summary>
/// <c>Nullable(X)</c> read as <c>T?</c>, for a value type <typeparamref name="T"/>. A NULL row gives null. The child
/// reads only the rows that are not NULL, because the inner column holds a placeholder under a NULL, and a child
/// conversion can refuse that placeholder.
/// </summary>
/// <typeparam name="T">The CLR type that the child gives for a row that is not NULL.</typeparam>
internal sealed class NullableValueReader<T> : ColumnReader<T?>
    where T : struct
{
    private readonly ColumnReader<T> inner;

    /// <summary>Initializes the lift over the reader of the inner type.</summary>
    /// <param name="inner">Reads the inner column of the <c>Nullable</c> column.</param>
    public NullableValueReader(ColumnReader<T> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<T?> Bind(IColumn column)
    {
        INullableColumn nullable = ColumnSurface.Of<INullableColumn>(column);
        return new Bound(nullable, inner.Bind(nullable.Inner));
    }

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => NullableEmit.Emit(inner, column, row, scope, typeof(T?));

    private sealed class Bound : BoundReader<T?>, INullableBound
    {
        private readonly INullableColumn column;
        private readonly BoundReader<T> inner;

        public Bound(INullableColumn column, BoundReader<T> inner)
        {
            this.column = column;
            this.inner = inner;
        }

        public override void Fill(int start, Span<T?> destination)
        {
            ReadOnlySpan<byte> nulls = column.NullMap.Slice(start, destination.Length);
            if (destination.IsEmpty)
            {
                return;
            }

            T[] scratch = ArrayPool<T>.Shared.Rent(destination.Length);
            try
            {
                Span<T> values = scratch.AsSpan(0, destination.Length);
                NullRuns.FillPresent(inner, start, nulls, values);
                for (int i = 0; i < nulls.Length; i++)
                {
                    destination[i] = nulls[i] != 0 ? null : values[i];
                }
            }
            finally
            {
                ArrayPool<T>.Shared.Return(scratch, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
            }
        }

        public int FindNull(int start, int length) => NullRuns.FindNull(column.NullMap, start, length);
    }
}

/// <summary>
/// <c>Nullable(X)</c> read as a reference type <typeparamref name="T"/>. A NULL row gives null. The child reads only the
/// rows that are not NULL (see <see cref="NullableValueReader{T}"/>).
/// </summary>
/// <typeparam name="T">The CLR type that the child gives, which also holds the NULL rows.</typeparam>
internal sealed class NullableReferenceReader<T> : ColumnReader<T>
    where T : class
{
    private readonly ColumnReader<T> inner;

    /// <summary>Initializes the lift over the reader of the inner type.</summary>
    /// <param name="inner">Reads the inner column of the <c>Nullable</c> column.</param>
    public NullableReferenceReader(ColumnReader<T> inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public override BoundReader<T> Bind(IColumn column)
    {
        INullableColumn nullable = ColumnSurface.Of<INullableColumn>(column);
        return new Bound(nullable, inner.Bind(nullable.Inner));
    }

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => NullableEmit.Emit(inner, column, row, scope, typeof(T));

    private sealed class Bound : BoundReader<T>, INullableBound
    {
        private readonly INullableColumn column;
        private readonly BoundReader<T> inner;

        public Bound(INullableColumn column, BoundReader<T> inner)
        {
            this.column = column;
            this.inner = inner;
        }

        public override void Fill(int start, Span<T> destination)
        {
            ReadOnlySpan<byte> nulls = column.NullMap.Slice(start, destination.Length);
            NullRuns.FillPresent(inner, start, nulls, destination);
            for (int i = 0; i < nulls.Length; i++)
            {
                if (nulls[i] != 0)
                {
                    destination[i] = null;
                }
            }
        }

        public int FindNull(int start, int length) => NullRuns.FindNull(column.NullMap, start, length);
    }
}

/// <summary>The loops over a null map that the <c>Nullable</c> readers share.</summary>
internal static class NullRuns
{
    /// <summary>
    /// Fills each run of rows that are not NULL through <paramref name="inner"/>. The positions of the NULL rows in
    /// <paramref name="values"/> keep what they held.
    /// </summary>
    /// <typeparam name="T">The CLR type of a value.</typeparam>
    /// <param name="inner">The bound reader of the inner column.</param>
    /// <param name="start">The row of the inner column that <c>values[0]</c> receives.</param>
    /// <param name="nulls">The null map of the rows, one byte for each value; not zero for a NULL.</param>
    /// <param name="values">Receives the values of the rows that are not NULL.</param>
    public static void FillPresent<T>(BoundReader<T> inner, int start, ReadOnlySpan<byte> nulls, Span<T> values)
    {
        int position = 0;
        while (position < nulls.Length)
        {
            int nullAt = nulls.Slice(position).IndexOfAnyExcept((byte)0);
            int runEnd = nullAt < 0 ? nulls.Length : position + nullAt;
            if (runEnd > position)
            {
                inner.Fill(start + position, values.Slice(position, runEnd - position));
            }

            if (runEnd == nulls.Length)
            {
                return;
            }

            int presentAt = nulls.Slice(runEnd).IndexOf((byte)0);
            if (presentAt < 0)
            {
                return;
            }

            position = runEnd + presentAt;
        }
    }

    /// <summary>The first NULL row of a range of a null map, or -1.</summary>
    /// <param name="nulls">The whole null map of the column.</param>
    /// <param name="start">The zero-based first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <returns>The row, or -1 when no row of the range holds NULL.</returns>
    public static int FindNull(ReadOnlySpan<byte> nulls, int start, int length)
    {
        int at = nulls.Slice(start, length).IndexOfAnyExcept((byte)0);
        return at < 0 ? -1 : start + at;
    }
}

/// <summary>The <see cref="ColumnReader.Emit"/> of both <c>Nullable</c> readers.</summary>
internal static class NullableEmit
{
    /// <summary>
    /// Builds <c>nulls[row] != 0 ? default : child</c>. The conditional evaluates the child only for a row that is not
    /// NULL.
    /// </summary>
    /// <param name="inner">The reader of the inner column.</param>
    /// <param name="column">An expression for the <c>Nullable</c> column.</param>
    /// <param name="row">The row index.</param>
    /// <param name="scope">Collects the setup.</param>
    /// <param name="surface">The type of the result: <c>T?</c> or the reference type.</param>
    /// <returns>An expression of type <paramref name="surface"/>.</returns>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public static Expression Emit(ColumnReader inner, Expression column, ParameterExpression row, EmitScope scope, Type surface)
    {
        ParameterExpression nullable = scope.Local(typeof(INullableColumn), "nullable", EmitScope.Surface<INullableColumn>(column));
        ParameterExpression nulls = scope.Local(typeof(ReadOnlySpan<byte>), "nulls", Expression.Property(nullable, nameof(INullableColumn.NullMap)));
        ParameterExpression innerColumn = scope.Local(typeof(IColumn), "inner", Expression.Property(nullable, nameof(INullableColumn.Inner)));
        Expression value = inner.Emit(innerColumn, row, scope);
        return Expression.Condition(
            Expression.NotEqual(EmitScope.ElementAt(nulls, row), Expression.Constant((byte)0)),
            Expression.Default(surface),
            value.Type == surface ? value : Expression.Convert(value, surface));
    }
}
