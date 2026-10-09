using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// A converter tree that reads the values of one ClickHouse type as one CLR type. A tree holds no column and no
/// per-block state, so one tree serves every block and every thread. <see cref="ConverterDerivation"/> builds and
/// caches the trees.
/// </summary>
/// <remarks>
/// A tree has two ways to read, and the two must give the same values (decision D4):
/// <list type="bullet">
/// <item><description><see cref="ColumnReader{T}.Bind"/>, then <see cref="BoundReader{T}.Fill"/>: a bulk read with no
/// dynamic code.</description></item>
/// <item><description><see cref="Emit"/>: an expression for one row, which a caller puts into its own compiled loop.
/// </description></item>
/// </list>
/// </remarks>
internal abstract class ColumnReader
{
    /// <summary>The CLR type that the tree gives for one row.</summary>
    public abstract Type ValueType { get; }

    /// <summary>
    /// Builds an expression for the value at <paramref name="row"/> of <paramref name="column"/>. The setup that does
    /// not change from row to row (casts, span locals, a converted dictionary) goes into <paramref name="scope"/>. The
    /// caller runs that setup one time, before its loop over the rows.
    /// </summary>
    /// <param name="column">An expression for the decoded column that the tree reads. Its type is <see cref="IColumn"/> or a more specific column type.</param>
    /// <param name="row">The zero-based row index. The result can use it more than one time.</param>
    /// <param name="scope">Collects the locals and the setup statements.</param>
    /// <returns>An expression of type <see cref="ValueType"/>.</returns>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public abstract Expression Emit(Expression column, ParameterExpression row, EmitScope scope);
}

/// <summary>A converter tree that reads one ClickHouse type as <typeparamref name="T"/>.</summary>
/// <typeparam name="T">The CLR type that the tree gives for one row.</typeparam>
internal abstract class ColumnReader<T> : ColumnReader
{
    /// <inheritdoc/>
    public sealed override Type ValueType => typeof(T);

    /// <summary>
    /// Pairs the tree with one decoded column. State that belongs to that column (for example a converted
    /// LowCardinality dictionary) lives in the result, not in the tree.
    /// </summary>
    /// <param name="column">A column that the codec of the tree's ClickHouse type decoded.</param>
    /// <returns>A reader for the rows of <paramref name="column"/>.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="column"/> does not have the decoded shape that the tree reads.</exception>
    public abstract BoundReader<T> Bind(IColumn column);
}

/// <summary>A converter tree paired with one decoded column.</summary>
/// <typeparam name="T">The CLR type that the reader gives for one row.</typeparam>
internal abstract class BoundReader<T>
{
    /// <summary>
    /// Converts the rows from <paramref name="start"/> into <paramref name="destination"/>: one row for each element,
    /// <c>destination.Length</c> rows in total.
    /// </summary>
    /// <param name="start">The zero-based first row.</param>
    /// <param name="destination">Receives the values.</param>
    /// <exception cref="ArgumentOutOfRangeException">The rows are not all in the column.</exception>
    public abstract void Fill(int start, Span<T> destination);
}
