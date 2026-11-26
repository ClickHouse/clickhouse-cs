using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// Collects the locals and the setup statements of one <see cref="ColumnReader.Emit"/> call. The caller declares
/// <see cref="Locals"/> in its block and runs <see cref="Setup"/> one time, before its loop over the rows. One scope
/// serves one compiled loop, on one thread.
/// </summary>
internal sealed class EmitScope
{
    private static readonly MethodInfo ElementAtMethod =
        typeof(EmitScope).GetMethod(nameof(ElementAtCore), BindingFlags.NonPublic | BindingFlags.Static);

    private readonly List<ParameterExpression> locals = new();

    private readonly List<Expression> setup = new();

    /// <summary>The locals that <see cref="Setup"/> assigns.</summary>
    public IReadOnlyList<ParameterExpression> Locals => locals;

    /// <summary>The setup statements, in the order that they must run.</summary>
    public IReadOnlyList<Expression> Setup => setup;

    /// <summary>Declares a local and adds the statement that assigns <paramref name="value"/> to it.</summary>
    /// <param name="type">The type of the local.</param>
    /// <param name="name">A name for the local. The scope adds a number to make it unique.</param>
    /// <param name="value">The value of the local, evaluated one time in the setup.</param>
    /// <returns>The local, for use in the setup that follows and in the expression for one row.</returns>
    public ParameterExpression Local(Type type, string name, Expression value)
    {
        ParameterExpression local = Expression.Variable(type, name + locals.Count.ToString(CultureInfo.InvariantCulture));
        locals.Add(local);
        setup.Add(Expression.Assign(local, value));
        return local;
    }

    /// <summary>
    /// An expression that reads one element of a span by value. The indexer of a span returns a reference, which an
    /// expression tree cannot represent.
    /// </summary>
    /// <param name="span">An expression of type <see cref="ReadOnlySpan{T}"/>.</param>
    /// <param name="index">An expression of type <see cref="int"/>.</param>
    /// <returns>An expression of the element type of <paramref name="span"/>.</returns>
    [RequiresDynamicCode("Builds a generic method for the element type of the span.")]
    public static Expression ElementAt(Expression span, Expression index)
    {
        Type type = span.Type;
        if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(ReadOnlySpan<>))
        {
            throw new ArgumentException($"Expected an expression of type ReadOnlySpan<T>, not {type}.", nameof(span));
        }

        return Expression.Call(ElementAtMethod.MakeGenericMethod(type.GenericTypeArguments[0]), span, index);
    }

    /// <summary>
    /// An expression for <paramref name="column"/> as <typeparamref name="TSurface"/>. When the static type of the
    /// expression does not show that surface, the expression checks at run time and throws the message of
    /// <see cref="ColumnSurface.Of{TSurface}"/>.
    /// </summary>
    /// <typeparam name="TSurface">The column interface or class that the reader needs.</typeparam>
    /// <param name="column">An expression for a column.</param>
    /// <returns>An expression of type <typeparamref name="TSurface"/>.</returns>
    public static Expression Surface<TSurface>(Expression column)
        where TSurface : class
    {
        if (column.Type == typeof(TSurface))
        {
            return column;
        }

        return typeof(TSurface).IsAssignableFrom(column.Type)
            ? Expression.Convert(column, typeof(TSurface))
            : Expression.Call(ColumnSurface.OfMethod<TSurface>(), Expression.Convert(column, typeof(IColumn)));
    }

    private static T ElementAtCore<T>(ReadOnlySpan<T> span, int index) => span[index];
}
