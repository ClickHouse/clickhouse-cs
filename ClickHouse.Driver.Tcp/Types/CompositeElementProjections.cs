using System;
using System.Linq.Expressions;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>Builds array projections for composite codecs.</summary>
internal static class CompositeElementProjections
{
    /// <summary>Builds an expression that projects each source element into a new array.</summary>
    public static Expression ProjectArray(Expression source, ParameterExpression element, Expression elementProjection)
    {
        Type sourceElement = source.Type.GetElementType();
        if (element.Type != sourceElement)
        {
            throw new ArgumentException(
                $"The element variable is of type {element.Type}, but the source row is {source.Type}, whose elements are {sourceElement}.",
                nameof(element));
        }

        // Evaluate the source and its length once.
        ParameterExpression sourceRow = Expression.Variable(source.Type, "sourceRow");
        ParameterExpression length = Expression.Variable(typeof(int), "length");
        ParameterExpression targetRow = Expression.Variable(elementProjection.Type.MakeArrayType(), "targetRow");
        ParameterExpression index = Expression.Variable(typeof(int), "index");
        LabelTarget done = Expression.Label("done");

        return Expression.Block(
            new[] { sourceRow, length, targetRow, index },
            Expression.Assign(sourceRow, source),
            Expression.Assign(length, Expression.ArrayLength(sourceRow)),
            Expression.Assign(targetRow, Expression.NewArrayBounds(elementProjection.Type, length)),
            Expression.Assign(index, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(index, length),
                    Expression.Block(
                        new[] { element },
                        Expression.Assign(element, Expression.ArrayIndex(sourceRow, index)),
                        Expression.Assign(Expression.ArrayAccess(targetRow, index), elementProjection),
                        Expression.PostIncrementAssign(index)),
                    Expression.Break(done)),
                done),

            targetRow);
    }

    /// <summary>Gets the element type when <paramref name="candidate"/> is <c>T[]</c>.</summary>
    public static bool TryGetArrayElement(Type candidate, out Type elementType)
    {
        if (candidate.IsSZArray)
        {
            elementType = candidate.GetElementType();
            return true;
        }

        elementType = null;
        return false;
    }
}
