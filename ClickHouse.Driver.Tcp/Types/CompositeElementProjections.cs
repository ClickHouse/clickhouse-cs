using System;
using System.Linq.Expressions;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The structural half of a container codec's projections: rebuilding one row's array with every element passed
/// through a child codec's own projection. <c>Array</c> and <c>Map</c> both reduce to this — a map row is an array of
/// pairs, so the caller's element projection builds the new pair and this loop does the rest.
///
/// <para>
/// The caller resolves the child projection first and passes it in already built, rather than handing over a callback.
/// That is what lets a caller refuse before any tree is built: a child that offers no projection means the container
/// offers none either, and there is nothing to unwind.
/// </para>
/// </summary>
internal static class CompositeElementProjections
{
    /// <summary>
    /// Builds an expression that turns one row's array into a new array whose elements are
    /// <paramref name="elementProjection"/> applied to each of the source's.
    ///
    /// <para>
    /// This allocates one array per row, which is inherent rather than a shortcut: the surface holds
    /// <c>TSource[]</c> and the caller asked for <c>TTarget[]</c>, so the elements cannot be converted in place. The
    /// identity case never reaches here — a container returns the source expression untouched when the target is its
    /// own element type — so a projection that costs nothing also allocates nothing.
    /// </para>
    /// </summary>
    /// <param name="source">An expression yielding one row's array. Evaluated exactly once.</param>
    /// <param name="element">The variable <paramref name="elementProjection"/> reads one source element from; its
    /// type must be <paramref name="source"/>'s element type. Declared and assigned by the loop this builds, so the
    /// projection sees each element bound to a local rather than re-indexing the array.</param>
    /// <param name="elementProjection">The child codec's projection of <paramref name="element"/>. Its type becomes
    /// the result array's element type.</param>
    /// <returns>An expression of type <c>elementProjection.Type[]</c>.</returns>
    public static Expression ProjectArray(Expression source, ParameterExpression element, Expression elementProjection)
    {
        Type sourceElement = source.Type.GetElementType();
        if (element.Type != sourceElement)
        {
            throw new ArgumentException(
                $"The element variable is of type {element.Type}, but the source row is {source.Type}, whose elements are {sourceElement}.",
                nameof(element));
        }

        // Bound to locals: the source may be an arbitrary expression that must not run twice, and the length is read
        // by both the allocation and every iteration of the test.
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

            // The block's value is the built array, so the whole projection is one expression a caller can inline.
            targetRow);
    }

    /// <summary>
    /// Whether <paramref name="candidate"/> is a zero-based vector (<c>T[]</c>), and if so its element type. A
    /// container's surface is always exactly that, so a multi-dimension target is refused rather than being mistaken
    /// for one.
    ///
    /// <para>
    /// <see cref="Type.IsSZArray"/>, not <c>IsArray</c> plus a rank check: <c>Type.MakeArrayType(1)</c> builds the
    /// non-zero-based <c>T[*]</c>, which has rank one and the right element type but is a distinct type. Accepting it
    /// would break both directions — a read projection builds a zero-based array, so it would return a type the caller
    /// did not ask for, and a write shape casts the column to <c>IColumn&lt;T[]&gt;</c>, so a plan-build refusal would
    /// become a cast failure with the insert already open.
    /// </para>
    /// </summary>
    /// <param name="candidate">The type to test.</param>
    /// <param name="elementType">The element type, or null when <paramref name="candidate"/> is not such an array.</param>
    /// <returns>Whether <paramref name="candidate"/> is a zero-based vector.</returns>
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
