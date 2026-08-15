using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Resolves how one decoded column value becomes one POCO property value, as an <see cref="Expression"/> the
/// scatter builder inlines into its per-row loop. Resolution happens once per (column, property) pair at plan
/// build, so a shape that cannot work fails before the first row rather than mid-stream.
///
/// <para>
/// The rules, first match winning:
/// <list type="number">
/// <item>the property type is the column's element type — the identity;</item>
/// <item>the codec advertises the property type in <see cref="IColumnCodec.ReadableElementTypes"/> — its own
/// projection is inlined, which is what reads a <c>DateTime</c> column into a <see cref="DateTime"/> property
/// rather than into the raw epoch seconds;</item>
/// <item>a <c>Nullable(T)</c> column into a property that cannot hold null — projected through the nullable
/// surface, then required to have a value per row (D6a: a NULL throws, naming the row);</item>
/// <item>a nullable property — the underlying types are matched by these same rules, and null stays null;</item>
/// <item>a CLR <c>enum</c> property over the column's own ordinal type — a blind unchecked cast (D6b);</item>
/// <item>a property type the element type is assignable to (<see cref="object"/>, or an interface the element
/// type implements).</item>
/// </list>
/// </para>
///
/// <para>
/// Numeric widening is deliberately absent (D6c): an <c>Int32</c> column does not fill a <see cref="long"/>
/// property. Reflection assignability admits no numeric conversion, so the rules decline it by construction rather
/// than by a special case.
/// </para>
///
/// <para>
/// One consequence of the last rule's position: an <see cref="object"/> property is assignable from any element type,
/// so it takes the <em>raw</em> value rather than a projection — a <c>DateTime</c> column fills it with boxed epoch
/// seconds, the same value the untyped <c>object[]</c> read produces. Declare the property as the type you want the
/// reading in.
/// </para>
/// </summary>
internal static class PocoValueProjection
{
    private static readonly MethodInfo NullNotAssignableMethod =
        typeof(PocoValueProjection).GetMethod(nameof(NullNotAssignable), BindingFlags.Public | BindingFlags.Static);

    /// <summary>
    /// Builds the projection from one decoded value to one property value.
    /// </summary>
    /// <param name="codec">The column's codec, consulted for the conversions it owns. Only ever asked to project
    /// <paramref name="value"/> itself, so the expression it receives is always of its element type.</param>
    /// <param name="value">An expression of the codec's element type yielding one decoded value. Some rules
    /// reference it twice, so the caller must pass a variable (or another repeatable expression).</param>
    /// <param name="target">The property type to produce.</param>
    /// <param name="site">The column/property names a failed projection reports at runtime.</param>
    /// <param name="projected">An expression of type <paramref name="target"/>, or null when no rule applies.</param>
    /// <returns>Whether a rule applied.</returns>
    public static bool TryResolve(IColumnCodec codec, Expression value, Type target, PocoProjectionSite site, out Expression projected)
    {
        Type elementType = value.Type;
        projected = null;

        if (target == elementType)
        {
            projected = value;
            return true;
        }

        if (Offers(codec, target))
        {
            projected = codec.ProjectRead(value, target);
            return true;
        }

        Type sourceUnderlying = Nullable.GetUnderlyingType(elementType);
        Type targetUnderlying = Nullable.GetUnderlyingType(target);

        // A value-nullable column into a property with nowhere to put a null. Both spellings of the conversion are
        // reachable: the codec may offer the lifted target (Nullable(DateTime) offers DateTime?), or the unwrapped
        // value may convert on its own (Nullable(Enum8)'s sbyte into a CLR enum).
        if (sourceUnderlying is not null && targetUnderlying is null && target.IsValueType)
        {
            Expression onNull = ThrowNull(site, target);
            Type lifted = typeof(Nullable<>).MakeGenericType(target);

            if (Offers(codec, lifted))
            {
                projected = OverValue(codec.ProjectRead(value, lifted), target, onNull, static present => present);
                return true;
            }

            if (CanConvert(sourceUnderlying, target))
            {
                projected = OverValue(value, target, onNull, present => Cast(present, target));
                return true;
            }

            return false;
        }

        // A nullable property: null stays null, whichever side the nullability came from.
        if (targetUnderlying is not null)
        {
            if (sourceUnderlying is null)
            {
                if (!TryResolve(codec, value, targetUnderlying, site, out Expression present))
                {
                    return false;
                }

                projected = Cast(present, target);
                return true;
            }

            if (!CanConvert(sourceUnderlying, targetUnderlying))
            {
                return false;
            }

            projected = OverValue(value, target, Expression.Default(target), present => Cast(present, targetUnderlying));
            return true;
        }

        if (!CanConvert(elementType, target))
        {
            return false;
        }

        projected = Cast(value, target);
        return true;
    }

    /// <summary>
    /// The exception a scatter throws when a NULL row reaches a property that cannot hold one. Called from
    /// compiled code, so the constant parts arrive as arguments rather than being formatted at build time.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <param name="columnType">The column's ClickHouse type.</param>
    /// <param name="pocoType">The POCO type's name.</param>
    /// <param name="memberName">The property name.</param>
    /// <param name="memberType">The property type.</param>
    /// <param name="row">The zero-based row of the result the NULL was found at.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception NullNotAssignable(string columnName, string columnType, string pocoType, string memberName, string memberType, long row)
        => new InvalidOperationException(
            $"Column '{columnName}' ({columnType}) is NULL at row {row} of the result, but it maps to property '{pocoType}.{memberName}' of type {memberType}, which cannot hold null. " +
            $"Make that property nullable, or exclude the NULLs in the query.");

    /// <summary>Whether the codec advertises a projection to <paramref name="target"/>.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="target">The type to look for.</param>
    /// <returns>Whether the type is one of the codec's readable element types.</returns>
    private static bool Offers(IColumnCodec codec, Type target)
    {
        IReadOnlyList<Type> readable = codec.ReadableElementTypes;
        for (int i = 0; i < readable.Count; i++)
        {
            if (readable[i] == target)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The conversions that need no codec: a CLR <c>enum</c> over its own ordinal type, and a target the source is
    /// assignable to. Both are exactly what the equivalent C# cast does, which is why neither needs the codec's
    /// per-column state (scale, timezone, labels).
    /// </summary>
    /// <param name="from">The source type.</param>
    /// <param name="to">The target type.</param>
    /// <returns>Whether a plain cast converts one to the other.</returns>
    private static bool CanConvert(Type from, Type to)
        // The ordinal cast is unchecked, matching (TEnum)raw in C#: the column's declared labels are not consulted
        // (D6b), so an ordinal the CLR enum does not name arrives as that ordinal instead of being rejected.
        => (to.IsEnum && Enum.GetUnderlyingType(to) == from) || to.IsAssignableFrom(from);

    private static Expression Cast(Expression value, Type target)
        => value.Type == target ? value : Expression.Convert(value, target);

    /// <summary>
    /// Runs <paramref name="fromValue"/> over a nullable expression's value, using <paramref name="onNull"/> for a
    /// null row. The source is bound to a local first: it is referenced twice (<c>HasValue</c> and <c>Value</c>)
    /// and may be a whole projection, which must not run twice.
    /// </summary>
    /// <param name="nullable">An expression of some <see cref="Nullable{T}"/> type.</param>
    /// <param name="target">The type the result must have.</param>
    /// <param name="onNull">The expression yielding the result for a null row (a default, or a throw).</param>
    /// <param name="fromValue">Builds the result from the unwrapped value.</param>
    /// <returns>An expression of type <paramref name="target"/>.</returns>
    private static Expression OverValue(Expression nullable, Type target, Expression onNull, Func<Expression, Expression> fromValue)
    {
        ParameterExpression source = Expression.Variable(nullable.Type, "nullable");
        return Expression.Block(
            new[] { source },
            Expression.Assign(source, nullable),
            Expression.Condition(
                Expression.Property(source, "HasValue"),
                Cast(fromValue(Expression.Property(source, "Value")), target),
                onNull));
    }

    private static Expression ThrowNull(PocoProjectionSite site, Type target) => Expression.Throw(
        Expression.Call(
            NullNotAssignableMethod,
            Expression.Constant(site.ColumnName, typeof(string)),
            Expression.Constant(site.ColumnType, typeof(string)),
            Expression.Constant(site.PocoTypeName, typeof(string)),
            Expression.Constant(site.MemberName, typeof(string)),
            Expression.Constant(target.ToString(), typeof(string)),
            site.Row),
        target);
}

/// <summary>
/// What a projection needs to name in a runtime failure: which column, which property, and which row. The row is
/// an expression because it is the scatter's loop counter, not a constant.
/// </summary>
internal readonly struct PocoProjectionSite
{
    /// <summary>The column name from the block header.</summary>
    public string ColumnName { get; init; }

    /// <summary>The column's ClickHouse type from the block header.</summary>
    public string ColumnType { get; init; }

    /// <summary>The POCO type's name.</summary>
    public string PocoTypeName { get; init; }

    /// <summary>The property's name.</summary>
    public string MemberName { get; init; }

    /// <summary>
    /// A <see cref="long"/> expression yielding the row being scattered, counted across the whole result rather than
    /// within the block — a caller never sees the blocks, so a block-local index would name the wrong row.
    /// </summary>
    public Expression Row { get; init; }
}
