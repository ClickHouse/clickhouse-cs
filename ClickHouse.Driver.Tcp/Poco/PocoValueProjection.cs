using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Builds the expression that converts a decoded column value into a POCO property value. It supports codec-owned
/// projections, nullable lifting/unwrapping, enum ordinal casts and assignability; numeric widening is not allowed.
/// An <see cref="object"/> target receives the column's raw element value.
/// </summary>
internal static class PocoValueProjection
{
    private static readonly MethodInfo NullNotAssignableMethod =
        typeof(PocoValueProjection).GetMethod(nameof(NullNotAssignable), BindingFlags.Public | BindingFlags.Static);

    /// <summary>Builds the projection from one decoded value to one property value.</summary>
    /// <param name="codec">The column's codec, consulted for codec-owned conversions.</param>
    /// <param name="value">A repeatable expression yielding one decoded element.</param>
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

        if (codec.TryProjectRead(value, target, out projected))
        {
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

            if (codec.TryProjectRead(value, lifted, out Expression liftedProjection))
            {
                projected = OverValue(liftedProjection, target, onNull, static present => present);
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
    /// Creates the exception thrown when a NULL reaches a property that cannot hold it.
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

    /// <summary>
    /// Whether a CLR enum ordinal cast or reference/value assignability can perform the conversion.
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
    /// Projects a nullable expression's value or evaluates <paramref name="onNull"/> when absent.
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
/// Column, property and row context for projection failures.
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
    /// A <see cref="long"/> expression yielding the row index across the whole result.
    /// </summary>
    public Expression Row { get; init; }
}
