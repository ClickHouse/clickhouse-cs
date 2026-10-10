using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The CLR write type choice and the property conversions of the POCO write plan before it moved onto the converter
/// derivation: the reference path of the differential tests (<see cref="LegacyPocoColumnBuilderFactory"/>). The converter
/// derivation has the same rules (<see cref="Types.Converters.WriteRules"/>).
/// </summary>
internal static class LegacyPocoWriteConversion
{
    private static readonly MethodInfo NullNotWritableMethod =
        typeof(PocoWriteErrors).GetMethod(nameof(PocoWriteErrors.NullNotWritable), BindingFlags.Public | BindingFlags.Static);

    /// <summary>Chooses the codec's preferred write type compatible with <paramref name="memberType"/>.</summary>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="memberType">The property type the values come from.</param>
    /// <param name="writeType">The chosen write type, or null when none applies.</param>
    /// <returns>Whether a write type applies.</returns>
    public static bool TryChooseWriteType(IColumnCodec codec, Type memberType, out Type writeType)
    {
        IReadOnlyList<Type> accepted = PocoWriteConversion.AcceptedWriteTypes(codec);
        for (int i = 0; i < accepted.Count; i++)
        {
            if (CanConvert(memberType, accepted[i]))
            {
                writeType = accepted[i];
                return true;
            }
        }

        // Composite codecs do not enumerate every writable child-type combination.
        if (codec.CanWriteElementType(memberType))
        {
            writeType = memberType;
            return true;
        }

        writeType = null;
        return false;
    }

    /// <summary>Builds the conversion for a property/write-type pair accepted by <see cref="TryChooseWriteType"/>.</summary>
    /// <param name="value">An expression yielding the property's value for one row.</param>
    /// <param name="writeType">The CLR type the column is written in.</param>
    /// <param name="targetTakesNull">Whether the target column has a NULL of its own
    /// (<see cref="PocoWriteConversion.TakesNull"/>).</param>
    /// <param name="site">The column/property names a failed conversion reports at runtime.</param>
    /// <returns>An expression of type <paramref name="writeType"/>.</returns>
    public static Expression Convert(Expression value, Type writeType, bool targetTakesNull, PocoGatherSite site)
    {
        Type memberType = value.Type;

        // Reject null before writing so the error identifies the property and row without breaking the connection.
        if (!targetTakesNull && !memberType.IsValueType)
        {
            value = RequireNotNull(value, site);
        }

        if (memberType == writeType)
        {
            return value;
        }

        Type memberUnderlying = Nullable.GetUnderlyingType(memberType);
        Type writeUnderlying = Nullable.GetUnderlyingType(writeType);

        if (memberUnderlying is not null)
        {
            // Evaluate the property once; a supported null becomes the write type's default value.
            bool nullSurvives = targetTakesNull && (writeUnderlying is not null || !writeType.IsValueType);
            ParameterExpression source = Expression.Variable(memberType, "nullable");
            Expression present = Convert(Expression.Property(source, "Value"), writeUnderlying ?? writeType, targetTakesNull, site);
            return Expression.Block(
                new[] { source },
                Expression.Assign(source, value),
                Expression.Condition(
                    Expression.Property(source, "HasValue"),
                    Lift(present, writeType),
                    nullSurvives ? Expression.Default(writeType) : ThrowNull(site, writeType)));
        }

        return writeUnderlying is not null
            ? Lift(Convert(value, writeUnderlying, targetTakesNull, site), writeType)
            : Expression.Convert(value, writeType);
    }

    /// <summary>Tests identity, nullable lifting, enum ordinal casts and reference assignability.</summary>
    /// <param name="from">The property type.</param>
    /// <param name="to">The candidate write type.</param>
    /// <returns>Whether the conversion applies.</returns>
    private static bool CanConvert(Type from, Type to)
    {
        if (from == to)
        {
            return true;
        }

        Type fromUnderlying = Nullable.GetUnderlyingType(from);
        Type toUnderlying = Nullable.GetUnderlyingType(to);
        if (fromUnderlying is not null)
        {
            return CanConvert(fromUnderlying, toUnderlying ?? to);
        }

        if (toUnderlying is not null)
        {
            return CanConvert(from, toUnderlying);
        }

        // Enum labels are not consulted; writing uses the underlying ordinal.
        return (from.IsEnum && Enum.GetUnderlyingType(from) == to) || to.IsAssignableFrom(from);
    }

    private static Expression Lift(Expression value, Type target)
        => value.Type == target ? value : Expression.Convert(value, target);

    /// <summary>Adds a null check while evaluating the source expression only once.</summary>
    /// <param name="value">An expression of a reference type.</param>
    /// <param name="site">The column/property names the failure reports.</param>
    /// <returns>An expression of the same type, throwing where that would be null.</returns>
    private static Expression RequireNotNull(Expression value, PocoGatherSite site)
    {
        ParameterExpression source = Expression.Variable(value.Type, "reference");
        return Expression.Block(
            new[] { source },
            Expression.Assign(source, value),
            Expression.Condition(
                Expression.ReferenceEqual(source, Expression.Constant(null, value.Type)),
                ThrowNull(site, value.Type),
                source));
    }

    private static Expression ThrowNull(PocoGatherSite site, Type writeType) => Expression.Throw(
        Expression.Call(
            NullNotWritableMethod,
            Expression.Constant(site.ColumnName, typeof(string)),
            Expression.Constant(site.ColumnType, typeof(string)),
            Expression.Constant(site.PocoTypeName, typeof(string)),
            Expression.Constant(site.MemberName, typeof(string)),
            site.Row),
        writeType);
}

/// <summary>
/// Identifies the property, target column and row used in gather-time errors.
/// </summary>
internal readonly struct PocoGatherSite
{
    /// <summary>The target column's name.</summary>
    public string ColumnName { get; init; }

    /// <summary>The target column's ClickHouse type.</summary>
    public string ColumnType { get; init; }

    /// <summary>The POCO type's name.</summary>
    public string PocoTypeName { get; init; }

    /// <summary>The property's name.</summary>
    public string MemberName { get; init; }

    /// <summary>The zero-based row index expression.</summary>
    public Expression Row { get; init; }
}
