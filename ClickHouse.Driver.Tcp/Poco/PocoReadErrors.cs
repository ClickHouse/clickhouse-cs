using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>The failures of a POCO read plan, which name the column, its type, the property and the row.</summary>
internal static class PocoReadErrors
{
    /// <summary>
    /// Reports a column that does not implement <see cref="IColumn{T}"/> for its codec's element type.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <param name="codec">The column's codec.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception NotSurfacingItsElementType(IColumn column, IColumnCodec codec)
        => new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) was read as {column.GetType()}, which does not implement IColumn<{codec.ElementType}> " +
            $"as its codec {codec.GetType()} declares. A POCO read sources every value through that interface, so the column cannot be read into a property.");

    /// <summary>
    /// Reports a property type the column cannot be read as.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <param name="readable">The CLR types to suggest (<see cref="ConverterDerivation.SuggestedTypes"/>).</param>
    /// <param name="member">The property that cannot be filled.</param>
    /// <param name="pocoType">The POCO type.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception NotReadableAs(IColumn column, IReadOnlyList<Type> readable, PocoMember member, Type pocoType)
    {
        var offered = new string[readable.Count];
        for (int i = 0; i < readable.Count; i++)
        {
            offered[i] = readable[i].ToString();
        }

        // A bare NULL or empty-array literal comes back as Nothing (or a composite of it): the column carries no type
        // of its own, so it reads only as object however nullable the property is. Changing the property cannot help,
        // so that case gets its own remedy.
        string remedy = NamesTheNothingType(TypeParser.Parse(column.TypeName))
            ? "That column is an untyped NULL, so it carries no type to read as anything else: give it one in the query (for example CAST(NULL AS Nullable(String))), or exclude the property with [ClickHouseTcpNotMapped]."
            : "Give the property one of those types, exclude it with [ClickHouseTcpNotMapped], or read the column through the block-level API.";

        return new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) maps to property '{pocoType.Name}.{member.MemberName}' of type {member.MemberType}, which it cannot be read as. " +
            $"It reads as {string.Join(" or ", offered)}. {remedy}");
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
    /// Whether a parsed type contains a real <c>Nothing</c> node, excluding labels or field names with that text.
    /// </summary>
    /// <param name="node">The parsed column type.</param>
    /// <returns>Whether the type is, or contains, <c>Nothing</c>.</returns>
    private static bool NamesTheNothingType(TypeNode node)
    {
        if (string.Equals(node.Name, NothingColumnCodec.Instance.TypeName, StringComparison.Ordinal))
        {
            return true;
        }

        for (int i = 0; i < node.Arguments.Count; i++)
        {
            if (NamesTheNothingType(node.Arguments[i]))
            {
                return true;
            }
        }

        return false;
    }
}
