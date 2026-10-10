using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The failures of the row inserts (<c>InsertRowsAsync&lt;T&gt;</c> and <c>InsertRowsAsync(object[])</c>), which name the
/// column, its type, the property or the position of the value, and the row.
/// </summary>
internal static class PocoWriteErrors
{
    /// <summary>Reports a property type that the column cannot be written from.</summary>
    /// <param name="column">The target column.</param>
    /// <param name="accepted">The CLR types to suggest (<see cref="ConverterDerivation.SuggestedTypes"/>).</param>
    /// <param name="member">The property that cannot fill it.</param>
    /// <param name="pocoType">The row type.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception NotWritableAs(IColumn column, IReadOnlyList<Type> accepted, PocoMember member, Type pocoType)
    {
        // Empty means the type is written only from a column of its own layout.
        string remedy = accepted.Count == 0
            ? $"No property type can fill a '{column.TypeName}' column: insert it through the columnar API, which can build the column shape it needs."
            : $"It accepts {string.Join(" or ", accepted)}, and an Array, Map or Tuple type also accepts rows of the types that its element types accept. " +
              "Give the property one of those types, or insert that column through the columnar API.";

        return new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) is filled from property '{pocoType.Name}.{member.MemberName}' of type {member.MemberType}, which it cannot be written from. " + remedy);
    }

    /// <summary>Builds the runtime error for a null property mapped to a non-nullable column.</summary>
    /// <param name="columnName">The target column's name.</param>
    /// <param name="columnType">The target column's ClickHouse type.</param>
    /// <param name="pocoType">The POCO type's name.</param>
    /// <param name="memberName">The property name.</param>
    /// <param name="row">The zero-based row of the insert the null was found at.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception NullNotWritable(string columnName, string columnType, string pocoType, string memberName, long row)
        => new InvalidOperationException(
            $"Property '{pocoType}.{memberName}' is null at row {row} of the insert, but it maps to column '{columnName}' ({columnType}), which cannot hold null. " +
            $"Make the column Nullable(...), or leave out the rows with no value.");

    /// <summary>Reports a target column that no CLR type of untyped values can fill.</summary>
    /// <param name="target">The target column.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception NotBuildableFromRows(IColumn target)
        => new InvalidOperationException(
            $"The target column '{target.Name}' has type '{target.TypeName}', which cannot be built from rows: insert it through the columnar API, which can build the column shape it needs.");

    /// <summary>Reports untyped values of a CLR type that the target column cannot be written from.</summary>
    /// <param name="index">The column's position in every row.</param>
    /// <param name="target">The target column.</param>
    /// <param name="accepted">The CLR types to suggest (<see cref="ConverterDerivation.SuggestedTypes"/>). A boxed value is
    /// never a nullable value type, so the message names the value type of each.</param>
    /// <param name="present">The CLR type of the first value that is not null.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception ValuesNotWritable(int index, IColumn target, IReadOnlyList<Type> accepted, Type present)
    {
        var boxed = new List<Type>(accepted.Count);
        foreach (Type type in accepted)
        {
            Type value = Nullable.GetUnderlyingType(type) ?? type;
            if (!boxed.Contains(value))
            {
                boxed.Add(value);
            }
        }

        return new InvalidOperationException(
            $"Column {index} ('{target.Name}', {target.TypeName}) was given values of type {present}, which it cannot be written from. " +
            $"It accepts {string.Join(" or ", boxed)}, and an Array, Map or Tuple type also accepts values of the types that its element types accept.");
    }
}
