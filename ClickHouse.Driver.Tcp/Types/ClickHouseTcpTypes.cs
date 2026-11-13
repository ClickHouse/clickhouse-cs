using System;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Reports whether a ClickHouse type can be written from or read as a CLR element type. Results come from the
/// same type resolution used by insert and read operations.
/// </summary>
public static class ClickHouseTcpTypes
{
    /// <summary>
    /// Whether values of <paramref name="elementType"/> can be written as <paramref name="clickHouseType"/>: a column of
    /// them by <c>InsertAsync</c>, a property of that type by <c>InsertRowsAsync&lt;T&gt;</c>, and untyped values of that
    /// type by <c>InsertRowsAsync</c> with <see cref="object"/> rows.
    /// </summary>
    /// <param name="clickHouseType">The target column's ClickHouse type (e.g. <c>Array(Nullable(DateTime))</c>).</param>
    /// <param name="elementType">The CLR type of one row's value.</param>
    /// <returns>Whether values of that type can be written to that type.</returns>
    /// <remarks>
    /// <para>
    /// Besides the CLR types that the type is written from, four rules apply to the whole column type: a CLR enum is
    /// written as its integer ordinal; a value is written as a type that it casts to with no conversion (for example a
    /// <see cref="string"/> as the <see cref="object"/> of a <c>Variant</c>); a value type is written into a column of its
    /// nullable type; and a nullable value type is written into a column that cannot hold NULL, where the insert throws
    /// at the first NULL. An array cast that gives the elements another meaning, such as a <see cref="T:uint[]"/> as an
    /// <see cref="T:int[]"/>, does not apply; an enum array casts to an array of its underlying type.
    /// </para>
    /// <para>
    /// <c>Variant</c> is written from <see cref="object"/>. A value goes to the alternative whose canonical CLR type is the
    /// value's type, or, when there is none, to the alternative that is written from the value's type. <c>Dynamic</c>
    /// infers a ClickHouse type from each runtime value. <c>Nested</c> is written only from a column that a query of the
    /// same type read, so the answer is false for it.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="clickHouseType"/> or <paramref name="elementType"/> is null.</exception>
    /// <exception cref="FormatException"><paramref name="clickHouseType"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed but this client does not support it.</exception>
    public static bool CanWrite(string clickHouseType, Type elementType)
    {
        ArgumentNullException.ThrowIfNull(elementType);
        ArgumentNullException.ThrowIfNull(clickHouseType);

        // The same derivation as the insert of a column.
        return ConverterDerivation.Default.Derive(clickHouseType, ResolveContext.ForWrite, elementType, ConversionDirection.Write).Succeeded;
    }

    /// <summary>
    /// Whether a <paramref name="clickHouseType"/> column can be read as <paramref name="elementType"/> by
    /// <see cref="Block.ReadAs{T}(string)"/> or POCO mapping. Numeric widening is not supported.
    /// </summary>
    /// <param name="clickHouseType">The column's ClickHouse type.</param>
    /// <param name="elementType">The CLR type to read the values as.</param>
    /// <returns>Whether that type offers a reading as that CLR type.</returns>
    /// <remarks>
    /// Besides the readings that the type offers, three rules apply to the whole column type: a CLR enum reads from
    /// its integer ordinal, a column reads as a type that its values cast to (for example <see cref="object"/>), and
    /// a column of a nullable type reads as a value type that cannot hold null. Such a read throws when it reaches a
    /// NULL value. An array cast that gives the elements another meaning, such as a <see cref="T:uint[]"/> as an
    /// <see cref="T:int[]"/>, does not apply; an array casts to an array of an enum whose underlying type is its element
    /// type.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="clickHouseType"/> or <paramref name="elementType"/> is null.</exception>
    /// <exception cref="FormatException"><paramref name="clickHouseType"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed but this client does not support it.</exception>
    public static bool CanRead(string clickHouseType, Type elementType)
    {
        ArgumentNullException.ThrowIfNull(elementType);
        ArgumentNullException.ThrowIfNull(clickHouseType);

        // The same derivation as ReadAs.
        return ConverterDerivation.Default.Derive(clickHouseType, ResolveContext.ForWrite, elementType, ConversionDirection.Read).Succeeded;
    }
}
