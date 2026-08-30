using System;
using System.Linq.Expressions;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Asks what this client can do with a ClickHouse type, given its type string: which CLR types a column of that
/// type can be written from, and which it can be read as. Both questions are answered by the same code the insert
/// and read paths use, so an answer of <see langword="true"/> is the operation succeeding rather than a claim about
/// it.
///
/// <para>
/// The answers are not guessable, which is why they are askable: <c>FixedString(N)</c> takes a
/// <c>byte[]</c> of exactly <c>N</c> and refuses a <see cref="string"/>; <c>Decimal(9, 2)</c> takes a
/// <see cref="decimal"/> and <c>Decimal(38, 2)</c> a <see cref="ClickHouseTcpDecimal"/>; <c>Date</c> takes a
/// <see cref="DateOnly"/> and not a <see cref="DateTime"/>. A composite answers by asking its children about the
/// matching part of the type, so <c>Array(Nullable(DateTime))</c> takes a <c>DateTime?[]</c> per row.
/// </para>
///
/// <para>
/// There is deliberately no method listing every accepted type. For a composite that list is the product of its
/// children's lists, which is large, uninteresting, and never what a caller has: a caller has a candidate type and
/// wants a yes or no.
/// </para>
///
/// <para>
/// A session timezone is not an argument because it cannot change either answer: it decides which instant a
/// timezone-less <c>DateTime</c> column means, not which CLR types express it.
/// </para>
/// </summary>
public static class ClickHouseTcpTypes
{
    /// <summary>
    /// Whether a column of <paramref name="elementType"/> values can be written to a
    /// <paramref name="clickHouseType"/> column — the question an insert asks of the column you built with
    /// <see cref="ClickHouseTcpColumn.Create{T}(string, T[])"/>.
    /// </summary>
    /// <param name="clickHouseType">The target column's ClickHouse type (e.g. <c>Array(Nullable(DateTime))</c>).</param>
    /// <param name="elementType">The CLR type of one row's value.</param>
    /// <returns>Whether a column of that element type can be written to that type.</returns>
    /// <remarks>
    /// A <c>Variant</c> (and a <c>Dynamic</c>) is written from a column of <see cref="object"/>, so the answer is
    /// about that shape: which alternative each row takes is decided per value from its runtime type, and only an
    /// alternative's own element type is matched there — not the extra CLR types that alternative would accept as a
    /// column of its own. A value of one of those is refused when the row is placed, naming the types the variant
    /// does match.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="clickHouseType"/> or <paramref name="elementType"/> is null.</exception>
    /// <exception cref="FormatException"><paramref name="clickHouseType"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed but this client does not support it.</exception>
    public static bool CanWrite(string clickHouseType, Type elementType)
    {
        ArgumentNullException.ThrowIfNull(elementType);
        return Resolve(clickHouseType).CanWriteElementType(elementType);
    }

    /// <summary>
    /// Whether a <paramref name="clickHouseType"/> column can be read as <paramref name="elementType"/> — the
    /// question <see cref="Block.ReadAs{T}(string)"/> asks, and the one the POCO tier asks of a property type.
    /// True for the type the column decodes to, and for every other reading that type offers (an <c>Enum8</c> as a
    /// <see cref="string"/> label, a <c>DateTime64</c> as a <see cref="DateTime"/>, a <c>String</c> as a
    /// <c>byte[]</c>, a <c>FixedString(N)</c> as a <see cref="string"/>). There is no numeric widening, so a
    /// <c>UInt32</c> column does not read as a <see cref="long"/>.
    /// </summary>
    /// <param name="clickHouseType">The column's ClickHouse type.</param>
    /// <param name="elementType">The CLR type to read the values as.</param>
    /// <returns>Whether that type offers a reading as that CLR type.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="clickHouseType"/> or <paramref name="elementType"/> is null.</exception>
    /// <exception cref="FormatException"><paramref name="clickHouseType"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed but this client does not support it.</exception>
    public static bool CanRead(string clickHouseType, Type elementType)
    {
        ArgumentNullException.ThrowIfNull(elementType);
        IColumnCodec codec = Resolve(clickHouseType);

        // The codec answers by building the conversion, which is also how ReadAs gets it, so the two cannot differ.
        // Both readings are asked for, and in ReadAs's order: off the column's storage, then off its decoded value.
        return codec.TryProjectColumnRead(Expression.Parameter(typeof(IColumn), "column"), Expression.Parameter(typeof(int), "row"), elementType, out _)
            || codec.TryProjectRead(Expression.Parameter(codec.ElementType, "value"), elementType, out _);
    }

    private static IColumnCodec Resolve(string clickHouseType)
    {
        ArgumentNullException.ThrowIfNull(clickHouseType);
        return ColumnCodecRegistry.Default.Resolve(clickHouseType, ResolveContext.ForWrite);
    }
}
