using System;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Reports whether a ClickHouse type can be written from or read as a CLR element type. Results come from the
/// same type resolution used by insert and read operations.
/// </summary>
public static class ClickHouseTcpTypes
{
    /// <summary>Whether a column of <paramref name="elementType"/> values can be written as <paramref name="clickHouseType"/>.</summary>
    /// <param name="clickHouseType">The target column's ClickHouse type (e.g. <c>Array(Nullable(DateTime))</c>).</param>
    /// <param name="elementType">The CLR type of one row's value.</param>
    /// <returns>Whether a column of that element type can be written to that type.</returns>
    /// <remarks>
    /// <c>Variant</c> accepts only the canonical CLR type of a declared alternative. <c>Dynamic</c> infers a
    /// ClickHouse type from each runtime value.
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
    /// Whether a <paramref name="clickHouseType"/> column can be read as <paramref name="elementType"/> by
    /// <see cref="Block.ReadAs{T}(string)"/> or POCO mapping. Numeric widening is not supported.
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

        // Use the same resolver as ReadAs and POCO mapping.
        return ColumnProjection.Offers(Resolve(clickHouseType), elementType);
    }

    private static IColumnCodec Resolve(string clickHouseType)
    {
        ArgumentNullException.ThrowIfNull(clickHouseType);
        return ColumnCodecRegistry.Default.Resolve(clickHouseType, ResolveContext.ForWrite);
    }
}
