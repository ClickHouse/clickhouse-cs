using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The alternative spellings ClickHouse accepts for a type name, and the canonical name each one means. The
/// table is <c>system.data_type_families</c> on 26.6: every row with a non-empty <c>alias_to</c>, plus the
/// families the server itself matches without regard to case.
///
/// <para>
/// A column header never carries one of these — the server always reports the canonical name — so this exists
/// for the names a caller writes: a <c>{p:Type}</c> hint, <see cref="ClickHouseTcpParameter.ClickHouseType"/>,
/// and <c>ClickHouseTcpTypes.CanRead</c>/<c>CanWrite</c>. The shipped HTTP driver accepts all of them, so a
/// caller moving a query from it should not have to respell their types.
/// </para>
///
/// <para>
/// Case is per family, not global: the server marks <c>case_insensitive = 1</c> for the families listed here and
/// <c>0</c> for the rest, so <c>datetime64(3)</c> resolves while <c>string</c>, <c>int64</c>, <c>array(uint8)</c>
/// and <c>geometry</c> are unknown families to it. Matching every name without regard to case would accept what
/// the server rejects.
/// </para>
/// </summary>
internal static class TypeAliases
{
    /// <summary>
    /// The spellings the server matches without regard to case: the <c>case_insensitive = 1</c> families, whether
    /// they are an alias of another family or the canonical name of their own.
    /// </summary>
    private static readonly Dictionary<string, string> AnyCase = new(StringComparer.OrdinalIgnoreCase)
    {
        // The families that are their own canonical name. Each is here so that any case of it resolves, and so
        // that the canonical spelling is what a codec is stamped with.
        ["Bool"] = "Bool",
        ["Date"] = "Date",
        ["Date32"] = "Date32",
        ["DateTime"] = "DateTime",
        ["DateTime64"] = "DateTime64",
        ["Decimal"] = "Decimal",
        ["Decimal32"] = "Decimal32",
        ["Decimal64"] = "Decimal64",
        ["Decimal128"] = "Decimal128",
        ["Decimal256"] = "Decimal256",
        ["Enum"] = "Enum",
        ["JSON"] = "JSON",
        ["Time"] = "Time",
        ["Time64"] = "Time64",

        // A family of its own on the server, which resolves to DateTime rather than aliasing it.
        ["DateTime32"] = "DateTime",

        ["boolean"] = "Bool",
        ["TIMESTAMP"] = "DateTime",
        ["DEC"] = "Decimal",
        ["FIXED"] = "Decimal",
        ["NUMERIC"] = "Decimal",
        ["BINARY"] = "FixedString",
        ["FLOAT"] = "Float32",
        ["REAL"] = "Float32",
        ["SINGLE"] = "Float32",
        ["DOUBLE"] = "Float64",
        ["DOUBLE PRECISION"] = "Float64",
        ["INET4"] = "IPv4",
        ["INET6"] = "IPv6",

        ["BYTE"] = "Int8",
        ["INT1"] = "Int8",
        ["INT1 SIGNED"] = "Int8",
        ["TINYINT"] = "Int8",
        ["TINYINT SIGNED"] = "Int8",
        ["SMALLINT"] = "Int16",
        ["SMALLINT SIGNED"] = "Int16",
        ["INT"] = "Int32",
        ["INT SIGNED"] = "Int32",
        ["INTEGER"] = "Int32",
        ["INTEGER SIGNED"] = "Int32",
        ["MEDIUMINT"] = "Int32",
        ["MEDIUMINT SIGNED"] = "Int32",
        ["BIGINT"] = "Int64",
        ["BIGINT SIGNED"] = "Int64",
        ["SIGNED"] = "Int64",

        ["INT1 UNSIGNED"] = "UInt8",
        ["TINYINT UNSIGNED"] = "UInt8",
        ["SMALLINT UNSIGNED"] = "UInt16",
        ["YEAR"] = "UInt16",
        ["INT UNSIGNED"] = "UInt32",
        ["INTEGER UNSIGNED"] = "UInt32",
        ["MEDIUMINT UNSIGNED"] = "UInt32",
        ["BIGINT UNSIGNED"] = "UInt64",
        ["BIT"] = "UInt64",
        ["SET"] = "UInt64",
        ["UNSIGNED"] = "UInt64",

        ["BINARY LARGE OBJECT"] = "String",
        ["BINARY VARYING"] = "String",
        ["BLOB"] = "String",
        ["BYTEA"] = "String",
        ["CHAR"] = "String",
        ["CHAR LARGE OBJECT"] = "String",
        ["CHAR VARYING"] = "String",
        ["CHARACTER"] = "String",
        ["CHARACTER LARGE OBJECT"] = "String",
        ["CHARACTER VARYING"] = "String",
        ["CLOB"] = "String",
        ["LONGBLOB"] = "String",
        ["LONGTEXT"] = "String",
        ["MEDIUMBLOB"] = "String",
        ["MEDIUMTEXT"] = "String",
        ["NATIONAL CHAR"] = "String",
        ["NATIONAL CHAR VARYING"] = "String",
        ["NATIONAL CHARACTER"] = "String",
        ["NATIONAL CHARACTER LARGE OBJECT"] = "String",
        ["NATIONAL CHARACTER VARYING"] = "String",
        ["NCHAR"] = "String",
        ["NCHAR LARGE OBJECT"] = "String",
        ["NCHAR VARYING"] = "String",
        ["NVARCHAR"] = "String",
        ["TEXT"] = "String",
        ["TINYBLOB"] = "String",
        ["TINYTEXT"] = "String",
        ["VARBINARY"] = "String",
        ["VARCHAR"] = "String",
        ["VARCHAR2"] = "String",
    };

    /// <summary>
    /// The one alias the server marks <c>case_insensitive = 0</c>: it takes <c>GEOMETRY</c> and the canonical
    /// <c>Geometry</c>, and answers "Unknown data type family" for <c>geometry</c>.
    /// </summary>
    private static readonly Dictionary<string, string> ExactCase = new(StringComparer.Ordinal)
    {
        ["GEOMETRY"] = "Geometry",
    };

    /// <summary>Finds the canonical name an alternative spelling means.</summary>
    /// <param name="name">The base type name as the caller wrote it.</param>
    /// <param name="canonical">The canonical name, or null when the spelling is not one the server accepts.</param>
    /// <returns>True when <paramref name="name"/> is an alias or a case variant of a canonical name.</returns>
    public static bool TryCanonical(string name, out string canonical)
        => ExactCase.TryGetValue(name, out canonical) || AnyCase.TryGetValue(name, out canonical);

    /// <summary>The canonical name an alternative spelling means, or <paramref name="name"/> unchanged.</summary>
    /// <param name="name">The base type name as the caller wrote it.</param>
    /// <returns>The canonical name when there is one, otherwise the name given.</returns>
    public static string Canonical(string name) => TryCanonical(name, out string canonical) ? canonical : name;

    /// <summary>Every alternative spelling and the canonical name it means, for the tests that check the table.</summary>
    /// <returns>One pair per spelling, the case-sensitive <c>GEOMETRY</c> included.</returns>
    public static IEnumerable<KeyValuePair<string, string>> All()
    {
        foreach (KeyValuePair<string, string> alias in ExactCase)
        {
            yield return alias;
        }

        foreach (KeyValuePair<string, string> alias in AnyCase)
        {
            yield return alias;
        }
    }
}
