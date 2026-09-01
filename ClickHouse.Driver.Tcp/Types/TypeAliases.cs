using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The alternative spellings ClickHouse accepts for a type name, and the canonical name each one means. The
/// table is every row of <c>system.data_type_families</c> on 26.6 with a non-empty <c>alias_to</c>.
///
/// <para>
/// A column header never carries one of these — the server always reports the canonical name — so this exists
/// for the names a caller writes: a <c>{p:Type}</c> hint, <see cref="ClickHouseTcpParameter.ClickHouseType"/>,
/// and <c>ClickHouseTcpTypes.CanRead</c>/<c>CanWrite</c>. The shipped HTTP driver accepts all of them, so a
/// caller moving a query from it should not have to respell their types.
/// </para>
///
/// <para>
/// Every name here matches without regard to case, and so does every name the codec registry knows. The server
/// marks only some families <c>case_insensitive</c>, but which ones is the server's business: a spelling it
/// rejects it rejects with a better message than a copy of that rule here would give, and the rule moves between
/// versions and settings. A name this client cannot resolve at all is a different matter, and is still refused.
/// </para>
/// </summary>
internal static class TypeAliases
{
    /// <summary>Every alias, keyed without regard to case.</summary>
    private static readonly Dictionary<string, string> Aliases = new(StringComparer.OrdinalIgnoreCase)
    {
        // A family of its own on the server, which resolves to DateTime rather than aliasing it.
        ["DateTime32"] = "DateTime",

        ["GEOMETRY"] = "Geometry",
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

    /// <summary>Finds the canonical name an alternative spelling means.</summary>
    /// <param name="name">The base type name as the caller wrote it.</param>
    /// <param name="canonical">The canonical name, or null when the name is not an alias.</param>
    /// <returns>True when <paramref name="name"/> is an alias of a canonical name.</returns>
    public static bool TryCanonical(string name, out string canonical)
    {
        canonical = null;
        return name is not null && Aliases.TryGetValue(name, out canonical);
    }

    /// <summary>The canonical name an alternative spelling means, or <paramref name="name"/> unchanged.</summary>
    /// <param name="name">The base type name as the caller wrote it.</param>
    /// <returns>The canonical name when there is one, otherwise the name given.</returns>
    public static string Canonical(string name) => TryCanonical(name, out string canonical) ? canonical : name;

    /// <summary>Every alternative spelling and the canonical name it means, for the tests that check the table.</summary>
    /// <returns>One pair per spelling.</returns>
    public static IEnumerable<KeyValuePair<string, string>> All() => Aliases;
}
