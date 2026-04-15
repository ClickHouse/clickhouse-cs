using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types.Grammar;

[assembly: InternalsVisibleTo("ClickHouse.Driver.Tests, PublicKey=00240000048000009400000006020000002400005253413100040000010001000968a6468f9d0397a051f167a25dcee773c674cf7a67629f78e884d232df23ff773fbfaba602e03eede6056b39bd6a4cddcd7e5b3ca9484bd83401d14a5e9ac5c98cbe676a1e89149816f5304f617b658440b2bd775e5ece71b5a38ceeb88e844869a376ceea71cbb6393b2ac14e506b92267a3cbcd6e7dc93ff6c750d53a5c7")] // assembly-level tag to expose below classes to tests

namespace ClickHouse.Driver.Types;

internal static class TypeConverter
{
    private static readonly Dictionary<string, ClickHouseType> SimpleTypes = [];
    private static readonly Dictionary<string, ParameterizedType> ParameterizedTypes = [];
    private static readonly Dictionary<Type, ClickHouseType> ReverseMapping = [];

    private static readonly Dictionary<string, string> Aliases = new()
    {
        { "BIGINT", "Int64" },
        { "BIGINT SIGNED", "Int64" },
        { "BIGINT UNSIGNED", "UInt64" },
        { "BINARY", "FixedString" },
        { "BINARY LARGE OBJECT", "String" },
        { "BINARY VARYING", "String" },
        { "BIT", "UInt64" },
        { "BLOB", "String" },
        { "BYTE", "Int8" },
        { "BYTEA", "String" },
        { "CHAR", "String" },
        { "CHAR LARGE OBJECT", "String" },
        { "CHAR VARYING", "String" },
        { "CHARACTER", "String" },
        { "CHARACTER LARGE OBJECT", "String" },
        { "CHARACTER VARYING", "String" },
        { "CLOB", "String" },
        { "DEC", "Decimal" },
        { "DOUBLE", "Float64" },
        { "DOUBLE PRECISION", "Float64" },
        { "ENUM", "Enum" },
        { "FIXED", "Decimal" },
        { "FLOAT", "Float32" },
        { "GEOMETRY", "Geometry" },
        { "INET4", "IPv4" },
        { "INET6", "IPv6" },
        { "INT", "Int32" },
        { "INT SIGNED", "Int32" },
        { "INT UNSIGNED", "UInt32" },
        { "INT1", "Int8" },
        { "INT1 SIGNED", "Int8" },
        { "INT1 UNSIGNED", "UInt8" },
        { "INTEGER", "Int32" },
        { "INTEGER SIGNED", "Int32" },
        { "INTEGER UNSIGNED", "UInt32" },
        { "LONGBLOB", "String" },
        { "LONGTEXT", "String" },
        { "MEDIUMBLOB", "String" },
        { "MEDIUMINT", "Int32" },
        { "MEDIUMINT SIGNED", "Int32" },
        { "MEDIUMINT UNSIGNED", "UInt32" },
        { "MEDIUMTEXT", "String" },
        { "NATIONAL CHAR", "String" },
        { "NATIONAL CHAR VARYING", "String" },
        { "NATIONAL CHARACTER", "String" },
        { "NATIONAL CHARACTER LARGE OBJECT", "String" },
        { "NATIONAL CHARACTER VARYING", "String" },
        { "NCHAR", "String" },
        { "NCHAR LARGE OBJECT", "String" },
        { "NCHAR VARYING", "String" },
        { "NUMERIC", "Decimal" },
        { "NVARCHAR", "String" },
        { "REAL", "Float32" },
        { "SET", "UInt64" },
        { "SINGLE", "Float32" },
        { "SMALLINT", "Int16" },
        { "SMALLINT SIGNED", "Int16" },
        { "SMALLINT UNSIGNED", "UInt16" },
        { "TEXT", "String" },
        { "TIMESTAMP", "DateTime" },
        { "TINYBLOB", "String" },
        { "TINYINT", "Int8" },
        { "TINYINT SIGNED", "Int8" },
        { "TINYINT UNSIGNED", "UInt8" },
        { "TINYTEXT", "String" },
        { "VARBINARY", "String" },
        { "VARCHAR", "String" },
        { "VARCHAR2", "String" },
        { "YEAR", "UInt16" },
        { "BOOL", "Bool" },
        { "BOOLEAN", "Bool" },
        { "OBJECT('JSON')", "Json" },
        { "JSON", "Json" },
    };

    public static IEnumerable<string> RegisteredTypes => SimpleTypes.Keys
        .Concat(ParameterizedTypes.Values.Select(t => t.Name))
        .OrderBy(x => x)
        .ToArray();

    internal static readonly string[] Separator = [" "];

    static TypeConverter()
    {
        RegisterPlainType<BooleanType>();

        // Integral types
        RegisterPlainType<Int8Type>();
        RegisterPlainType<Int16Type>();
        RegisterPlainType<Int32Type>();
        RegisterPlainType<Int64Type>();
        RegisterPlainType<Int128Type>();
        RegisterPlainType<Int256Type>();

        RegisterPlainType<UInt8Type>();
        RegisterPlainType<UInt16Type>();
        RegisterPlainType<UInt32Type>();
        RegisterPlainType<UInt64Type>();
        RegisterPlainType<UInt128Type>();
        RegisterPlainType<UInt256Type>();

        // Floating point types
        RegisterPlainType<Float32Type>();
        RegisterPlainType<Float64Type>();
        RegisterPlainType<BFloat16Type>();

        // Special types
        RegisterPlainType<DynamicType>();
        RegisterPlainType<UuidType>();
        RegisterPlainType<IPv4Type>();
        RegisterPlainType<IPv6Type>();

        // String types
        RegisterPlainType<StringType>();
        RegisterParameterizedType<FixedStringType>();

        // DateTime types
        RegisterPlainType<DateType>();
        RegisterPlainType<Date32Type>();
        RegisterParameterizedType<DateTimeType>();
        RegisterParameterizedType<DateTime32Type>();
        RegisterParameterizedType<DateTime64Type>();
        RegisterPlainType<TimeType>();
        RegisterParameterizedType<Time64Type>();

        // Special 'nothing' type
        RegisterPlainType<NothingType>();

        // complex types like Tuple/Array/Nested etc.
        RegisterParameterizedType<ArrayType>();
        RegisterParameterizedType<NullableType>();
        RegisterParameterizedType<TupleType>();
        RegisterParameterizedType<NestedType>();
        RegisterParameterizedType<LowCardinalityType>();

        RegisterParameterizedType<DecimalType>();
        RegisterParameterizedType<Decimal32Type>();
        RegisterParameterizedType<Decimal64Type>();
        RegisterParameterizedType<Decimal128Type>();
        RegisterParameterizedType<Decimal256Type>();

        RegisterParameterizedType<EnumType>();
        RegisterParameterizedType<Enum8Type>();
        RegisterParameterizedType<Enum16Type>();
        RegisterParameterizedType<SimpleAggregateFunctionType>();
        RegisterParameterizedType<MapType>();
        RegisterParameterizedType<VariantType>();

        // Geo types
        RegisterPlainType<PointType>();
        RegisterPlainType<RingType>();
        RegisterPlainType<LineStringType>();
        RegisterPlainType<PolygonType>();
        RegisterPlainType<MultiLineStringType>();
        RegisterPlainType<MultiPolygonType>();
        RegisterPlainType<GeometryType>();

        RegisterParameterizedType<ObjectType>();
        RegisterParameterizedType<JsonType>();

        RegisterParameterizedType<AggregateFunctionType>();

        RegisterParameterizedType<QBitType>();

        // Mapping fixups
        ReverseMapping.Add(typeof(ClickHouseDecimal), new Decimal128Type { Scale = 9 });
        ReverseMapping.Add(typeof(decimal), new Decimal128Type { Scale = 9 });
#if NET6_0_OR_GREATER
        ReverseMapping.Add(typeof(DateOnly), new DateType());
#endif
        ReverseMapping[typeof(DateTime)] = new DateTimeType();
        ReverseMapping[typeof(DateTimeOffset)] = new DateTimeType();
        ReverseMapping[typeof(TimeSpan)] = new Time64Type
        {
            Scale = 7, // Matches precision of TimeSpan
        };

        ReverseMapping[typeof(DBNull)] = new NullableType() { UnderlyingType = new NothingType() };
        ReverseMapping[typeof(JsonObject)] = new JsonType();
    }

    private static void RegisterPlainType<T>()
        where T : ClickHouseType, new()
    {
        var type = new T();
        var name = string.Intern(type.ToString()); // There is a limited number of types, interning them will help performance
        SimpleTypes.Add(name, type);
        if (!ReverseMapping.ContainsKey(type.FrameworkType))
        {
            ReverseMapping.Add(type.FrameworkType, type);
        }
    }

    private static void RegisterParameterizedType<T>()
        where T : ParameterizedType, new()
    {
        var t = new T();
        var name = string.Intern(t.Name); // There is a limited number of types, interning them will help performance
        ParameterizedTypes.Add(name, t);
    }

    public static ClickHouseType ParseClickHouseType(string type, TypeSettings settings)
    {
        var node = Parser.Parse(type);
        return ParseClickHouseType(node, settings);
    }

    internal static string ExtractTypeName(SyntaxTreeNode node)
    {
        var typeName = node.Value.Trim().Trim('\'');

        if (Aliases.TryGetValue(typeName.ToUpperInvariant(), out var alias))
            typeName = alias;

        if (typeName.Contains(' '))
        {
            var parts = typeName.Split(Separator, 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 2)
            {
                typeName = parts[1].Trim();
            }
            else
            {
                throw new ArgumentException($"Cannot parse {node.Value} as type", nameof(node));
            }
        }

        return typeName;
    }

    internal static ClickHouseType ParseClickHouseType(SyntaxTreeNode node, TypeSettings settings)
    {
        var typeName = ExtractTypeName(node);

        if (node.ChildNodes.Count == 0 && SimpleTypes.TryGetValue(typeName, out var typeInfo))
        {
            if (typeName == "Dynamic")
            {
                return new DynamicType()
                {
                    TypeSettings = settings,
                };
            }

            if (typeName == "String")
            {
                return new StringType()
                {
                    ReadAsByteArray = settings.readStringsAsByteArrays,
                };
            }

            return typeInfo;
        }

        if (ParameterizedTypes.TryGetValue(typeName, out var value))
        {
            return value.Parse(node, (n) => ParseClickHouseType(n, settings), settings);
        }

        throw new ArgumentException("Unknown type: " + node.ToString());
    }

    /// <summary>
    /// Recursively build ClickHouse type from .NET complex type
    /// Supports nullable and arrays.
    /// </summary>
    /// <param name="type">framework type to map</param>
    /// <returns>Corresponding ClickHouse type</returns>
    public static ClickHouseType ToClickHouseType(Type type)
    {
        if (ReverseMapping.TryGetValue(type, out var value))
        {
            return value;
        }

        if (type.IsArray)
        {
            return new ArrayType() { UnderlyingType = ToClickHouseType(type.GetElementType()) };
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            return new ArrayType() { UnderlyingType = ToClickHouseType(type.GetGenericArguments()[0]) };
        }

        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType != null)
        {
            return new NullableType() { UnderlyingType = ToClickHouseType(underlyingType) };
        }

        // FlattenTupleGenericArgs unwraps TRest nesting for >7 elements so the inferred
        // ClickHouse types match ITuple indexing, which also flattens TRest.
        if (IsTupleType(type))
        {
            return new TupleType { UnderlyingTypes = FlattenTupleGenericArgs(type).Select(ToClickHouseType).ToArray() };
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var types = type.GetGenericArguments().Select(ToClickHouseType).ToArray();
            return new MapType { UnderlyingTypes = Tuple.Create(types[0], types[1]) };
        }

        throw new ArgumentOutOfRangeException(nameof(type), "Unknown type: " + type.ToString());
    }

    /// <summary>
    /// Infer ClickHouse type from a .NET value, inspecting the value itself for ambiguous types.
    /// <para>
    /// Some .NET types map to multiple ClickHouse types (e.g. <see cref="IPAddress"/> can be IPv4 or IPv6).
    /// The type-only overload <see cref="ToClickHouseType(Type)"/> cannot distinguish these cases.
    /// This overload resolves the ambiguity by inspecting the actual value.
    /// </para>
    /// <para>
    /// Resolution priority:
    /// <list type="number">
    ///   <item>Value-based inference: inspect the value to resolve ambiguous types (e.g. IPAddress.AddressFamily)</item>
    ///   <item>Collection element peeking: for arrays, lists, tuples, and dictionaries, check the first
    ///         element so that value-based inference propagates through nested structures</item>
    ///   <item>Type-based fallback: if it's not a collection, or if the collection is empty or if the first element is null,
    ///         fall back to <see cref="ToClickHouseType(Type)"/>.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="value">The value to infer the ClickHouse type from.</param>
    /// <returns>Corresponding ClickHouse type.</returns>
    public static ClickHouseType ToClickHouseType(object value)
    {
        ArgumentNullException.ThrowIfNull(value);

        // 1. Value-based inference for ambiguous types
        // IPAddress maps to both IPv4 and IPv6
        if (value is IPAddress ip)
            return SimpleTypes[ip.AddressFamily == AddressFamily.InterNetwork ? "IPv4" : "IPv6"];

        var type = value.GetType();

        // 2. Collection handling: peek at the first element so value-based inference propagates
        // through nested structures (e.g. List<IPAddress> or Dictionary<string, IPAddress>).
        // If the collection is empty or the first element is null, fall back to type-based inference.
        if (type.IsArray)
        {
            var array = (Array)value;
            if (array.Length > 0 && array.GetValue(0) is { } firstElement)
                return new ArrayType { UnderlyingType = ToClickHouseType(firstElement) };
            return new ArrayType { UnderlyingType = ToClickHouseType(type.GetElementType()!) };
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            var list = (System.Collections.IList)value;
            if (list.Count > 0 && list[0] is { } firstElement)
                return new ArrayType { UnderlyingType = ToClickHouseType(firstElement) };
            return new ArrayType { UnderlyingType = ToClickHouseType(type.GetGenericArguments()[0]) };
        }

        if (IsTupleType(type))
        {
            // Both System.Tuple and ValueTuple use TRest nesting for >7 elements.
            // ITuple flattens this, so we must flatten the generic args to match.
            var tuple = (ITuple)value;
            var genericArgs = FlattenTupleGenericArgs(type);
            if (genericArgs.Length != tuple.Length)
                throw new ArgumentException($"Tuple shape mismatch: expected {genericArgs.Length} generic args but ITuple reports {tuple.Length} elements");
            var items = new ClickHouseType[tuple.Length];
            for (var i = 0; i < tuple.Length; i++)
            {
                items[i] = tuple[i] is { } itemValue ? ToClickHouseType(itemValue) : ToClickHouseType(genericArgs[i]);
            }

            return new TupleType { UnderlyingTypes = items };
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            // Peek at the first entry; null keys/values fall back to type-based inference
            var dict = (System.Collections.IDictionary)value;
            if (dict.Count > 0)
            {
                var enumerator = dict.GetEnumerator();
                try
                {
                    enumerator.MoveNext();
                    var key = enumerator.Key;
                    var val = enumerator.Value;
                    var keyType = key != null ? ToClickHouseType(key) : ToClickHouseType(type.GetGenericArguments()[0]);
                    var valType = val != null ? ToClickHouseType(val) : ToClickHouseType(type.GetGenericArguments()[1]);
                    return new MapType { UnderlyingTypes = Tuple.Create(keyType, valType) };
                }
                finally
                {
                    (enumerator as IDisposable)?.Dispose();
                }
            }

            var argTypes = type.GetGenericArguments().Select(ToClickHouseType).ToArray();
            return new MapType { UnderlyingTypes = Tuple.Create(argTypes[0], argTypes[1]) };
        }

        // 3. No ambiguity for this type; delegate to type-based inference
        return ToClickHouseType(type);
    }

    /// <summary>
    /// Flattens the generic type arguments of a Tuple or ValueTuple type, unwrapping the TRest
    /// nesting that both System.Tuple and System.ValueTuple use for more than 7 elements.
    /// <para>
    /// For example, <c>Tuple&lt;int, int, int, int, int, int, int, Tuple&lt;int, string&gt;&gt;</c>
    /// is flattened to <c>[int, int, int, int, int, int, int, int, string]</c>.
    /// </para>
    /// </summary>
    private static bool IsTupleType(Type type) =>
        type.IsGenericType && (
            type.GetGenericTypeDefinition().FullName!.StartsWith("System.Tuple", StringComparison.InvariantCulture) ||
            type.GetGenericTypeDefinition().FullName!.StartsWith("System.ValueTuple", StringComparison.InvariantCulture));

    private static Type[] FlattenTupleGenericArgs(Type type)
    {
        var result = new List<Type>();
        while (IsTupleType(type))
        {
            var args = type.GetGenericArguments();
            // Both System.Tuple`8 and System.ValueTuple`8 use the 8th generic arg as TRest.
            // TRest is itself a Tuple/ValueTuple holding the remaining elements.
            if (args.Length == 8 && IsTupleType(args[7]))
            {
                for (int i = 0; i < 7; i++)
                    result.Add(args[i]);
                type = args[7];
            }
            else
            {
                result.AddRange(args);
                break;
            }
        }

        return result.ToArray();
    }
}
