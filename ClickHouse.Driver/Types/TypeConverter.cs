using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;

[assembly: InternalsVisibleTo("ClickHouse.Driver.Tests, PublicKey=00240000048000009400000006020000002400005253413100040000010001000968a6468f9d0397a051f167a25dcee773c674cf7a67629f78e884d232df23ff773fbfaba602e03eede6056b39bd6a4cddcd7e5b3ca9484bd83401d14a5e9ac5c98cbe676a1e89149816f5304f617b658440b2bd775e5ece71b5a38ceeb88e844869a376ceea71cbb6393b2ac14e506b92267a3cbcd6e7dc93ff6c750d53a5c7")] // assembly-level tag to expose below classes to tests

namespace ClickHouse.Driver.Types;

internal static class TypeConverter
{
    private static readonly Dictionary<string, ClickHouseType> SimpleTypes = [];
    private static readonly Dictionary<string, ParameterizedType> ParameterizedTypes = [];
    private static readonly Dictionary<Type, ClickHouseType> ReverseMapping = [];

    // 10^0 to 10^77, and every power of ten a decimal holds, to count the digits of a mantissa (see DigitCount).
    private static readonly BigInteger[] PowersOfTen = Enumerable.Range(0, 78).Select(n => BigInteger.Pow(10, n)).ToArray();
    private static readonly decimal[] DecimalPowersOfTen = PowersOfTen.Take(29).Select(p => (decimal)p).ToArray();

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

        // 'Identifier' is ClickHouse's server-side query-parameter pseudo-type ({name:Identifier}),
        // used to bind a database/table/column name. It is never a column data type, so it is
        // registered for parameter type-name parsing only and intentionally excluded from
        // ReverseMapping — a .NET string value must keep inferring as String, not Identifier.
        RegisterParameterOnlyType<IdentifierType>();

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
        ReverseMapping.Add(typeof(DateOnly), new DateType());
        // TimeOnly is the natural time-of-day analogue of TimeSpan; infer it as Time64(7) to match.
        ReverseMapping.Add(typeof(TimeOnly), new Time64Type { Scale = 7 });
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

    // Registers a type that can appear only as a query-parameter type hint (never a column type).
    // Unlike RegisterPlainType, it is deliberately left out of ReverseMapping so that inferring a
    // ClickHouse type from a .NET value is unaffected (e.g. a string still infers as String).
    private static void RegisterParameterOnlyType<T>()
        where T : ClickHouseType, new()
    {
        var type = new T();
        var name = string.Intern(type.ToString());
        SimpleTypes.Add(name, type);
    }

    public static ClickHouseType ParseClickHouseType(string type, TypeSettings settings)
    {
        var node = Parser.Parse(type);
        return ParseClickHouseType(node, settings);
    }

    internal static string ExtractTypeName(SyntaxTreeNode node)
    {
        var typeName = node.Value.Trim().Trim('\'');

        // Looked up before the named element check: a multi-word alias such as DOUBLE PRECISION
        // contains a space too.
        if (Aliases.TryGetValue(typeName.ToUpperInvariant(), out var alias))
            return alias;

        if (typeName.Contains(' '))
        {
            // Named element, e.g. "id Int32" in Tuple(id Int32) — strip the element name. A name
            // which needs quoting is enclosed in backticks by the server and may itself contain
            // spaces, so the separator is located past the quoted identifier.
            var separator = typeName.IndexOfNameTypeSeparator();
            if (separator > 0)
            {
                typeName = typeName.Substring(separator + 1).Trim();
            }
            else
            {
                throw new ArgumentException($"Cannot parse {node.Value} as type", nameof(node));
            }

            // The element type can be an alias too, e.g. JSON in Tuple(x JSON), as the server reports it.
            if (Aliases.TryGetValue(typeName.ToUpperInvariant(), out alias))
                typeName = alias;
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
            // Rank>1 (e.g. byte[,]) wraps in N nested ArrayType layers; the wire format is jagged.
            ClickHouseType result = ToClickHouseType(type.GetElementType());
            var rank = type.GetArrayRank();
            for (var i = 0; i < rank; i++)
            {
                result = new ArrayType { UnderlyingType = result };
            }
            return result;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            // A list of key-value pairs is the MapReadMode.KeyValuePairs representation of a map
            var elementType = type.GetGenericArguments()[0];
            if (IsKeyValuePairType(elementType))
            {
                var pairTypes = elementType.GetGenericArguments().Select(ToClickHouseType).ToArray();
                return new MapType { UnderlyingTypes = Tuple.Create(pairTypes[0], pairTypes[1]) };
            }

            return new ArrayType() { UnderlyingType = ToClickHouseType(elementType) };
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

        // Instant-bearing DateTime values infer as DateTime('UTC') so the wire wall-clock
        // formatted in UTC is parsed unambiguously by the server in UTC, not session_timezone (issue #350).
        // Unspecified DateTime falls through to bare DateTime (wall-clock semantics).
        // This applies inside composite structures too: e.g. an array of UTC DateTime values
        // infers as Array(DateTime('UTC')) via the recursive element peek below.
        if (value is DateTime { Kind: DateTimeKind.Utc or DateTimeKind.Local }
            || value is DateTimeOffset)
            return new DateTimeType { TimeZone = NodaTime.DateTimeZone.Utc };

        var type = value.GetType();

        // 2. Collection handling: peek at the first element so value-based inference propagates
        // through nested structures (e.g. List<IPAddress> or Dictionary<string, IPAddress>).
        // If the collection is empty or the first element is null, fall back to type-based inference.
        if (type.IsArray)
        {
            var array = (Array)value;
            var rank = type.GetArrayRank();
            // Rank-aware first-element peek; Array.GetValue(int) throws for rank > 1.
            // Honour per-axis lower bounds so non-zero-bound arrays (Array.CreateInstance
            // with lowerBounds) don't throw IndexOutOfRangeException on the [0,0,...] peek.
            object firstElement = null;
            if (array.Length > 0)
            {
                var indices = new int[rank];
                for (var d = 0; d < rank; d++)
                    indices[d] = array.GetLowerBound(d);
                firstElement = array.GetValue(indices);
            }
            ClickHouseType inner = firstElement is not null
                ? ToClickHouseType(firstElement)
                : ToClickHouseType(type.GetElementType()!);
            for (var i = 0; i < rank; i++)
            {
                inner = new ArrayType { UnderlyingType = inner };
            }
            return inner;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            var list = (System.Collections.IList)value;

            // A list of key-value pairs is the MapReadMode.KeyValuePairs representation of a map;
            // peek at the first pair so value-based inference propagates into keys and values
            var listElementType = type.GetGenericArguments()[0];
            if (IsKeyValuePairType(listElementType))
            {
                var pairTypes = listElementType.GetGenericArguments();
                var firstPair = list.Count > 0 ? MapType.EnumerateEntries(list).First() : default;
                var mapKeyType = firstPair.Key is { } pairKey ? ToClickHouseType(pairKey) : ToClickHouseType(pairTypes[0]);
                var mapValueType = firstPair.Value is { } pairValue ? ToClickHouseType(pairValue) : ToClickHouseType(pairTypes[1]);
                return new MapType { UnderlyingTypes = Tuple.Create(mapKeyType, mapValueType) };
            }

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
    /// Infers the ClickHouse <c>Decimal</c> type for <paramref name="value"/> where the type is chosen
    /// from the value rather than a column definition, e.g. when writing into a <c>Dynamic</c> column.
    /// <para>
    /// The width is the narrowest of Decimal32/64/128/256 whose precision P holds both the value's
    /// integer digits and its scale. The scale is then set to <c>P - integerDigits</c>, so values that
    /// differ only in their number of fractional digits share one type (1.2, 1.23 and 1.2345 are all
    /// <c>Decimal32(8)</c>). The numeric value is kept exactly.
    /// </para>
    /// </summary>
    /// <param name="value">The decimal value to infer a type for.</param>
    /// <returns>The inferred <see cref="DecimalType"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The value needs more than 76 digits without its trailing fractional zeros, which exceeds the capacity of
    /// ClickHouse's widest Decimal256.
    /// </exception>
    internal static ClickHouseType InferDecimalType(ClickHouseDecimal value)
    {
        var scale = value.Scale;
        var integerDigits = IntegerDigits(value);
        var precision = integerDigits + scale;
        if (precision > 76)
        {
            // Trailing fractional zeros (all the digits of a zero) are exact at a smaller scale, so only the
            // other digits must fit Decimal256.
            var significantPrecision = integerDigits + SignificantScale(value);
            if (significantPrecision > 76)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(value),
                    value,
                    $"Decimal value requires a precision of {significantPrecision} digits, which exceeds the maximum of 76 supported by ClickHouse (Decimal256).");
            }

            precision = 76;
        }

        return NarrowestDecimalType(precision, integerDigits);
    }

    /// <summary>
    /// Infers one ClickHouse <c>Decimal</c> type that holds all of <paramref name="values"/> exactly, e.g. for
    /// the elements of a collection written into a <c>Dynamic</c> column.
    /// <para>
    /// The policy is that of <see cref="InferDecimalType(ClickHouseDecimal)"/>, applied to the largest
    /// integer-digit count and the largest scale among the values: the width is the narrowest whose
    /// precision P holds both, and the scale is <c>P - integerDigits</c>. A single value gets the same type
    /// as from <see cref="InferDecimalType(ClickHouseDecimal)"/>.
    /// </para>
    /// </summary>
    /// <param name="values">The decimal values to infer a common type for.</param>
    /// <returns>The inferred <see cref="DecimalType"/>, or <c>null</c> if <paramref name="values"/> is empty.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The values together need more than 76 digits without their trailing fractional zeros, which exceeds the
    /// capacity of ClickHouse's widest Decimal256.
    /// </exception>
    internal static ClickHouseType InferCommonDecimalType(IEnumerable<ClickHouseDecimal> values)
    {
        var hasValues = false;
        var integerDigits = 0;
        var scale = 0;
        foreach (var value in values)
        {
            hasValues = true;
            integerDigits = Math.Max(integerDigits, IntegerDigits(value));
            scale = Math.Max(scale, value.Scale);
        }

        if (!hasValues)
            return null;

        var precision = integerDigits + scale;
        if (precision > 76)
        {
            // As for a single value, trailing fractional zeros are exact at a smaller scale, so only the
            // other digits must fit Decimal256. This rare path enumerates the values a second time.
            var significantScale = values.Max(SignificantScale);
            if (integerDigits + significantScale > 76)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(values),
                    $"Decimal values require a common precision of {integerDigits + significantScale} digits ({integerDigits} integer and {significantScale} fractional), which exceeds the maximum of 76 supported by ClickHouse (Decimal256).");
            }

            precision = 76;
        }

        return NarrowestDecimalType(precision, integerDigits);
    }

    /// <summary>
    /// <see cref="InferCommonDecimalType(IEnumerable{ClickHouseDecimal})"/> for <see cref="decimal"/> values, which
    /// are read from their bits: the conversion to <see cref="ClickHouseDecimal"/> allocates for each value whose
    /// mantissa exceeds 31 bits.
    /// </summary>
    /// <param name="values">The decimal values to infer a common type for.</param>
    /// <returns>The inferred <see cref="DecimalType"/>, or <c>null</c> if <paramref name="values"/> is empty.</returns>
    internal static ClickHouseType InferCommonDecimalType(IEnumerable<decimal> values)
    {
        var hasValues = false;
        var integerDigits = 0;
        var scale = 0;
        Span<int> bits = stackalloc int[4];
        foreach (var value in values)
        {
            hasValues = true;
            decimal.GetBits(value, bits);
            var valueScale = (bits[3] >> 16) & 0x7F;
            var digits = DigitCount(DecimalPowersOfTen, new decimal(bits[0], bits[1], bits[2], false, 0));
            integerDigits = Math.Max(integerDigits, Math.Max(digits - valueScale, 0));
            scale = Math.Max(scale, valueScale);
        }

        // A decimal has at most 29 digits and a scale of at most 28, so no set of them needs more than 76 digits.
        return hasValues ? NarrowestDecimalType(integerDigits + scale, integerDigits) : null;
    }

    private static int IntegerDigits(ClickHouseDecimal value)
    {
        var magnitude = BigInteger.Abs(value.Mantissa);
        var digits = magnitude > PowersOfTen[PowersOfTen.Length - 1]
            ? magnitude.ToString(CultureInfo.InvariantCulture).Length
            : DigitCount(PowersOfTen, magnitude);
        return Math.Max(digits - value.Scale, 0);
    }

    // The number of decimal digits (1 for zero) of a non-negative integer below ten times the largest power of ten
    // in the table. This runs for each element of a collection, so it searches the powers of ten instead of
    // formatting the number to a string.
    private static int DigitCount<T>(T[] powersOfTen, T magnitude)
    {
        var index = Array.BinarySearch(powersOfTen, magnitude);
        return index >= 0 ? index + 1 : Math.Max(~index, 1);
    }

    // The scale without the trailing zeros of the fraction, which a smaller scale also holds exactly.
    private static int SignificantScale(ClickHouseDecimal value)
    {
        if (value.Mantissa.IsZero)
            return 0;

        var mantissa = value.Mantissa;
        var scale = value.Scale;
        while (scale > 0)
        {
            var quotient = BigInteger.DivRem(mantissa, 10, out var remainder);
            if (!remainder.IsZero)
                break;
            mantissa = quotient;
            scale--;
        }

        return scale;
    }

    // The narrowest Decimal width with at least `precision` digits, at the largest scale that keeps `integerDigits`.
    private static ClickHouseType NarrowestDecimalType(int precision, int integerDigits) => precision switch
    {
        <= 9 => new Decimal32Type { Scale = 9 - integerDigits },
        <= 18 => new Decimal64Type { Scale = 18 - integerDigits },
        <= 38 => new Decimal128Type { Scale = 38 - integerDigits },
        _ => new Decimal256Type { Scale = 76 - integerDigits },
    };

    private static bool IsKeyValuePairType(Type type) =>
        type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>);

    private static bool IsTupleType(Type type) =>
        type.IsGenericType && (
            type.GetGenericTypeDefinition().FullName!.StartsWith("System.Tuple", StringComparison.InvariantCulture) ||
            type.GetGenericTypeDefinition().FullName!.StartsWith("System.ValueTuple", StringComparison.InvariantCulture));

    /// <summary>
    /// Flattens the generic type arguments of a Tuple or ValueTuple type, unwrapping the TRest
    /// nesting that both System.Tuple and System.ValueTuple use for more than 7 elements.
    /// <para>
    /// For example, <c>Tuple&lt;int, int, int, int, int, int, int, Tuple&lt;int, string&gt;&gt;</c>
    /// is flattened to <c>[int, int, int, int, int, int, int, int, string]</c>.
    /// </para>
    /// </summary>
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
