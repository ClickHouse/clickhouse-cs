using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;
using Microsoft.IO;

namespace ClickHouse.Driver.Types;

internal class JsonType : ParameterizedType
{
    private static readonly string[] JsonSettingNames =
    [
        "max_dynamic_paths",
        "max_dynamic_types",
        "skip "
    ];

    /// <summary>
    /// Cache for reflected property metadata per type.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, JsonTypeCache> PropertyCache = new();

    /// <summary>
    /// Cache for inferred ClickHouse types from .NET types.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, ClickHouseType> InferredTypeCache = new();

    /// <summary>
    /// Memory stream manager for temporary buffers during POCO serialization.
    /// </summary>
    private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new();

    /// <summary>
    /// Cached type information for JSON serialization.
    /// </summary>
    private sealed class JsonTypeCache
    {
        public JsonPropertyInfo[] Properties { get; init; }
    }

    internal TypeSettings TypeSettings { get; init; }

    public override Type FrameworkType => typeof(JsonObject);

    public override string Name => "Json";

    public Dictionary<string, ClickHouseType> HintedTypes { get; }

    public JsonType()
        : this(new Dictionary<string, ClickHouseType>())
    {
    }

    internal JsonType(Dictionary<string, ClickHouseType> hintedTypes)
    {
        HintedTypes = hintedTypes;
    }

    /// <summary>
    /// Cached property information for JSON serialization.
    /// </summary>
    private sealed class JsonPropertyInfo
    {
        public PropertyInfo Property { get; init; }

        public string JsonPath { get; init; }

        public bool IsIgnored { get; init; }

        public bool IsNestedObject { get; init; }
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        JsonObject root = new();

        var nfields = reader.Read7BitEncodedInt();
        for (int i = 0; i < nfields; i++)
        {
            var current = root;
            var name = reader.ReadString();

            HintedTypes.TryGetValue(name, out var hintedType);
            if (ReadJsonNode(reader, hintedType) is not { } jsonNode)
            {
                continue;
            }

            var pathParts = name.Split('.');
            foreach (var part in pathParts.SkipLast1(1))
            {
                if (current.ContainsKey(part))
                {
                    current = (JsonObject)current[part];
                }
                else
                {
                    var newCurrent = new JsonObject();
                    current.Add(part, newCurrent);
                    current = newCurrent;
                }
            }

            current[pathParts.Last()] = jsonNode;
        }

        return root;
    }

    public override ParameterizedType Parse(
        SyntaxTreeNode node,
        Func<SyntaxTreeNode, ClickHouseType> parseClickHouseType,
        TypeSettings settings)
    {
        var hintedTypes = node.ChildNodes
            .Where(childNode => !JsonSettingNames.Any(jsonSettingName => childNode.Value.StartsWith(jsonSettingName, StringComparison.OrdinalIgnoreCase)))
            .Select(childNode =>
            {
                var hintParts = childNode.Value.Split(' ');
                if (hintParts.Length != 2)
                {
                    throw new SerializationException($"Unsupported path in JSON hint: {childNode.Value}");
                }

                var hintTypeSyntaxTreeNode = new SyntaxTreeNode
                {
                    Value = hintParts[1],
                };

                foreach (var childNodeChildNode in childNode.ChildNodes)
                {
                    hintTypeSyntaxTreeNode.ChildNodes.Add(childNodeChildNode);
                }

                return (
                    path: hintParts[0].Trim('`'),
                    type: parseClickHouseType(hintTypeSyntaxTreeNode));
            })
            .ToDictionary(
                hint => hint.path,
                hint => hint.type);

        return new JsonType(hintedTypes)
        {
            TypeSettings = settings,
        };
    }

    public override string ToString() => Name;

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        // String and JsonNode inputs: write as string, let server parse
        if (value is string || value is JsonNode)
        {
            WriteAsString(writer, value);
            return;
        }

        // POCO with hints
        WritePocoWithHints(writer, value);
    }

    private void WritePocoWithHints(ExtendedBinaryWriter writer, object value)
    {
        // Single-pass approach: write fields to temp buffer while counting, then copy to output
        using var tempStream = MemoryStreamManager.GetStream();
        using var tempWriter = new ExtendedBinaryWriter(tempStream);

        // Track visited objects to detect circular references
        var visited = new HashSet<object>(ObjectReferenceEqualityComparer.Instance);
        var fieldCount = WritePocoFields(tempWriter, value, string.Empty, visited);

        writer.Write7BitEncodedInt(fieldCount);
        tempStream.Position = 0;
        tempStream.CopyTo(writer.BaseStream);
    }

    /// <summary>
    /// Write a json string or JsonNode as a string.
    /// This will only work with input_format_binary_read_json_as_string=1
    /// </summary>
    private static void WriteAsString(ExtendedBinaryWriter writer, object value)
    {
        var jsonString = value switch
        {
            string s => s,
            JsonNode node => node.ToJsonString(),
            _ => throw new ArgumentException($"Expected string or JsonNode, got {value.GetType().Name}")
        };

        writer.Write(jsonString);
    }

    /// <summary>
    /// Writes POCO fields to the writer and returns the number of fields written.
    /// </summary>
    /// <param name="visited">Set of already visited objects to detect circular references.</param>
    private int WritePocoFields(ExtendedBinaryWriter writer, object poco, string prefix, HashSet<object> visited)
    {
        // Check for circular reference
        if (!visited.Add(poco))
        {
            throw new InvalidOperationException(
                $"Circular reference detected at path '{prefix}'. " +
                "JSON serialization does not support circular object references.");
        }

        try
        {
            var type = poco.GetType();
            var propertyInfos = GetCachedPropertyInfo(type);
            int count = 0;

            foreach (var propInfo in propertyInfos)
            {
                if (propInfo.IsIgnored)
                    continue;
                string path = GetJsonPath(prefix, propInfo);
                var value = propInfo.Property.GetValue(poco);

                if (value is null)
                {
                    // Only write nulls for hinted Nullable types
                    // ClickHouse doesn't allow Nullable inside dynamic JSON paths (Variant type)
                    if (HintedTypes.TryGetValue(path, out ClickHouseType hintedType) && hintedType is NullableType)
                    {
                        writer.Write(path);
                        WriteHintedValue(writer, null, hintedType);
                        count++;
                    }
                }
                else if (propInfo.IsNestedObject)
                {
                    // Recurse into sub-object
                    // Note: Collections (IEnumerable) are excluded from IsNestedObject, so they go to the else branch
                    count += WritePocoFields(writer, value, path, visited);
                }
                else
                {
                    // Write out a value
                    HintedTypes.TryGetValue(path, out ClickHouseType hintedType);
                    writer.Write(path);
                    if (hintedType != null)
                    {
                        WriteHintedValue(writer, value, hintedType);
                    }
                    else
                    {
                        WriteUnhintedValue(writer, value);
                    }
                    count++;
                }
            }

            return count;
        }
        finally
        {
            // Remove from visited when leaving this object's scope
            // This allows the same object to appear in different branches (diamond pattern)
            visited.Remove(poco);
        }
    }

    private static string GetJsonPath(string prefix, JsonPropertyInfo propInfo)
    {
        return string.IsNullOrEmpty(prefix)
            ? propInfo.JsonPath
            : $"{prefix}.{propInfo.JsonPath}";
    }

    /// <summary>
    /// Gets cached type information, computing it if not already cached.
    /// </summary>
    private static JsonTypeCache GetCachedTypeInfo(Type type)
    {
        return PropertyCache.GetOrAdd(type, t =>
        {
            var properties = t.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var result = new List<JsonPropertyInfo>(properties.Length);

            foreach (var property in properties)
            {
                if (!property.CanRead)
                    continue;

                var ignoreAttr = property.GetCustomAttribute<ClickHouseJsonIgnoreAttribute>();
                var pathAttr = property.GetCustomAttribute<ClickHouseJsonPathAttribute>();

                var jsonPath = pathAttr?.Path ?? property.Name;
                var propertyType = property.PropertyType;
                var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
                var isNested = IsNestedObject(underlyingType);

                result.Add(new JsonPropertyInfo
                {
                    Property = property,
                    JsonPath = jsonPath,
                    IsIgnored = ignoreAttr != null,
                    IsNestedObject = isNested,
                });
            }

            return new JsonTypeCache
            {
                Properties = result.ToArray(),
            };
        });
    }

    /// <summary>
    /// Gets cached property information for a type.
    /// </summary>
    private static JsonPropertyInfo[] GetCachedPropertyInfo(Type type)
    {
        return GetCachedTypeInfo(type).Properties;
    }

    private static bool IsNestedObject(Type type)
    {
        return !type.IsPrimitive
            && type != typeof(string)
            && type != typeof(decimal)
            && type != typeof(DateTime)
            && type != typeof(DateTimeOffset)
            && type != typeof(TimeSpan)
#if NET6_0_OR_GREATER
            && type != typeof(DateOnly)
#endif
            && type != typeof(Guid)
            && type != typeof(BigInteger)
            && type != typeof(ClickHouseDecimal)
            && type != typeof(IPAddress)
            && !typeof(IEnumerable).IsAssignableFrom(type)
            && !type.IsEnum;
    }

    /// <summary>
    /// Uses the type from the column definition to write the given value.
    /// </summary>
    private static void WriteHintedValue(ExtendedBinaryWriter writer, object value, ClickHouseType hintedType)
    {
        if (value is null || value is DBNull)
        {
            if (hintedType is NullableType)
            {
                // Nullable types handle null via their own Write method (writes byte 1)
                hintedType.Write(writer, null);
            }
            else
            {
                writer.Write(BinaryTypeIndex.Nothing);
            }
            return;
        }

        hintedType.Write(writer, value);
    }

    /// <summary>
    /// For cases when there is no type hint, we use type inference to write the value.
    /// </summary>
    private static void WriteUnhintedValue(ExtendedBinaryWriter writer, object value)
    {
        var inferredType = GetCachedInferredType(value);

        // For dynamic paths in json (ie those without a defined type in the table),
        // the value must be preceded by the binary encoding type index.
        BinaryTypeEncoder.WriteTypeHeader(writer, inferredType);
        inferredType.Write(writer, value);
    }

    /// <summary>
    /// Use the type converter to infer the ClickHouse type from the native type, and cache the results.
    /// </summary>
    private static ClickHouseType GetCachedInferredType(object value)
        => GetCachedInferredType(value.GetType());

    /// <summary>
    /// Use the type converter to infer the ClickHouse type from the .NET type, and cache the results.
    /// </summary>
    private static ClickHouseType GetCachedInferredType(Type type)
        => InferredTypeCache.GetOrAdd(type, TypeConverter.ToClickHouseType);

    internal JsonNode ReadJsonNode(ExtendedBinaryReader reader, ClickHouseType hintedType)
    {
        var type = hintedType ?? BinaryTypeDecoder.FromByteCode(reader, TypeSettings);
        return type switch
        {
            ArrayType at => ReadJsonArray(reader, at),
            MapType mt => ReadJsonMap(reader, mt),
            FixedStringType => ReadJsonFixedString(reader, type),
            _ => ReadJsonValue(reader, type),
        };
    }

    private JsonArray ReadJsonArray(ExtendedBinaryReader reader, ArrayType arrayType)
    {
        var count = reader.Read7BitEncodedInt();
        var array = new JsonArray();
        for (int i = 0; i < count; i++)
        {
            array.Add(ReadJsonNode(reader, arrayType.UnderlyingType));
        }

        return array;
    }

    private JsonObject ReadJsonMap(ExtendedBinaryReader reader, MapType mapType)
    {
        if (mapType.KeyType is not StringType)
        {
            throw new NotSupportedException($"JSON Map keys must be strings, got {mapType.KeyType}");
        }

        var count = reader.Read7BitEncodedInt();
        var obj = new JsonObject();
        for (int i = 0; i < count; i++)
        {
            var key = (string)mapType.KeyType.Read(reader);
            var value = ReadJsonNode(reader, mapType.ValueType);
            obj[key] = value;
        }
        return obj;
    }

    private static JsonValue ReadJsonFixedString(ExtendedBinaryReader reader, ClickHouseType type)
    {
        var value = type.Read(reader);
        return JsonValue.Create(Encoding.UTF8.GetString((byte[])value));
    }

    private static JsonNode ReadJsonValue(ExtendedBinaryReader reader, ClickHouseType type)
    {
        var value = type.Read(reader);
        if (value is DBNull)
            value = null;

        // Handle specific types that need special serialization to JSON
        // For types that don't have a direct JsonValue representation, convert to string
        return value switch
        {
            null => null,
            JsonObject jo => jo,
            string s => JsonValue.Create(s),
            bool b => JsonValue.Create(b),
            byte by => JsonValue.Create(by),
            sbyte sb => JsonValue.Create(sb),
            short sh => JsonValue.Create(sh),
            ushort us => JsonValue.Create(us),
            int i => JsonValue.Create(i),
            uint ui => JsonValue.Create(ui),
            long l => JsonValue.Create(l),
            ulong ul => JsonValue.Create(ul),
            float f => JsonValue.Create(f),
            double d => JsonValue.Create(d),
            decimal dec => JsonValue.Create(dec),
            DateTime dt => JsonValue.Create(dt),
            // Types that need string representation
            BigInteger bi => JsonValue.Create(bi.ToString(CultureInfo.InvariantCulture)),
            Guid guid => JsonValue.Create(guid.ToString()),
            IPAddress ip => JsonValue.Create(ip.ToString()),
            ClickHouseDecimal chDec => JsonValue.Create(chDec.ToString(CultureInfo.InvariantCulture)),
            // Default: try JsonSerializer for complex types
            _ => JsonValue.Create(JsonSerializer.SerializeToElement(value))
        };
    }

    /// <summary>
    /// Reference equality comparer for cycle detection.
    /// Uses RuntimeHelpers.GetHashCode for identity-based hashing.
    /// </summary>
    private sealed class ObjectReferenceEqualityComparer : IEqualityComparer<object>
    {
        public static readonly ObjectReferenceEqualityComparer Instance = new();

        private ObjectReferenceEqualityComparer() { }

        public new bool Equals(object x, object y) => ReferenceEquals(x, y);

        public int GetHashCode(object obj) => RuntimeHelpers.GetHashCode(obj);
    }
}
