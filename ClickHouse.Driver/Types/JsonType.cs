using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
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
    /// <summary>
    /// Shared DynamicType instance for writing unhinted values.
    /// </summary>
    private static readonly DynamicType DynamicTypeInstance = new();

    /// <summary>
    /// Memory stream manager for temporary buffers during POCO serialization.
    /// </summary>
    private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new();

    internal TypeSettings TypeSettings { get; init; }

    public override Type FrameworkType => TypeSettings.jsonReadMode == JsonReadMode.String
        ? typeof(string)
        : typeof(JsonObject);

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

    public override object Read(ExtendedBinaryReader reader)
    {
        // When JsonReadMode.String is configured, ClickHouse sends JSON as a plain string
        // (via output_format_binary_write_json_as_string=1 setting)
        if (TypeSettings.jsonReadMode == JsonReadMode.String)
            return reader.ReadString();

        return ReadAsJsonObject(reader);
    }

    private object ReadAsJsonObject(ExtendedBinaryReader reader)
    {
        JsonObject root = new();

        var nfields = reader.Read7BitEncodedInt();
        for (int i = 0; i < nfields; i++)
        {
            var current = root;
            var name = reader.ReadString();

            HintedTypes.TryGetValue(name, out var hintedType);
            var jsonNode = ReadJsonNode(reader, hintedType);
            if (jsonNode is null && hintedType is null)
            {
                // A dynamic path only exists in the rows that have a value for it, and the server
                // omits it from its own JSON rendering when the value is null. A typed path, in
                // contrast, is declared by the column type and is rendered with an explicit null,
                // so it must be materialized to keep "absent" distinguishable from "null".
                continue;
            }

            var pathParts = name.Split('.');
            foreach (var part in pathParts.SkipLast1(1))
            {
                if (current[part] is { } existing)
                {
                    current = (JsonObject)existing;
                }
                else
                {
                    // Either the parent is not there yet, or it holds the null of an overlapping
                    // typed leaf path (ClickHouse allows both `a` and `a.b` to be typed); the
                    // subtree replaces that null so the deeper path stays readable.
                    var newCurrent = new JsonObject();
                    current[part] = newCurrent;
                    current = newCurrent;
                }
            }

            var leaf = pathParts.Last();
            if (jsonNode is null && current[leaf] is JsonObject)
            {
                // Mirror of the walk above for the opposite path order: a typed leaf path's null
                // must not erase the subtree an overlapping deeper typed path already filled in.
                continue;
            }

            current[leaf] = jsonNode;
        }

        return root;
    }

    public override ParameterizedType Parse(
        SyntaxTreeNode node,
        Func<SyntaxTreeNode, ClickHouseType> parseClickHouseType,
        TypeSettings settings)
    {
        var hintedTypes = node.ChildNodes
            .Where(childNode => !IsJsonSetting(childNode.Value))
            .Select(childNode =>
            {
                var separator = childNode.Value.IndexOfNameTypeSeparator();
                var hintedTypeName = separator > 0 ? childNode.Value.Substring(separator + 1).Trim() : string.Empty;
                if (separator <= 0 || hintedTypeName.Length == 0)
                {
                    throw new SerializationException($"Unsupported path in JSON hint: {childNode.Value}");
                }

                var hintTypeSyntaxTreeNode = new SyntaxTreeNode
                {
                    Value = hintedTypeName,
                };

                foreach (var childNodeChildNode in childNode.ChildNodes)
                {
                    hintTypeSyntaxTreeNode.ChildNodes.Add(childNodeChildNode);
                }

                return (
                    path: childNode.Value.Substring(0, separator).DiscloseColumnName(),
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

    private static bool IsJsonSetting(string value) =>
        IsAssignment(value, "max_dynamic_paths")
        || IsAssignment(value, "max_dynamic_types")
        || value.StartsWith("skip ", StringComparison.OrdinalIgnoreCase);

    private static bool IsAssignment(string value, string settingName)
    {
        if (!value.StartsWith(settingName, StringComparison.OrdinalIgnoreCase))
            return false;

        var remainder = value.AsSpan(settingName.Length).TrimStart();
        return !remainder.IsEmpty && remainder[0] == '=';
    }

    public override string ToString() => Name;

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        // String mode: serialize everything to JSON string, let server parse
        if (TypeSettings.jsonWriteMode == JsonWriteMode.String)
        {
            WriteAsString(writer, value);
            return;
        }

        // Binary mode: only POCOs supported
        if (value is string or JsonNode)
        {
            throw new ArgumentException(
                $"String and JsonNode inputs require JsonWriteMode.String. Current value: {TypeSettings.jsonWriteMode}. " +
                $"Either use JsonWriteMode.String in your connection string, or use a registered POCO type.");
        }

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
    /// Write any value as a JSON string.
    /// This will only work with input_format_binary_read_json_as_string=1
    /// </summary>
    private static void WriteAsString(ExtendedBinaryWriter writer, object value)
    {
        var jsonString = value switch
        {
            string s => s,
            JsonNode node => node.ToJsonString(),
            _ => JsonSerializer.Serialize(value) // POCO
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
            var propertyInfos = TypeSettings.jsonTypeRegistry?.GetProperties(type);
            if (propertyInfos == null)
            {
                throw new ClickHouseJsonSerializationException(type);
            }

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
    /// Uses the type from the column definition to write the given value.
    /// </summary>
    private static void WriteHintedValue(ExtendedBinaryWriter writer, object value, ClickHouseType hintedType)
    {
        if ((value is null || value is DBNull) && hintedType is NullableType)
        {
            // Nullable types handle null via their own Write method (writes byte 1)
            hintedType.Write(writer, null);
            return;
        }

        hintedType.Write(writer, value);
    }

    /// <summary>
    /// For cases when there is no type hint, we delegate to DynamicType
    /// which handles type inference and binary encoding.
    /// </summary>
    private static void WriteUnhintedValue(ExtendedBinaryWriter writer, object value)
        => DynamicTypeInstance.Write(writer, value);

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
            var key = DecodeString(mapType.KeyType.Read(reader));
            var value = ReadJsonNode(reader, mapType.ValueType);
            obj[key] = value;
        }
        return obj;
    }

    /// <summary>
    /// Decodes a String or FixedString value into text. Under <c>ReadStringsAsByteArrays</c> those
    /// types read as a <see cref="byte"/> array, but JSON strings are text and <see cref="JsonValue"/>
    /// has no byte-array form, so they are decoded either way. Lenient: invalid bytes become U+FFFD.
    /// </summary>
    private static string DecodeString(object value)
        => value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : (string)value;

    /// <summary>
    /// Whether values of this type are text rather than raw bytes. Decided from the ClickHouse type,
    /// not the CLR type: <c>Array(UInt8)</c> also reads as a <see cref="byte"/> array, so decoding on
    /// <c>byte[]</c> alone would corrupt it. <c>Variant</c>/<c>Dynamic</c> are false because their
    /// subtype is only known per value.
    /// </summary>
    private static bool IsTextBacked(ClickHouseType type) => type switch
    {
        StringType or FixedStringType => true,
        LowCardinalityType lc => IsTextBacked(lc.UnderlyingType),
        NullableType nt => IsTextBacked(nt.UnderlyingType),
        SimpleAggregateFunctionType sa => IsTextBacked(sa.UnderlyingType),
        _ => false,
    };

    private static JsonValue ReadJsonFixedString(ExtendedBinaryReader reader, ClickHouseType type)
        => JsonValue.Create(DecodeString(type.Read(reader)));

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

            // Without this arm a byte[] from a string path falls through to the JsonSerializer
            // default below, which renders it as base64.
            byte[] bytes when IsTextBacked(type) => JsonValue.Create(DecodeString(bytes)),
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
