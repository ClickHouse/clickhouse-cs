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

        // A Map path, or a Dynamic one holding an object, decodes to a JsonObject which is that
        // path's value — not a container built here to hold deeper paths. The two are the same
        // type, so the values are tracked by reference. Allocated only where such a path exists,
        // which a column of plain scalar paths never has.
        HashSet<object> objectValues = null;

        // When set, an overlap keeps whichever value the row carries last and drops the other,
        // rather than reporting that neither can be dropped without losing data.
        var allowDuplicateKeys = TypeSettings.allowDuplicateJsonKeys;

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
            var depth = 0;
            var givesWay = false;
            foreach (var part in pathParts.SkipLast1(1))
            {
                depth++;
                var occupant = current[part];

                if (occupant is JsonObject subtree && (allowDuplicateKeys || !IsObjectValue(objectValues, subtree)))
                {
                    // A container built earlier in this row for an overlapping deeper path, or —
                    // when duplicate keys are allowed — an object value this path merges into.
                    current = subtree;
                }
                else if (occupant is null || (occupant is JsonObject empty && IsAllNull(empty, out _)))
                {
                    // Nothing is there yet, or what is there holds no value — the null of an
                    // overlapping leaf path, or an empty object value. ClickHouse allows both `a`
                    // and `a.b` to be declared; the subtree replaces it so the deeper path stays
                    // readable.
                    var newCurrent = new JsonObject();
                    current[part] = newCurrent;
                    current = newCurrent;
                }
                else if (HoldsNoValue(jsonNode))
                {
                    // The parent holds a value, but this path holds none — a null, or an empty
                    // object value — so it gives way instead of colliding with it, the same rule
                    // the leaf applies to a subtree it lands on. Nothing is lost by dropping it,
                    // and no container was built for it: a part which reaches this branch has an
                    // occupant, so it was already there before this path was walked.
                    givesWay = true;
                    break;
                }
                else
                {
                    // The parent holds a value of its own, so this row needs both a value and a
                    // subtree under one key. Nothing can be dropped without losing data.
                    throw OverlappingPathsException(string.Join(".", pathParts.Take(depth)), name);
                }
            }

            if (givesWay)
            {
                continue;
            }

            var leaf = pathParts.Last();
            if (current[leaf] is JsonObject occupied)
            {
                // A deeper overlapping path was read first and put its subtree here.
                if (HoldsNoValue(jsonNode))
                {
                    // This path holds no value — a null, or an empty object value — so it must not
                    // erase the subtree.
                    continue;
                }

                if (!allowDuplicateKeys && !IsAllNull(occupied, out var occupiedPath))
                {
                    throw OverlappingPathsException(name, $"{name}.{occupiedPath}");
                }

                // The subtree carries no value either, so this path's value replaces it.
            }

            if (!allowDuplicateKeys && jsonNode is JsonObject objectValue)
            {
                // Mark it as a value so that an overlapping deeper path read later throws instead
                // of descending into it and merging the two. Only the throw consults this set, so
                // it stays unallocated when duplicate keys are allowed.
                (objectValues ??= new HashSet<object>(ObjectReferenceEqualityComparer.Instance)).Add(objectValue);
            }

            current[leaf] = jsonNode;
        }

        return root;
    }

    /// <summary>
    /// Reports whether the node holds no value — a JSON null, or an object which is empty or
    /// all-null. Such a path gives way to an overlapping one instead of colliding with it,
    /// whichever of the two the wire puts first.
    /// </summary>
    private static bool HoldsNoValue(JsonNode node) =>
        node is null || (node is JsonObject value && IsAllNull(value, out _));

    /// <summary>
    /// Reports whether the object is a path's own value rather than a container built to hold
    /// deeper paths.
    /// </summary>
    private static bool IsObjectValue(HashSet<object> objectValues, JsonObject value) =>
        objectValues is not null && objectValues.Contains(value);

    /// <summary>
    /// Reports whether every value the object holds, at any depth, is a JSON null. An empty object
    /// holds no value, so it counts as all-null. An array counts as a value.
    /// </summary>
    /// <param name="valuePath">
    /// Dotted path of the first value which is not null, relative to <paramref name="value"/>;
    /// <c>null</c> when the object is all-null.
    /// </param>
    private static bool IsAllNull(JsonObject value, out string valuePath)
    {
        foreach (var property in value)
        {
            if (property.Value is null)
            {
                continue;
            }

            if (property.Value is JsonObject nested)
            {
                if (IsAllNull(nested, out var nestedPath))
                {
                    continue;
                }

                valuePath = $"{property.Key}.{nestedPath}";
                return false;
            }

            valuePath = property.Key;
            return false;
        }

        valuePath = null;
        return true;
    }

    /// <summary>
    /// Builds the error for a row where one path holds a value and is also the parent of another
    /// path which holds a value. ClickHouse accepts such a column (for example
    /// <c>JSON(a Int64, a.b Int64)</c>) and renders the row with a duplicate key, which a
    /// <see cref="JsonObject"/> cannot hold.
    /// </summary>
    private static SerializationException OverlappingPathsException(string path, string nestedPath) =>
        new SerializationException(
            $"JSON paths '{path}' and '{nestedPath}' overlap and both hold a value in this row. " +
            $"'{path}' is a value and is also the parent of '{nestedPath}', so the server sends the row with a " +
            $"duplicate '{path}' key, which a JsonObject cannot hold. " +
            $"Read this column with JsonReadMode.String to get the server's JSON text unchanged.");

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

    // Write picks WriteHintedValue vs WriteUnhintedValue per path from HintedTypes, which ToString() does
    // not render — so the hints, ordered for stability, have to be part of the signature. Without them two
    // differently hinted JSON columns share one cache entry and values go out against the wrong hints.
    // A path is an arbitrary identifier — the server escapes it, the client discloses it back — so it can
    // hold whatever character a separator uses, and is length-prefixed to keep the signature injective.
    internal override string CacheSignature
    {
        get
        {
            if (HintedTypes.Count == 0)
                return Name;

            var builder = new StringBuilder(Name).Append('(');
            foreach (var hint in HintedTypes.OrderBy(hint => hint.Key, StringComparer.Ordinal))
                builder.AppendLengthPrefixed(hint.Key).AppendLengthPrefixed(hint.Value.CacheSignature);
            return builder.Append(')').ToString();
        }
    }

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
        tempStream.CopyTo(writer.RawStream);
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
