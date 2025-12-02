using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Types;

internal class JsonType : ParameterizedType
{
    private readonly string[] jsonSettingNames =
    [
        "max_dynamic_paths",
        "max_dynamic_types",
        "skip "
    ];

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
            .Where(childNode => !jsonSettingNames.Any(jsonSettingName => childNode.Value.StartsWith(jsonSettingName, StringComparison.OrdinalIgnoreCase)))
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
        JsonObject rootObject;
        if (value is string inputString)
        {
            rootObject = (JsonObject)JsonNode.Parse(inputString);
        }
        else if (value is JsonObject inputObject)
        {
            rootObject = inputObject;
        }
        else
        {
            rootObject = (JsonObject)JsonSerializer.SerializeToNode(value);
        }

        // Simple depth-first search to flatten the JSON object into a dictionary
        WriteJsonObject(writer, rootObject);
    }

    internal static void WriteJsonObject(ExtendedBinaryWriter writer, JsonObject rootObject)
    {
        Dictionary<string, JsonNode> fields = new();
        StringBuilder currentPath = new();
        FlattenJson(rootObject, ref currentPath, ref fields);

        writer.Write7BitEncodedInt(fields.Count);
        foreach (var field in fields)
        {
            writer.Write(field.Key);
            WriteJsonNode(writer, field.Value);
        }
    }

    internal static void FlattenJson(JsonObject parent, ref StringBuilder currentPath, ref Dictionary<string, JsonNode> fields)
    {
        foreach (var property in parent)
        {
            var pathLengthBefore = currentPath.Length;
            if (currentPath.Length > 0)
                currentPath.Append('.');
            currentPath.Append(property.Key);

            if (property.Value is JsonObject jObject)
            {
                FlattenJson(jObject, ref currentPath, ref fields);
            }
            else if (property.Value is null || property.Value.GetValueKind() == JsonValueKind.Null)
            {
                fields[currentPath.ToString()] = null;
            }
            else
            {
                fields[currentPath.ToString()] = property.Value;
            }

            currentPath.Length = pathLengthBefore;
        }
    }

    internal static IEnumerable<JsonNode> LeafNodes(JsonNode node)
    {
        if (node is JsonObject jObject)
        {
            foreach (var property in jObject)
            {
                if (property.Value is JsonObject)
                {
                    foreach (var child in LeafNodes(property.Value))
                        yield return child;
                }
                else
                {
                    yield return property.Value;
                }
            }
        }
        else if (node is JsonArray jArray)
        {
            yield return jArray;
        }
        else
        {
            yield break;
        }
    }

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

    private static JsonNode ReadJsonFixedString(ExtendedBinaryReader reader, ClickHouseType type)
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
            BigInteger bi => JsonValue.Create(bi.ToString()),
            Guid guid => JsonValue.Create(guid.ToString()),
            IPAddress ip => JsonValue.Create(ip.ToString()),
            ClickHouseDecimal chDec => JsonValue.Create(chDec.ToString()),
            // Default: try JsonSerializer for complex types
            _ => JsonValue.Create(JsonSerializer.SerializeToElement(value))
        };
    }

    internal static void WriteJsonNode(ExtendedBinaryWriter writer, JsonNode node)
    {
        switch (node)
        {
            case JsonArray array:
                WriteJsonArray(writer, array);
                break;
            case JsonValue value:
                WriteJsonValue(writer, value);
                break;
            case null:
                writer.Write(BinaryTypeIndex.Nothing);
                break;
            default:
                throw new SerializationException($"Unsupported JSON node type: {node.GetType()}");
        }
    }

    internal static void WriteJsonArray(ExtendedBinaryWriter writer, JsonArray array)
    {
        writer.Write(BinaryTypeIndex.Array);

        var kind = array.Count > 0 ? array[0].GetValueKind() : JsonValueKind.Null;

        // For numbers, detect the specific type from the first element
        byte numericTypeIndex = BinaryTypeIndex.Float64;
        if (kind == JsonValueKind.Number)
        {
            var firstVal = (JsonValue)array[0];
            if (firstVal.TryGetValue<int>(out _))
                numericTypeIndex = BinaryTypeIndex.Int32;
            else if (firstVal.TryGetValue<long>(out _))
                numericTypeIndex = BinaryTypeIndex.Int64;
        }

        // Step 1: Write binary tag for array element type
        switch (kind)
        {
            case JsonValueKind.Undefined:
            case JsonValueKind.String:
                writer.Write(BinaryTypeIndex.String);
                break;
            case JsonValueKind.Number:
                writer.Write(numericTypeIndex);
                break;
            case JsonValueKind.False:
            case JsonValueKind.True:
                writer.Write(BinaryTypeIndex.Bool);
                break;
            case JsonValueKind.Null:
                writer.Write(BinaryTypeIndex.Nothing);
                break;
            case JsonValueKind.Object:
                writer.Write(BinaryTypeIndex.Json);
                writer.Write((byte)0); // serialization version
                writer.Write7BitEncodedInt(256); // max_dynamic_paths
                writer.Write((int)16); // max_dynamic_types
                break;
            default:
                throw new SerializationException($"Unsupported JSON value kind: {kind}");
        }

        // Step 2: Write array length
        writer.Write7BitEncodedInt(array.Count);

        // Step 3: Write array elements
        foreach (var value in array)
        {
            var elementKind = value.GetValueKind();
            if (elementKind != kind && !(IsBoolKind(kind) && IsBoolKind(elementKind))) // True and False are different kinds, so we need a special check for them
            {
                throw new SerializationException("Array contains mixed value types");
            }

            switch (kind)
            {
                case JsonValueKind.Undefined:
                case JsonValueKind.String:
                    writer.Write(value.ToString());
                    break;
                case JsonValueKind.Number:
                    WriteJsonArrayNumber(writer, (JsonValue)value, numericTypeIndex);
                    break;
                case JsonValueKind.False:
                case JsonValueKind.True:
                    writer.Write(value.GetValue<bool>());
                    break;
                case JsonValueKind.Null:
                    writer.Write(BinaryTypeIndex.Nothing);
                    break;
                case JsonValueKind.Object:
                    WriteJsonObject(writer, (JsonObject)value);
                    break;
                default:
                    throw new SerializationException($"Unsupported JSON value kind: {value.GetValueKind()}");
            }
        }
    }

    internal static void WriteJsonValue(ExtendedBinaryWriter writer, JsonValue value)
    {
        switch (value.GetValueKind())
        {
            case JsonValueKind.Undefined:
            case JsonValueKind.String:
                writer.Write(BinaryTypeIndex.String);
                writer.Write(value.ToString());
                break;
            case JsonValueKind.Number:
                WriteJsonNumber(writer, value);
                break;
            case JsonValueKind.False:
            case JsonValueKind.True:
                writer.Write(BinaryTypeIndex.Bool);
                writer.Write(value.GetValue<bool>());
                break;
            case JsonValueKind.Null:
                writer.Write(BinaryTypeIndex.Nothing);
                break;
            default:
                throw new SerializationException($"Unsupported JSON value kind: {value.GetValueKind()}");
        }
    }

    private static void WriteJsonNumber(ExtendedBinaryWriter writer, JsonValue value)
    {
        if (value.TryGetValue<long>(out var l))
        {
            writer.Write(BinaryTypeIndex.Int64);
            writer.Write(l);
        }
        else if (value.TryGetValue<int>(out var i))
        {
            writer.Write(BinaryTypeIndex.Int32);
            writer.Write(i);
        }
        else if (value.TryGetValue<double>(out var d))
        {
            writer.Write(BinaryTypeIndex.Float64);
            writer.Write(d);
        }
        else
        {
            // Fallback: parse from string representation
            writer.Write(BinaryTypeIndex.Float64);
            writer.Write(double.Parse(value.ToString(), CultureInfo.InvariantCulture));
        }
    }

    private static void WriteJsonArrayNumber(ExtendedBinaryWriter writer, JsonValue value, byte typeIndex)
    {
        switch (typeIndex)
        {
            case BinaryTypeIndex.Int32:
                if (value.TryGetValue<int>(out var i))
                {
                    writer.Write(i);
                    break;
                }
                throw new ArgumentException($"Expected json array element to be Int32: {value}. Arrays must contain only one type of element.");
            case BinaryTypeIndex.Int64:
                if (value.TryGetValue<long>(out var l))
                {
                    writer.Write(l);
                    break;
                }
                throw new ArgumentException($"Expected json array element to be Int64: {value}. Arrays must contain only one type of element.");
            default: // Float64
                if (value.TryGetValue<double>(out var d))
                {
                    writer.Write(d);
                }
                else
                {
                    writer.Write(double.Parse(value.ToString(), CultureInfo.InvariantCulture));
                }
                break;
        }
    }

    private static bool IsBoolKind(JsonValueKind kind) => kind is JsonValueKind.True or JsonValueKind.False;
}
