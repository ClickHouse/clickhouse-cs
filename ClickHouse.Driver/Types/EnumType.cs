using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class EnumType : ParameterizedType
{
    internal readonly Dictionary<string, int> Values;
    private readonly Dictionary<int, string> reverseValues;

    public EnumType()
    {
        Values = new();
        reverseValues = new();
    }

    protected EnumType(Dictionary<string, int> values)
    {
        this.Values = values;
        reverseValues = new Dictionary<int, string>(values.Count);
        foreach (var kvp in values)
        {
            reverseValues[kvp.Value] = kvp.Key;
        }
    }

    public override string Name => "Enum";

    public override Type FrameworkType => typeof(string);

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var parameters = node.ChildNodes
            .Select(cn => cn.Value)
            .Select(ParseEnumMember)
            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        string typeName = TypeConverter.ExtractTypeName(node);

        switch (typeName)
        {
            case "Enum":
            case "Enum8":
                return new Enum8Type(parameters);
            case "Enum16":
                return new Enum16Type(parameters);
            default: throw new ArgumentOutOfRangeException($"Unsupported Enum type: {node.Value}");
        }
    }

    public int Lookup(string key) => Values[key];

    public string Lookup(int value) => reverseValues.TryGetValue(value, out var key) ? key : throw new KeyNotFoundException($"Enum value {value} not found");

    public override string ToString() => $"{Name}({string.Join(",", Values.Select(kvp => kvp.Key + "=" + kvp.Value))}";

    public override object Read(ExtendedBinaryReader reader) => throw new NotImplementedException();

    public override void Write(ExtendedBinaryWriter writer, object value) => throw new NotImplementedException();

    private static KeyValuePair<string, int> ParseEnumMember(string value)
    {
        var separatorIndex = value.LastIndexOf('=');
        if (separatorIndex < 0)
            throw new FormatException($"Invalid enum member definition: {value}");

        var rawLabel = value[..separatorIndex].Trim();
        var rawValue = value[(separatorIndex + 1)..].Trim();

        if (rawLabel.Length >= 2 && rawLabel[0] == '\'' && rawLabel[^1] == '\'')
        {
            rawLabel = rawLabel[1..^1];
        }

        return new KeyValuePair<string, int>(
            Regex.Unescape(rawLabel),
            Convert.ToInt32(rawValue, CultureInfo.InvariantCulture));
    }
}
