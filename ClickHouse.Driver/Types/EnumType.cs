using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class EnumType : ParameterizedType
{
    private readonly Dictionary<string, int> values;
    private readonly Dictionary<int, string> reverseValues;

    public EnumType()
    {
        values = new();
        reverseValues = new();
    }

    protected EnumType(Dictionary<string, int> values)
    {
        this.values = values;
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
            .Select(p => p.Split('='))
            .ToDictionary(kvp => kvp[0].Trim().Trim('\''), kvp => Convert.ToInt32(kvp[1].Trim(), CultureInfo.InvariantCulture));

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

    public int Lookup(string key) => values[key];

    public string Lookup(int value) => reverseValues.TryGetValue(value, out var key) ? key : throw new KeyNotFoundException($"Enum value {value} not found");

    public override string ToString() => $"{Name}({string.Join(",", values.Select(kvp => kvp.Key + "=" + kvp.Value))}";

    public override object Read(ExtendedBinaryReader reader) => throw new NotImplementedException();

    public override void Write(ExtendedBinaryWriter writer, object value) => throw new NotImplementedException();
}
