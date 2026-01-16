using System;
using System.Linq;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class VariantType : ParameterizedType
{
    public ClickHouseType[] UnderlyingTypes { get; internal set; }

    public override Type FrameworkType => typeof(object);

    public override string Name => "Variant";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new VariantType
        {
            UnderlyingTypes = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray(),
        };
    }

    public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";

    public override object Read(ExtendedBinaryReader reader)
    {
        var typeIndex = reader.ReadByte();
        var type = UnderlyingTypes[typeIndex];

        return type.Read(reader);
    }

    public (int, ClickHouseType) GetMatchingType(object value)
    {
        var valueType = value?.GetType() ?? typeof(DBNull);
        for (int i = 0; i < UnderlyingTypes.Length; i++)
        {
            var type = UnderlyingTypes[i];
            if (type.FrameworkType == valueType)
            {
                return (i, type);
            }
        }
        throw new ArgumentException("Could not find matching type for variant", nameof(value));
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var (index, type) = GetMatchingType(value);
        writer.Write((byte)index);
        type.Write(writer, value);
    }
}
