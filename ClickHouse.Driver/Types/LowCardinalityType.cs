using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class LowCardinalityType : ParameterizedType
{
    public ClickHouseType UnderlyingType { get; set; }

    public override string Name => "LowCardinality";

    public override Type FrameworkType => UnderlyingType.FrameworkType;

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new LowCardinalityType
        {
            UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
        };
    }

    public override string ToString() => $"{Name}({UnderlyingType})";

    public override object Read(ExtendedBinaryReader reader) => UnderlyingType.Read(reader);

    public override void Write<T>(ExtendedBinaryWriter writer, T value)
    {
        UnderlyingType.Write(writer, value);
    }
}
