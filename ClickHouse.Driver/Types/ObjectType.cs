using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class ObjectType : ParameterizedType
{
    public ClickHouseType UnderlyingType { get; set; }

    public override Type FrameworkType => UnderlyingType.FrameworkType;

    public override string Name => "Object";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new SimpleAggregateFunctionType
        {
            UnderlyingType = parseClickHouseTypeFunc(node.ChildNodes[0]),
        };
    }

    public override object Read(ExtendedBinaryReader reader) => UnderlyingType.Read(reader);

    public override string ToString() => $"{Name}({UnderlyingType})";

    public override void Write<T>(ExtendedBinaryWriter writer, T value) => UnderlyingType.Write(writer, value);
}
