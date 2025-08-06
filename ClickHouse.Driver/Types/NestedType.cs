using System;
using System.Collections;
using System.Linq;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Types;

internal class NestedType : TupleType
{
    public override string Name => "Nested";

    public override Type FrameworkType => base.FrameworkType.MakeArrayType();

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new NestedType
        {
            UnderlyingTypes = node.ChildNodes.Select(ClearFieldName).Select(parseClickHouseTypeFunc).ToArray(),
        };
    }

    // Try to determine if something which is inside a Nested column is a name-type pair
    // (param_id UInt8) or a more complex structure (another parameterized type)
    // We do not currently support multi-word types
    private static SyntaxTreeNode ClearFieldName(SyntaxTreeNode node)
    {
        if (node.ChildNodes.Count > 0)
            return node;

        var name = node.Value;

        var lastSpaceIndex = name.LastIndexOf(' ');
        return lastSpaceIndex > 0 ? new SyntaxTreeNode { Value = name.Substring(lastSpaceIndex + 1) } : node;
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();

#if NET462
        return TupleHelper.ReadNestedArrayWithRuntimeType(reader, length, this);
#else
        var data = Array.CreateInstance(base.FrameworkType, length);
        for (var i = 0; i < length; i++)
        {
            data.SetValue(ClearDBNull(base.Read(reader)), i);
        }
        return data;
#endif
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is null || value is DBNull)
        {
            writer.Write7BitEncodedInt(0);
            return;
        }

        var collection = (IList)value;
        writer.Write7BitEncodedInt(collection.Count);
        for (var i = 0; i < collection.Count; i++)
        {
            base.Write(writer, collection[i]);
        }
    }
}
