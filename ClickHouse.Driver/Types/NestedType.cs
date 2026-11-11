using System;
using System.Collections;
using System.Linq;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class NestedType : TupleType
{
    public override string Name => "Nested";

    public override Type FrameworkType => ClrTypes.MakeArrayType(base.FrameworkType);

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

    /// <summary>
    /// A Nested value is an array of its tuples: an array of the exact framework type, each of whose elements fits
    /// the tuple when the tuple's runtime type does not identify it (see <see cref="TupleType.CanWrite"/>).
    /// </summary>
    public override bool CanWrite(object value)
    {
        if (value is not Array { Rank: 1 } array || array.GetType() != FrameworkType)
            return false;

        if (ElementsIdentifyTuple)
            return true;

        foreach (var element in array)
        {
            if (!TupleFits(element))
                return false;
        }

        return true;
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();
        var data = ClrTypes.CreateArray(base.FrameworkType, length);
        for (var i = 0; i < length; i++)
        {
            data.SetValue(ClearDBNull(base.Read(reader)), i);
        }
        return data;
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
