using System;
using System.Collections;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class ArrayType : ParameterizedType
{
    public ClickHouseType UnderlyingType { get; set; }

    public override Type FrameworkType => UnderlyingType.FrameworkType.MakeArrayType();

    public override string Name => "Array";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new ArrayType
        {
            UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
        };
    }

    public override string ToString() => $"{Name}({UnderlyingType})";

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();
        var data = Array.CreateInstance(UnderlyingType.FrameworkType, length);
        for (var i = 0; i < length; i++)
        {
            data.SetValue(ClearDBNull(UnderlyingType.Read(reader)), i);
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

        // Rank>1 CLR arrays (e.g. byte[,]) iterate flattened via IEnumerable/IList, so slice along
        // the outermost rank into sub-arrays before delegating to the inner type's writer.
        if (value is Array multidim && multidim.Rank > 1)
        {
            var outerLength = multidim.GetLength(0);
            writer.Write7BitEncodedInt(outerLength);
            foreach (var slice in MultiDimArrayHelper.EnumerateOutermostRank(multidim))
            {
                UnderlyingType.Write(writer, slice);
            }
            return;
        }

        var collection = (IList)value;
        writer.Write7BitEncodedInt(collection.Count);
        for (var i = 0; i < collection.Count; i++)
        {
            UnderlyingType.Write(writer, collection[i]);
        }
    }
}
