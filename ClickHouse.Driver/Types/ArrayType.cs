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

        // Rank>1 CLR arrays (e.g. byte[,]) iterate flattened via IEnumerable/IList. Walk the
        // axes directly via MultiDimArrayHelper so leaf scalars are written without per-row
        // sub-array allocation. Rank-1 arrays (including jagged outer T[][]) keep the IList
        // path because the outer rank is 1 even though the element type is itself an array.
        if (value is Array multidim && multidim.Rank > 1)
        {
            var leaf = MultiDimArrayHelper.ResolveLeafType(this, multidim.Rank);
            MultiDimArrayHelper.WriteMultidimensional(writer, multidim, leaf);
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
