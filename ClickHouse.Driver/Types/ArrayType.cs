using System;
using System.Buffers;
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
        if (length == 0)
        {
            return Array.CreateInstance(UnderlyingType.FrameworkType, 0);
        }

        var buffer = ArrayPool<object>.Shared.Rent(length);
        try
        {
            for (var i = 0; i < length; i++)
            {
                buffer[i] = ClearDBNull(UnderlyingType.Read(reader));
            }

            var data = Array.CreateInstance(UnderlyingType.FrameworkType, length);
            Array.Copy(buffer, data, length);
            return data;
        }
        finally
        {
            ArrayPool<object>.Shared.Return(buffer, clearArray: true);
        }
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
            UnderlyingType.Write(writer, collection[i]);
        }
    }
}
