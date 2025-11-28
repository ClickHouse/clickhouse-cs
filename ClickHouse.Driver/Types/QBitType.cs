using System;
using System.Globalization;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Represents the ClickHouse QBit type: a quantized vector type for efficient storage.
/// On the wire, QBit is simply an Array of the underlying element type (Float32/Float64/BFloat16).
/// The bit-transpose optimization happens server-side for storage, not in the wire protocol.
/// </summary>
internal class QBitType : ParameterizedType
{
    // Delegate to ArrayType for wire format
    private ArrayType UnderlyingArrayType => new ArrayType { UnderlyingType = ElementType };
    
    public ClickHouseType ElementType { get; set; }
    public int Dimension { get; set; }
    
    public override Type FrameworkType => ElementType.FrameworkType.MakeArrayType();

    public override string Name => "QBit";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new QBitType
        {
            ElementType = parseClickHouseTypeFunc(node.ChildNodes[0]),
            Dimension = int.Parse(node.ChildNodes[1].Value, CultureInfo.InvariantCulture),
        };
    }

    public override string ToString() => $"{Name}({ElementType},{Dimension})";

    public override object Read(ExtendedBinaryReader reader)
    {
        // QBit wire format is Array(UnderlyingType), but the length is padded to the nearest 8
        var length = reader.Read7BitEncodedInt();
        var data = Array.CreateInstance(ElementType.FrameworkType, Dimension); // Could use a pool here
        for (var i = 0; i < length; i++)
        {
            var value = ElementType.Read(reader);
            if (i < Dimension)
            {
                data.SetValue(ClearDBNull(value), i);
            }
        }
    
        return data;
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        // QBit wire format is just Array(ElementType)
        UnderlyingArrayType.Write(writer, value);
    }
}
