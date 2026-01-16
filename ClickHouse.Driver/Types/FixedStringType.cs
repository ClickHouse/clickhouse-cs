using System;
using System.Globalization;
using System.Text;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class FixedStringType : ParameterizedType
{
    public int Length { get; set; }

    public override Type FrameworkType => typeof(byte[]);

    public override string Name => "FixedString";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new FixedStringType
        {
            Length = int.Parse(node.SingleChild.Value, CultureInfo.InvariantCulture),
        };
    }

    public override string ToString() => $"FixedString({Length})";

    public override object Read(ExtendedBinaryReader reader) => reader.ReadBytes(Length);

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is string s)
        {
            var stringBytes = new byte[Length];
            Encoding.UTF8.GetBytes(s, 0, s.Length, stringBytes, 0);
            writer.Write(stringBytes);
        }
        else if (value is byte[] b)
        {
            if (b.Length != Length)
            {
                throw new ArgumentException($"Byte array length {b.Length} does not match FixedString({Length}). Byte arrays must be exactly {Length} bytes.");
            }
            writer.Write(b);
        }
        else
        {
            throw new ArgumentException($"FixedString requires string or byte[], got {value?.GetType().Name ?? "null"}");
        }
    }
}
