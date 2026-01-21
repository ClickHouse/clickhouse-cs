using System;
using System.Buffers;
using System.Globalization;
using System.Text;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class FixedStringType : ParameterizedType
{
    private int length;

    public int Length
    {
        get => length;
        set
        {
            length = value;
            buffer = new  byte[Length];
        }
    }

    private byte[] buffer;

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
            Array.Clear(buffer, 0, Length);
            Encoding.UTF8.GetBytes(s, 0, s.Length, buffer, 0);
            writer.Write(buffer, 0, Length);
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
