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
        ReadOnlySpan<byte> span;

        if (value is string s)
        {
            span = Encoding.UTF8.GetBytes(s).AsSpan();
        }
        else if (value is byte[] bytes)
        {
            span = bytes;
        }
        else if (value is ReadOnlyMemory<byte> memory)
        {
            span = memory.Span;
        }
        else
        {
            throw new ArgumentException(
                $"FixedString({Length}) expects string, byte[], or ReadOnlyMemory<byte>, but got {value?.GetType().Name ?? "null"}");
        }

        if (span.Length > Length)
        {
            throw new ArgumentException(
                $"FixedString({Length}) cannot accept data longer than {Length} bytes, but got {span.Length} bytes");
        }

        writer.Write(span);

        if (span.Length < Length)
        {
            writer.Write(new byte[Length - span.Length]);
        }
    }
}
