using System;
using System.Globalization;
using System.IO;
using System.Text;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class FixedStringType : ParameterizedType
{
    public int Length { get; set; }

    public bool ReadAsByteArray { get; set; }

    public override Type FrameworkType => ReadAsByteArray ? typeof(byte[]) : typeof(string);

    public override string Name => "FixedString";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new FixedStringType
        {
            Length = int.Parse(node.SingleChild.Value, CultureInfo.InvariantCulture),
            ReadAsByteArray = settings.readStringsAsByteArrays,
        };
    }

    public override string ToString() => $"FixedString({Length})";

    public override object Read(ExtendedBinaryReader reader)
    {
        var bytes = reader.ReadBytes(Length);
        if (ReadAsByteArray)
        {
            return bytes;
        }
        return Encoding.UTF8.GetString(bytes);
    }

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
#if NET6_0_OR_GREATER
        else if (value is ReadOnlyMemory<byte> memory)
        {
            if (memory.Length != Length)
            {
                throw new ArgumentException($"ReadOnlyMemory<byte> length {memory.Length} does not match FixedString({Length}). ReadOnlyMemory<byte> must be exactly {Length} bytes.");
            }

            writer.Write(memory.Span);
        }
#endif
        else if (value is Stream stream)
        {
            if (stream.CanSeek)
            {
                var streamLength = checked((int)(stream.Length - stream.Position));
                if (streamLength != Length)
                {
                    throw new ArgumentException($"Stream length {streamLength} does not match FixedString({Length}). Stream must be exactly {Length} bytes.");
                }
            }
            // For non-seekable streams, skip validation - server will reject if wrong length
            stream.CopyTo(writer.BaseStream);
        }
        else
        {
            throw new ArgumentException($"FixedString requires string, byte[], ReadOnlyMemory<byte>, or Stream, got {value?.GetType().Name ?? "null"}");
        }
    }
}
