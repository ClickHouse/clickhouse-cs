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
            WriteString(writer, s);
        }
        else if (value is byte[] b)
        {
            WriteByteArray(writer, b);
        }
        else if (value is Stream stream)
        {
            WriteStream(writer, stream);
        }
#if NET6_0_OR_GREATER
        else if (value is ReadOnlyMemory<byte> memory)
        {
            WriteReadOnlyMemory(writer, memory);
        }
#endif
        else
        {
            throw new ArgumentException($"FixedString requires string, byte[], ReadOnlyMemory<byte>, or Stream, got {value?.GetType().Name ?? "null"}");
        }
    }

    private void WriteString(ExtendedBinaryWriter writer, string s)
    {
        var stringBytes = new byte[Length];
        Encoding.UTF8.GetBytes(s, 0, s.Length, stringBytes, 0);
        writer.Write(stringBytes);
    }

    private void WriteByteArray(ExtendedBinaryWriter writer, byte[] b)
    {
        if (b.Length != Length)
        {
            throw new ArgumentException($"Byte array length {b.Length} does not match FixedString({Length}). Byte arrays must be exactly {Length} bytes.");
        }
        writer.Write(b);
    }

#if NET6_0_OR_GREATER
    private void WriteReadOnlyMemory(ExtendedBinaryWriter writer, ReadOnlyMemory<byte> memory)
    {
        if (memory.Length != Length)
        {
            throw new ArgumentException($"ReadOnlyMemory<byte> length {memory.Length} does not match FixedString({Length}). ReadOnlyMemory<byte> must be exactly {Length} bytes.");
        }
        writer.Write(memory.Span);
    }
#endif

    private void WriteStream(ExtendedBinaryWriter writer, Stream stream)
    {
        if (stream.CanSeek)
        {
            var streamLength = checked((int)(stream.Length - stream.Position));
            if (streamLength != Length)
            {
                throw new ArgumentException($"Stream length {streamLength} does not match FixedString({Length}). Stream must be exactly {Length} bytes.");
            }
            stream.CopyTo(writer.BaseStream);
        }
        else
        {
            // Non-seekable streams must be buffered to validate length
            var buffer = new byte[Length + 1];
            int totalRead = 0;
            int bytesRead;
            while (totalRead < buffer.Length && (bytesRead = stream.Read(buffer, totalRead, buffer.Length - totalRead)) > 0)
            {
                totalRead += bytesRead;
            }

            if (totalRead != Length)
            {
                throw new ArgumentException($"Stream length {totalRead}{(totalRead > Length ? "+" : string.Empty)} does not match FixedString({Length}). Stream must be exactly {Length} bytes.");
            }
            writer.Write(buffer, 0, Length);
        }
    }
}
