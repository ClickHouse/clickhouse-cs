using System;
using System.IO;
using ClickHouse.Driver.Formats;
using Microsoft.IO;

namespace ClickHouse.Driver.Types;

internal class StringType : ClickHouseType
{
    private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new();

    public bool ReadAsByteArray { get; init; }

    public override Type FrameworkType => ReadAsByteArray ? typeof(byte[]) : typeof(string);

    public override object Read(ExtendedBinaryReader reader)
    {
        if (ReadAsByteArray)
        {
            var length = reader.Read7BitEncodedInt();
            return reader.ReadBytes(length);
        }
        return reader.ReadString();
    }

    public override string ToString() => "String";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is string s)
        {
            writer.Write(s);
        }
        else if (value is byte[] b)
        {
            writer.Write7BitEncodedInt(b.Length);
            writer.Write(b);
        }
#if NET6_0_OR_GREATER
        else if (value is ReadOnlyMemory<byte> memory)
        {
            writer.Write7BitEncodedInt(memory.Length);
            writer.Write(memory.Span);
        }
#endif
        else if (value is Stream stream)
        {
            if (stream.CanSeek)
            {
                var length = checked((int)(stream.Length - stream.Position));
                writer.Write7BitEncodedInt(length);
                stream.CopyTo(writer.BaseStream);
            }
            else
            {
                // Non-seekable streams must be buffered to determine length
                using var memoryStream = MemoryStreamManager.GetStream();
                stream.CopyTo(memoryStream);
                var length = (int)memoryStream.Length;
                writer.Write7BitEncodedInt(length);
                memoryStream.Position = 0;
                memoryStream.CopyTo(writer.BaseStream);
            }
        }
        else
        {
            throw new ArgumentException($"String requires string, byte[], ReadOnlyMemory<byte>, or Stream, got {value?.GetType().Name ?? "null"}");
        }
    }
}
