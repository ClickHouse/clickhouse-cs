using System;
using System.IO;
using System.Text;

namespace ClickHouse.Driver.Native;

/// <summary>
/// Primitive read/write helpers for the ClickHouse Native protocol wire format:
/// LEB128 variable-length unsigned integers ("VarUInt") and length-prefixed UTF-8 strings.
/// Fixed-width integers/floats are little-endian and are handled directly via
/// <see cref="BinaryReader"/>/<see cref="BinaryWriter"/>, which are little-endian on all
/// supported platforms.
/// </summary>
internal static class NativeWire
{
    /// <summary>Reads a VarUInt (LEB128, up to 64 bits).</summary>
    public static ulong ReadVarUInt(BinaryReader reader)
    {
        ulong result = 0;
        for (var shift = 0; shift < 64; shift += 7)
        {
            var b = reader.ReadByte();
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
        }

        throw new FormatException("VarUInt is too long (more than 10 bytes)");
    }

    /// <summary>Writes a VarUInt (LEB128).</summary>
    public static void WriteVarUInt(BinaryWriter writer, ulong value)
    {
        // At most 10 bytes for a 64-bit value.
        for (var i = 0; i < 10; i++)
        {
            var b = (byte)(value & 0x7F);
            value >>= 7;
            if (value != 0)
                b |= 0x80;
            writer.Write(b);
            if (value == 0)
                return;
        }
    }

    /// <summary>Reads a ClickHouse String: VarUInt byte length followed by raw UTF-8 bytes.</summary>
    public static string ReadString(BinaryReader reader)
    {
        var length = checked((int)ReadVarUInt(reader));
        if (length == 0)
            return string.Empty;
        var bytes = ReadFully(reader, length);
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>Writes a ClickHouse String: VarUInt byte length followed by raw UTF-8 bytes.</summary>
    public static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = value is null ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(value);
        WriteVarUInt(writer, (ulong)bytes.Length);
        if (bytes.Length > 0)
            writer.Write(bytes);
    }

    /// <summary>Reads exactly <paramref name="count"/> bytes or throws <see cref="EndOfStreamException"/>.</summary>
    public static byte[] ReadFully(BinaryReader reader, int count)
    {
        var buffer = new byte[count];
        var offset = 0;
        while (offset < count)
        {
            var read = reader.Read(buffer, offset, count - offset);
            if (read == 0)
                throw new EndOfStreamException($"Expected to read {count} bytes, got {offset}");
            offset += read;
        }

        return buffer;
    }
}
