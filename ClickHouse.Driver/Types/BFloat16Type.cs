using System;
using System.Globalization;
using System.Runtime.InteropServices;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Represents the ClickHouse BFloat16 (Brain Floating Point) type.
/// BFloat16 is a 16-bit floating point format with 1 sign bit, 8 exponent bits, and 7 mantissa bits.
/// It preserves the range of Float32 while reducing precision, making it useful for machine learning workloads.
/// </summary>
/// <remarks>
/// This type converts between ClickHouse's 16-bit BFloat16 wire format and .NET's System.Single (float).
/// Conversion is performed by truncating/extending the bit representation. The top 16 bits of a Float32
/// are equivalent to a BFloat16.
/// </remarks>
internal class BFloat16Type : FloatType
{
    public override Type FrameworkType => typeof(float);

#if NET462 || NET48 || NETSTANDARD2_1
    [StructLayout(LayoutKind.Explicit)]
    private struct FloatUInt32Union
    {
        [FieldOffset(0)] public float FloatValue;
        [FieldOffset(0)] public uint UIntValue;
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        // BFloat16 is 16 bits: 1 sign + 8 exponent + 7 mantissa
        // Read as ushort and expand to float32 by left-shifting 16 bits
        ushort bfloat16Bits = reader.ReadUInt16();

        var converter = new FloatUInt32Union { UIntValue = (uint)bfloat16Bits << 16 };
        return converter.FloatValue;
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        // Convert float to BFloat16 by truncating to top 16 bits
        float floatValue = Convert.ToSingle(value, CultureInfo.InvariantCulture);

        var converter = new FloatUInt32Union { FloatValue = floatValue };
        ushort bfloat16Bits = (ushort)(converter.UIntValue >> 16);

        writer.Write(bfloat16Bits);
    }

#else
    public override object Read(ExtendedBinaryReader reader)
    {
        // BFloat16 is 16 bits: 1 sign + 8 exponent + 7 mantissa
        // Read as ushort and expand to float32 by left-shifting 16 bits
        ushort bfloat16Bits = reader.ReadUInt16();
        uint float32Bits = (uint)bfloat16Bits << 16;
        return BitConverter.UInt32BitsToSingle(float32Bits);
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        // Convert float to BFloat16 by truncating to top 16 bits
        float floatValue = Convert.ToSingle(value, CultureInfo.InvariantCulture);
        uint float32Bits = BitConverter.SingleToUInt32Bits(floatValue);
        ushort bfloat16Bits = (ushort)(float32Bits >> 16);
        writer.Write(bfloat16Bits);
    }

#endif
    public override string ToString() => "BFloat16";
}
