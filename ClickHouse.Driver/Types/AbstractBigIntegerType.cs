using System;
using System.Globalization;
using System.Numerics;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal abstract class AbstractBigIntegerType : IntegerType, ITypedWriter<BigInteger>, ITypedReader<BigInteger>
{
    public virtual int Size { get; }

    public override Type FrameworkType => typeof(BigInteger);

    public override object Read(ExtendedBinaryReader reader) => ReadValue(reader);

    public BigInteger ReadValue(ExtendedBinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[Size];
        reader.ReadBytes(buffer);
        return new BigInteger(buffer, isUnsigned: !Signed);
    }

    public abstract override string ToString();

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var bigInt = value switch
        {
            BigInteger bi => bi,
            decimal dl => new BigInteger(dl),
            double d => new BigInteger(d),
            float f => new BigInteger(f),
            int i => new BigInteger(i),
            uint ui => new BigInteger(ui),
            long l => new BigInteger(l),
            ulong ul => new BigInteger(ul),
            _ => new BigInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture))
        };

        WriteValue(writer, bigInt);
    }

    public void WriteValue(ExtendedBinaryWriter writer, BigInteger value)
    {
        if (value < 0 && !Signed)
            throw new ArgumentException("Cannot convert negative BigInteger to UInt");

        // Size is 16 or 32 (Int128/UInt128/Int256/UInt256), so a stack buffer avoids both the
        // BigInteger.ToByteArray() and the new byte[Size] allocation. isUnsigned mirrors the read
        // side above: on an unsigned type the extra sign byte a positive value carries is dropped,
        // which is what the old "trim a trailing zero" step did.
        Span<byte> buffer = stackalloc byte[Size];
        if (!value.TryWriteBytes(buffer, out int bytesWritten, isUnsigned: !Signed))
            throw new OverflowException($"Got {value.GetByteCount(isUnsigned: !Signed)} bytes, {Size} expected");

        // Fill the bytes past the value: 0xFF to sign-extend a negative value, 0x00 for a positive
        // one. The positive case is explicit rather than relying on the stackalloc being
        // zero-initialized (guaranteed only while the compiler emits localsinit).
        buffer.Slice(bytesWritten).Fill(value.Sign < 0 ? (byte)0xFF : (byte)0x00);
        writer.Write(buffer);
    }
}
