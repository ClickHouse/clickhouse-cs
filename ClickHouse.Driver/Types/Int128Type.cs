#if NET8_0_OR_GREATER
using System.Buffers.Binary;
using ClickHouse.Driver.Formats;
#endif

namespace ClickHouse.Driver.Types;

internal class Int128Type : AbstractBigIntegerType
#if NET8_0_OR_GREATER
    , ITypedReader<System.Int128>
#endif
{
    public override int Size => 16;

    public override string ToString() => "Int128";

#if NET8_0_OR_GREATER
    // Native box-free read: the wire form is 16-byte little-endian two's-complement, identical to the layout
    // BigInteger uses, so this yields the same value as the BigInteger path without its internal heap array.
    // Explicit impl so it does not hide the base ITypedReader<BigInteger>.ReadValue (same name/params).
    System.Int128 ITypedReader<System.Int128>.ReadValue(ExtendedBinaryReader reader)
        => BinaryPrimitives.ReadInt128LittleEndian(reader.ReadBytes(Size));
#endif
}
