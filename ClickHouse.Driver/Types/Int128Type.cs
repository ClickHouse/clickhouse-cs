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
    // The wire form is 16-byte little-endian two's-complement, the same layout BigInteger uses, so this gives
    // the same value without its internal heap array. Explicit impl so it does not hide the base
    // ITypedReader<BigInteger>.ReadValue.
    System.Int128 ITypedReader<System.Int128>.ReadValue(ExtendedBinaryReader reader)
        => BinaryPrimitives.ReadInt128LittleEndian(reader.ReadBytes(Size));
#endif
}
