#if NET8_0_OR_GREATER
using System;
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
    // Reads into a stack buffer: the ReadBytes(int) overload returns a fresh byte[Size] per value, which
    // would put a heap allocation back on the path this typed reader exists to keep allocation-free.
    System.Int128 ITypedReader<System.Int128>.ReadValue(ExtendedBinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[Size];
        reader.ReadBytes(buffer);
        return BinaryPrimitives.ReadInt128LittleEndian(buffer);
    }
#endif
}
