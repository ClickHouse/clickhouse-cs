#if NET8_0_OR_GREATER
using System;
using System.Buffers.Binary;
using ClickHouse.Driver.Formats;
#endif

namespace ClickHouse.Driver.Types;

internal class UInt128Type : AbstractBigIntegerType
#if NET8_0_OR_GREATER
    , ITypedReader<System.UInt128>
#endif
{
    public override int Size => 16;

    public override string ToString() => "UInt128";

    public override bool Signed => false;

#if NET8_0_OR_GREATER
    // The wire form is 16-byte little-endian unsigned, so this gives the same value as the BigInteger path
    // (which pads with a trailing 0 to stay positive) without its internal heap array. Explicit impl so it
    // does not hide the base ITypedReader<BigInteger>.ReadValue.
    // Reads into a stack buffer: the ReadBytes(int) overload returns a fresh byte[Size] per value, which
    // would put a heap allocation back on the path this typed reader exists to keep allocation-free.
    System.UInt128 ITypedReader<System.UInt128>.ReadValue(ExtendedBinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[Size];
        reader.ReadBytes(buffer);
        return BinaryPrimitives.ReadUInt128LittleEndian(buffer);
    }
#endif
}
