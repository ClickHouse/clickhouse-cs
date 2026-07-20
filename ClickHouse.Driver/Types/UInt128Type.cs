#if NET8_0_OR_GREATER
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
    // Native box-free read: the wire form is 16-byte little-endian unsigned, so this yields the same value as
    // the BigInteger path (which pads with a trailing 0 to stay positive) without its internal heap array.
    // Explicit impl so it does not hide the base ITypedReader<BigInteger>.ReadValue (same name/params).
    System.UInt128 ITypedReader<System.UInt128>.ReadValue(ExtendedBinaryReader reader)
        => BinaryPrimitives.ReadUInt128LittleEndian(reader.ReadBytes(Size));
#endif
}
