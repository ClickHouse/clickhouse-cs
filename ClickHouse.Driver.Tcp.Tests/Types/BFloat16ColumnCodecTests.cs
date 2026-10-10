using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class BFloat16ColumnCodecTests
{
    // BFloat16 values, and the narrowing of a float that BFloat16 cannot hold, go to a live server in
    // InsertRoundTripCase, with the experimental BFloat16 flag enabled.

    // BFloat16 and Float32 both read as float. The storage of BFloat16 is the ArrayColumn<float> that its read gives.
    // A decoded Float32 column holds 32-bit values, so the converter layer narrows it.
    [Test]
    public void CanWrite_DecodedBFloat16Column_IsTrueAndDecodedFloat32ColumnIsFalse()
    {
        using IColumn bfloat16 = DecodedColumns.Of("c", "BFloat16", 1f, -2f);
        using IColumn float32 = DecodedColumns.Of("c", "Float32", 1f, -2f);

        Assert.Multiple(() =>
        {
            Assert.That(BFloat16ColumnCodec.Instance.CanWrite(bfloat16), Is.True);
            Assert.That(BFloat16ColumnCodec.Instance.CanWrite(float32), Is.False);
        });
    }
}
