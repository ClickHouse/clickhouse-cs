using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class BFloat16ColumnCodecTests
{
    // Round-trips of exactly-representable values run against a live server (InsertRoundTripCase, with the
    // experimental BFloat16 flag enabled). These unit tests cover only what a server round-trip cannot: the
    // lossy narrowing of a non-representable value, and the accepted CLR element type.
    [Test]
    public async Task WriteColumn_NarrowsToTop16BitsOfFloat32()
    {
        // 1.1f is 0x3F8CCCCD in float32; narrowing to bfloat16 keeps the top 16 bits (0x3F8C), written
        // little-endian as 0x8C 0x3F.
        byte[] bytes = await WriteAsync(w => BFloat16ColumnCodec.Instance.WriteColumn(w, new ArrayColumn<float>("c", "BFloat16", new[] { 1.1f })));

        CollectionAssert.AreEqual(new byte[] { 0x8C, 0x3F }, bytes);
    }

    [Test]
    public void CanWrite_AcceptsFloatOnly()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BFloat16ColumnCodec.Instance.CanWrite(new ArrayColumn<float>("c", "BFloat16", System.Array.Empty<float>())), Is.True);
            Assert.That(BFloat16ColumnCodec.Instance.CanWrite(new ArrayColumn<double>("c", "BFloat16", System.Array.Empty<double>())), Is.False);
        });
    }
}
