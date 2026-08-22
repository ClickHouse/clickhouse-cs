using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class UuidColumnCodecTests
{
    [Test]
    public async Task WriteColumn_ProducesClickHouseByteOrder()
    {
        // ClickHouse stores a UUID as two little-endian 64-bit halves. For 00112233-4455-6677-8899-aabbccddeeff
        // the first half is 0x0011223344556677 (LE) and the second 0x8899aabbccddeeff (LE).
        var guid = new Guid("00112233-4455-6677-8899-aabbccddeeff");

        byte[] bytes = await WriteAsync(w => UuidColumnCodec.Instance.WriteColumn(w, new ArrayColumn<Guid>("c", "UUID", new[] { guid })));

        var expected = new byte[]
        {
            0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, // first half, little-endian
            0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, // second half, little-endian
        };
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task RoundTrip_ManyRandomGuids_PreservesEveryBytePosition()
    {
        // A deterministic fuzz over the full 16-byte permutation: any single-position mask error would corrupt
        // at least one of these round-trips. Also exercises reads of arbitrary wire patterns (write is a
        // bijection on 16 bytes, so these cover every wire byte layout).
        var random = new Random(20260727);
        var values = new Guid[512];
        Span<byte> bytes = stackalloc byte[16];
        for (int i = 0; i < values.Length; i++)
        {
            random.NextBytes(bytes);
            values[i] = new Guid(bytes);
        }

        using var column = (IColumn<Guid>)await RoundTripAsync(UuidColumnCodec.Instance, new ArrayColumn<Guid>("c", "UUID", values), "UUID", values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public void CanWrite_AcceptsGuidOnly()
    {
        Assert.Multiple(() =>
        {
            Assert.That(UuidColumnCodec.Instance.CanWrite(new ArrayColumn<Guid>("c", "UUID", Array.Empty<Guid>())), Is.True);
            Assert.That(UuidColumnCodec.Instance.CanWrite(new ArrayColumn<string>("c", "UUID", Array.Empty<string>())), Is.False);
        });
    }
}
