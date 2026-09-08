using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class StringColumnCodecTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task WriteColumn_SingleValue_IsVarUIntLengthThenBytes()
    {
        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<string>("c", "String", new[] { "hello" })));
        CollectionAssert.AreEqual(new byte[] { 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F }, bytes);
    }

    [Test]
    public async Task RoundTrip_EmptyUnicodeAndEmbeddedNul_Preserved()
    {
        var values = new[] { string.Empty, "hello", "héllo✓", "a\0b", new string('x', 500) };

        byte[] bytes = await WriteAsync(w => StringColumnCodec.Instance.WriteColumn(w, new ArrayColumn<string>("c", "String", values)));
        using var reader = ReaderOver(bytes);
        using var column = (IColumn<string>)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", values.Length, None);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumn()
    {
        using var reader = ReaderOver(Array.Empty<byte>());
        using var column = (IColumn<string>)await StringColumnCodec.Instance.ReadColumnAsync(reader, "c", "String", 0, None);
        Assert.That(column.RowCount, Is.EqualTo(0));
    }

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
