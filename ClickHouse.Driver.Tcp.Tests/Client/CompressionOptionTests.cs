using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Tcp.Tests.Client;

/// <summary>
/// The compression option's surface: how a connection-string value maps to a codec, and the codecs the client
/// refuses. <c>none</c> means the query carries no compression at all, which is not the same as a frame whose
/// method byte is NONE — that byte only ever appears on the read side, chosen by the server.
/// </summary>
[TestFixture]
public class CompressionOptionTests
{
    [TestCase("lz4", CompressionFrame.MethodLz4)]
    [TestCase("LZ4", CompressionFrame.MethodLz4)]
    [TestCase("  lz4  ", CompressionFrame.MethodLz4)]
    [TestCase("zstd", CompressionFrame.MethodZstd)]
    [TestCase("ZSTD", CompressionFrame.MethodZstd)]
    public void FromConnectionString_ACodecName_ResolvesToThatCodec(string value, byte expectedMethod)
    {
        var options = ClickHouseTcpClientOptions.FromConnectionString($"Host=localhost;Compression={value}");

        Assert.That(options.Compressor, Is.Not.Null);
        Assert.That(options.Compressor.MethodByte, Is.EqualTo(expectedMethod));
    }

    [TestCase("none")]
    [TestCase("NONE")]
    public void FromConnectionString_None_LeavesNoCompressor(string value)
    {
        var options = ClickHouseTcpClientOptions.FromConnectionString($"Host=localhost;Compression={value}");

        Assert.That(options.Compressor, Is.Null);
    }

    [Test]
    public void FromConnectionString_AnUnknownCodecName_ThrowsNamingTheSupportedOnes()
    {
        var failure = Assert.Throws<ArgumentException>(
            () => ClickHouseTcpClientOptions.FromConnectionString("Host=localhost;Compression=snappy"));

        Assert.That(failure.Message, Does.Contain("snappy").And.Contains("lz4").And.Contains("zstd"));
    }

    [Test]
    public void Compression_RoundTripsThroughTheBuilder()
    {
        var builder = new ClickHouseTcpConnectionStringBuilder { Compression = "zstd" };

        Assert.Multiple(() =>
        {
            Assert.That(builder.Compression, Is.EqualTo("zstd"));
            Assert.That(builder.ToOptions().Compressor, Is.SameAs(ZstdCompressor.Default));
        });
    }

    [Test]
    public void Validate_ACodecWithoutTheNativeBlockPath_IsRefusedAtConstruction()
    {
        // GZip and Brotli implement only the HTTP body path, so their MethodByte throws. Catching that when the
        // client is built beats discovering it mid-query, after the Query packet has promised the server frames.
        var options = new ClickHouseTcpClientOptions { Host = "localhost", Compressor = new GZipCompressor() };

        var failure = Assert.Throws<ArgumentException>(() => new ClickHouseTcpClient(options));

        Assert.That(failure.Message, Does.Contain("native block path").And.Contains(nameof(Lz4Compressor)));
    }

    [Test]
    public async Task Validate_TheBuiltInBlockCodecsAndNone_AreAccepted()
    {
        // Construction validates without connecting, so this needs no server.
        IClickHouseCompressor[] accepted = [Lz4Compressor.Default, ZstdCompressor.Default, null];

        foreach (IClickHouseCompressor codec in accepted)
        {
            var client = new ClickHouseTcpClient(new ClickHouseTcpClientOptions { Host = "localhost", Compressor = codec });
            await client.DisposeAsync();
        }

        Assert.Pass();
    }
}
