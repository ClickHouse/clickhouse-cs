using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Http;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Http;

/// <summary>
/// End-to-end response compression against a real ClickHouse server: the server encodes, the driver
/// decodes. Values are compared against the same query read uncompressed, so the expectations come from
/// the server rather than from the code under test.
/// <para>
/// The inherited <c>client</c> is a default one, which means these run over the driver's default
/// <c>Accept-Encoding</c> — and therefore over lz4, since that is what the server picks from it. That is
/// deliberate: it pins the negotiation and the decode together, so a default that stopped being decodable
/// would fail here rather than silently degrade.
/// </para>
/// </summary>
[TestFixture]
public class ResponseDecompressionIntegrationTests : AbstractConnectionTestFixture
{
    /// <summary>A client that opts out of compression, for uncompressed baselines.</summary>
    private static ClickHouseClient CreateUncompressedClient()
        => new(new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            AcceptEncoding = "identity",
        });

    private static ClickHouseClient CreateClientWith(string acceptEncoding)
        => new(new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            AcceptEncoding = acceptEncoding,
        });

    /// <summary>
    /// The default negotiation, confirmed on the wire: ClickHouse resolves <c>Accept-Encoding</c> by its
    /// own fixed preference order, so what the driver advertises decides what it has to decode. If the
    /// server ever answered with something else, every other test here would still pass by accident —
    /// this one would not.
    /// </summary>
    [Test]
    public async Task DefaultAcceptEncoding_AgainstRealServer_IsAnsweredWithLz4()
    {
        using var result = await client.ExecuteRawResultAsync(
            "SELECT number FROM numbers(500) FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = ResponseDecompression.DefaultAcceptEncoding });

        Assert.That(result.ContentEncoding, Is.EqualTo("lz4"));
    }

    [TestCase("lz4", ExpectedResult = "lz4")]
    [TestCase("gzip", ExpectedResult = "gzip")]
    [TestCase("br", ExpectedResult = "br")]
    [TestCase("lz4, gzip, deflate", ExpectedResult = "lz4")]
    [TestCase("gzip, br", ExpectedResult = "br", TestName = "{m}(br wins over gzip regardless of order)")]
    public async Task<string> AcceptEncoding_AgainstRealServer_IsResolvedByTheServersOwnPreference(string acceptEncoding)
    {
        using var raw = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var httpClient = new HttpClient(raw);
        using var probe = new ClickHouseClient(new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = httpClient,
            AcceptEncoding = acceptEncoding,
        });

        using var result = await probe.ExecuteRawResultAsync("SELECT number FROM numbers(500) FORMAT TSV");
        return result.ContentEncoding;
    }

    /// <summary>
    /// Only lz4 and br are listed: the driver's own handler sets <c>AutomaticDecompression</c> to
    /// <c>GZip | Deflate</c>, so a gzip or deflate response is decoded by .NET before the driver sees it
    /// and would not exercise the driver's decoder at all. Those two are covered against a real server by
    /// <c>ConnectionTests.ExecuteReaderAsync_WithAnHttpClientThatCannotDecodeTheCodec_DecodesItInTheDriver</c>, which uses a
    /// plain <see cref="HttpClient"/>.
    /// </summary>
    [TestCase("lz4")]
    [TestCase("br")]
    public async Task ExecuteReaderAsync_WithACodecDotNetCannotStrip_ReadsValuesIdenticalToUncompressed(string acceptEncoding)
    {
        var table = CreateTableName($"codec_{acceptEncoding}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64, s String, d DateTime64(3)) ENGINE Memory");
        await client.ExecuteNonQueryAsync(
            $"INSERT INTO {table} SELECT number, concat('row-', toString(number)), toDateTime64('2024-01-01 00:00:00.123', 3) + number FROM numbers(5000)");

        using var uncompressed = CreateUncompressedClient();
        var expected = await ReadRowsAsync(uncompressed, table);

        using var compressed = CreateClientWith(acceptEncoding);
        var actual = await ReadRowsAsync(compressed, table);

        Assert.Multiple(() =>
        {
            Assert.That(expected, Has.Count.EqualTo(5000), "the baseline must have read the rows");
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// A payload well over 1 MB, so the LZ4 body the server sends is split into SEVERAL blocks and the
    /// decoder has to stream across block boundaries instead of decoding one self-contained block. Do not
    /// shrink this: a small body fits in a single block and would not exercise streaming decode at all.
    /// The test first proves on the wire that the response really is a multi-block LZ4 body — otherwise a
    /// silently-uncompressed response would let a broken decoder pass.
    /// </summary>
    [Test]
    public async Task ExecuteReaderAsync_WithMultiMegabyteLz4Response_DecodesEveryRowAcrossBlockBoundaries()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64, payload String) ENGINE Memory");

        // 20,000 rows x ~70 bytes of non-repeating hex ~= 1.4 MB of plaintext.
        await client.ExecuteNonQueryAsync(
            $"INSERT INTO {table} SELECT number, concat(toString(number), '-', hex(MD5(toString(number))), '-', hex(MD5(toString(number + 1)))) FROM numbers(20000)");

        var plaintextBytes = Convert.ToInt64(
            await client.ExecuteScalarAsync($"SELECT sum(length(payload) + 1) FROM {table}"),
            CultureInfo.InvariantCulture);
        Assert.That(plaintextBytes, Is.GreaterThan(1024 * 1024), "the payload must exceed 1 MiB");

        using (var rawLz4 = await client.ExecuteRawResultAsync(
            $"SELECT payload FROM {table} ORDER BY id FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "lz4" }))
        {
            Assert.That(rawLz4.ContentEncoding, Is.EqualTo("lz4"), "the server must have answered with lz4");

            var frame = await rawLz4.ReadAsByteArrayAsync();
            Assert.That(CountLz4Blocks(frame), Is.GreaterThan(1), "the body must span multiple LZ4 blocks");
        }

        using var uncompressed = CreateUncompressedClient();
        var expected = await ReadPayloadsAsync(uncompressed, table);
        var actual = await ReadPayloadsAsync(client, table);

        Assert.Multiple(() =>
        {
            Assert.That(actual, Has.Count.EqualTo(20000));
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task ReadDecompressedStreamAsync_AgainstRealServer_YieldsTheSameBytesAsAnUncompressedExport()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {table} SELECT number FROM numbers(3000)");

        using var plainResult = await client.ExecuteRawResultAsync($"SELECT id FROM {table} ORDER BY id FORMAT TSV");
        var expected = await plainResult.ReadAsByteArrayAsync();

        using var lz4Result = await client.ExecuteRawResultAsync(
            $"SELECT id FROM {table} ORDER BY id FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "lz4" });

        // ClickHouse compresses whatever it is asked to, with no minimum body size, so this is not
        // conditional: an uncompressed answer here means the negotiation broke.
        Assert.That(lz4Result.ContentEncoding, Is.EqualTo("lz4"));

        using var decompressed = await lz4Result.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(expected));
    }

    /// <summary>
    /// A raw request keeps the driver's historical <c>gzip, deflate</c> rather than the default list, so
    /// what a caller receives is unchanged. With the driver's own handler, whose mask covers exactly those
    /// two, the framework decodes and strips them: plaintext body, no <c>Content-Encoding</c>.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_WithDefaultSettings_ReturnsAPlaintextBody()
    {
        using var result = await client.ExecuteRawResultAsync("SELECT number FROM numbers(2000) FORMAT TSV");

        var body = await result.ReadAsByteArrayAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.ContentEncoding, Is.Null);
            Assert.That(body[..2], Is.EqualTo(new byte[] { (byte)'0', (byte)'\n' }), "expected plaintext TSV");
        });
    }

    /// <summary>
    /// The configuration this exemption exists to protect: a caller-supplied <see cref="HttpClient"/> with
    /// no <c>AutomaticDecompression</c>, taking a raw export with nothing configured. It used to receive
    /// gzip bytes because the driver advertised <c>gzip, deflate</c> for every request, and it must still
    /// receive gzip bytes — advertising the driver's own default here would hand it lz4 that neither the
    /// framework nor the driver would decode for it.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_WithAnHttpClientThatDecodesNothing_StillReceivesGzip()
    {
        using var raw = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var httpClient = new HttpClient(raw);
        using var exporter = new ClickHouseClient(new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = httpClient,
        });

        using var result = await exporter.ExecuteRawResultAsync("SELECT number FROM numbers(2000) FORMAT TSV");
        var body = await result.ReadAsByteArrayAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.ContentEncoding, Is.EqualTo("gzip"));
            Assert.That(body[..2], Is.EqualTo(new byte[] { 0x1F, 0x8B }), "expected a gzip magic number");
        });
    }

    [Test]
    public async Task ExecuteScalarAsync_OverTheDefaultCompressedPath_ReturnsTheServerValue()
    {
        var value = await client.ExecuteScalarAsync("SELECT count() FROM numbers(1234)");

        Assert.That(Convert.ToInt64(value, CultureInfo.InvariantCulture), Is.EqualTo(1234));
    }

    [Test]
    public async Task ConnectionString_WithAcceptEncoding_ReadsThroughTheAdoLayer()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.AcceptEncoding = "lz4";

        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {table} SELECT number FROM numbers(4000)");

        using var lz4Connection = new ClickHouseConnection(builder.ToString());
        lz4Connection.Open();
        using var command = lz4Connection.CreateCommand();
        command.CommandText = $"SELECT sum(id) FROM {table}";

        Assert.That(
            Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture),
            Is.EqualTo(4000L * 3999 / 2));
    }

    /// <summary>
    /// Walks the LZ4 frame format — magic(4) FLG(1) BD(1) [contentSize(8)] [dictId(4)] HC(1) followed by
    /// length-prefixed blocks terminated by a zero length — and returns the number of data blocks.
    /// </summary>
    private static int CountLz4Blocks(byte[] frame)
    {
        Assert.That(frame[..4], Is.EqualTo(new byte[] { 0x04, 0x22, 0x4D, 0x18 }), "expected an LZ4 frame magic");

        var flg = frame[4];
        var offset = 6;
        if ((flg & 0x08) != 0)
            offset += 8; // content size present
        if ((flg & 0x01) != 0)
            offset += 4; // dictionary id present
        offset += 1;     // header checksum

        var blocks = 0;
        while (offset + 4 <= frame.Length)
        {
            var size = BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(offset));
            offset += 4;
            if (size == 0)
                break; // end mark

            offset += (int)(size & 0x7FFF_FFFF);
            blocks++;
        }

        return blocks;
    }

    private static async Task<List<string>> ReadRowsAsync(ClickHouseClient source, string table)
    {
        using var reader = await source.ExecuteReaderAsync($"SELECT id, s, d FROM {table} ORDER BY id");
        var rows = new List<string>();
        while (reader.Read())
        {
            rows.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"{reader.GetInt64(0)}|{reader.GetString(1)}|{reader.GetDateTime(2):O}"));
        }

        return rows;
    }

    private static async Task<List<string>> ReadPayloadsAsync(ClickHouseClient source, string table)
    {
        using var reader = await source.ExecuteReaderAsync($"SELECT payload FROM {table} ORDER BY id");
        var rows = new List<string>();
        while (reader.Read())
            rows.Add(reader.GetString(0));

        return rows;
    }
}
