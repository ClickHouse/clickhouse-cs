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
/// <c>Accept-Encoding</c> — and therefore over zstd, since that is what the server picks from it. That is
/// deliberate: it pins the negotiation and the decode together, so a default that stopped being decodable
/// would fail here rather than silently degrade. It also means the inherited <c>client</c> is only ever
/// the right instrument for a test <i>about the default</i>: a test that means to exercise one specific
/// codec must say so with <see cref="CreateClientWith"/>, or it silently follows the default wherever it
/// goes next.
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
    public async Task DefaultAcceptEncoding_AgainstRealServer_IsAnsweredWithZstd()
    {
        using var result = await client.ExecuteRawResultAsync(
            "SELECT number FROM numbers(500) FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = ResponseDecompression.DefaultAcceptEncoding });

        Assert.That(result.ContentEncoding, Is.EqualTo("zstd"));
    }

    [TestCase("lz4", ExpectedResult = "lz4")]
    [TestCase("zstd", ExpectedResult = "zstd")]
    [TestCase("gzip", ExpectedResult = "gzip")]
    [TestCase("br", ExpectedResult = "br")]
    [TestCase("lz4, gzip, deflate", ExpectedResult = "lz4")]
    [TestCase("gzip, br", ExpectedResult = "br", TestName = "{m}(br wins over gzip regardless of order)")]
    [TestCase("gzip, lz4, br, zstd", ExpectedResult = "zstd", TestName = "{m}(zstd wins even when listed last)")]
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
    /// Every codec the driver decodes, over the driver's own handler — which now leaves
    /// <c>AutomaticDecompression</c> off, so gzip and deflate reach the driver still encoded and go through
    /// its decoder just like lz4 and br. (While the handler carried a <c>GZip | Deflate</c> mask those two
    /// were stripped by .NET before the driver saw them, and could only be exercised through a
    /// caller-supplied plain <see cref="HttpClient"/>, as
    /// <c>ConnectionTests.ExecuteReaderAsync_WithAnHttpClientThatCannotDecodeTheCodec_DecodesItInTheDriver</c>
    /// still does.)
    /// </summary>
    [TestCase("lz4")]
    [TestCase("zstd")]
    [TestCase("br")]
    [TestCase("gzip")]
    [TestCase("deflate")]
    public async Task ExecuteReaderAsync_WithACompressedResponse_ReadsValuesIdenticalToUncompressed(string acceptEncoding)
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
    /// silently-uncompressed response would let a broken decoder pass. The wire check and the decode both
    /// go through the <i>same</i> lz4 client, so what was proven on the wire is what is then decoded; the
    /// fixture's default client would not do, since it now negotiates zstd.
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

        using var lz4 = CreateClientWith("lz4");

        using (var rawLz4 = await lz4.ExecuteRawResultAsync(
            $"SELECT payload FROM {table} ORDER BY id FORMAT TSV"))
        {
            Assert.That(rawLz4.ContentEncoding, Is.EqualTo("lz4"), "the server must have answered with lz4");

            var frame = await rawLz4.ReadAsByteArrayAsync();
            Assert.That(CountLz4Blocks(frame), Is.GreaterThan(1), "the body must span multiple LZ4 blocks");
        }

        using var uncompressed = CreateUncompressedClient();
        var expected = await ReadPayloadsAsync(uncompressed, table);
        var actual = await ReadPayloadsAsync(lz4, table);

        Assert.Multiple(() =>
        {
            Assert.That(actual, Has.Count.EqualTo(20000));
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// The zstd counterpart, and the case that pins streaming decode for this codec: over 1 MiB of
    /// plaintext, so the server's <c>ZSTD_e_flush</c> boundaries land mid-body and the decoder has to
    /// carry state across reads instead of decoding one self-contained frame. The wire check first
    /// proves the body really is a zstd frame, otherwise a silently-uncompressed response would let a
    /// broken decoder pass — and, as in the lz4 twin, it goes through the same client the decode does, so
    /// naming the codec once decides both halves.
    /// </summary>
    [Test]
    public async Task ExecuteReaderAsync_WithMultiMegabyteZstdResponse_DecodesEveryRow()
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

        using var zstd = CreateClientWith("zstd");

        using (var rawZstd = await zstd.ExecuteRawResultAsync(
            $"SELECT payload FROM {table} ORDER BY id FORMAT TSV"))
        {
            Assert.That(rawZstd.ContentEncoding, Is.EqualTo("zstd"), "the server must have answered with zstd");

            var frame = await rawZstd.ReadAsByteArrayAsync();
            Assert.Multiple(() =>
            {
                Assert.That(frame[..4], Is.EqualTo(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }), "expected a zstd frame magic");
                Assert.That(frame.Length, Is.LessThan(plaintextBytes), "the body must actually be compressed");
            });
        }

        using var uncompressed = CreateUncompressedClient();
        var expected = await ReadPayloadsAsync(uncompressed, table);
        var actual = await ReadPayloadsAsync(zstd, table);

        Assert.Multiple(() =>
        {
            Assert.That(actual, Has.Count.EqualTo(20000));
            Assert.That(actual, Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// <c>ReadDecompressedStreamAsync</c> over zstd, which is the member a caller uses when they asked
    /// for a compressed export and then want the plaintext after all.
    /// </summary>
    [Test]
    public async Task ReadDecompressedStreamAsync_WithZstd_YieldsTheSameBytesAsAnUncompressedExport()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int64) ENGINE Memory");
        await client.ExecuteNonQueryAsync($"INSERT INTO {table} SELECT number FROM numbers(3000)");

        using var plainResult = await client.ExecuteRawResultAsync($"SELECT id FROM {table} ORDER BY id FORMAT TSV");
        var expected = await plainResult.ReadAsByteArrayAsync();

        using var zstdResult = await client.ExecuteRawResultAsync(
            $"SELECT id FROM {table} ORDER BY id FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "zstd" });

        Assert.That(zstdResult.ContentEncoding, Is.EqualTo("zstd"));

        using var decompressed = await zstdResult.ReadDecompressedStreamAsync();
        using var buffer = new MemoryStream();
        await decompressed.CopyToAsync(buffer);

        Assert.That(buffer.ToArray(), Is.EqualTo(expected));
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
    /// The rule a raw export now follows, and the reason it no longer depends on anyone's decompression
    /// mask: with nothing configured the driver advertises no codec, so the server sends plaintext and
    /// there is nothing to decode. Asserted for both handlers that used to disagree here — the driver's own
    /// (which stripped a gzip it had not asked for, so the body <i>looked</i> plaintext) and a
    /// caller-supplied one with <c>AutomaticDecompression = None</c> (which received that gzip verbatim).
    /// Both now see the same bytes, which is the whole point.
    /// </summary>
    [TestCase(false, TestName = "{m}(the driver's own handler)")]
    [TestCase(true, TestName = "{m}(a caller-supplied handler that decodes nothing)")]
    public async Task ExecuteRawResultAsync_WithNothingConfigured_ReturnsPlaintextWhateverTheCallersMask(bool callerSuppliedClient)
    {
        using var raw = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var httpClient = new HttpClient(raw);
        using ClickHouseClient exporter = callerSuppliedClient
            ? new ClickHouseClient(new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
            {
                HttpClient = httpClient,
            })
            : null;

        using var result = await (exporter ?? client).ExecuteRawResultAsync("SELECT number FROM numbers(2000) FORMAT TSV");
        var body = await result.ReadAsByteArrayAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.ContentEncoding, Is.Null);
            Assert.That(body[..2], Is.EqualTo(new byte[] { (byte)'0', (byte)'\n' }), "expected plaintext TSV");
        });
    }

    /// <summary>
    /// Naming a codec is how a caller gets verbatim compressed bytes, and over the driver's own handler that
    /// no longer depends on the server's codec ranking: the offer is exactly <c>lz4</c>, because the handler
    /// leaves <c>AutomaticDecompression</c> off and so no longer appends gzip/deflate to it. (While it did,
    /// this passed only because ClickHouse's fixed preference happens to rank lz4 above both — a server
    /// without lz4 would have answered gzip and had it silently decoded to plaintext.) Nothing decodes lz4
    /// on the way out either, so <c>Content-Encoding</c> survives and the body is an LZ4 frame.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_AskingForLz4_OverTheDefaultHandler_ReceivesAnLz4Frame()
    {
        // A default client: the driver's own handler, AutomaticDecompression = None.
        using var result = await client.ExecuteRawResultAsync(
            "SELECT number FROM numbers(2000) FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "lz4" });

        var body = await result.ReadAsByteArrayAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.ContentEncoding, Is.EqualTo("lz4"),
                "the exact codec asked for must reach the server, and nothing must decode it on the way back");
            Assert.That(body[..4], Is.EqualTo(new byte[] { 0x04, 0x22, 0x4D, 0x18 }),
                "expected an LZ4 frame magic number, i.e. genuinely compressed bytes");
        });
    }

    /// <summary>
    /// The uncompressed baseline the comparisons in this fixture rest on, verified rather than assumed:
    /// <c>AcceptEncoding = "identity"</c> really does get an uncompressed body. It did not while the
    /// driver's handler carried a <c>GZip | Deflate</c> mask — the handler appended those two to the
    /// request, ClickHouse answered gzip, and the handler decoded and stripped it, so the baseline read
    /// correctly while the wire was compressed after all.
    /// </summary>
    [Test]
    public async Task UncompressedClient_AgainstRealServer_ReceivesABodyWithNoContentEncoding()
    {
        using var uncompressed = CreateUncompressedClient();

        using var result = await uncompressed.ExecuteRawResultAsync("SELECT number FROM numbers(2000) FORMAT TSV");
        var body = await result.ReadAsByteArrayAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.ContentEncoding, Is.Null);
            Assert.That(body[..2], Is.EqualTo(new byte[] { (byte)'0', (byte)'\n' }), "expected plaintext TSV");
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
