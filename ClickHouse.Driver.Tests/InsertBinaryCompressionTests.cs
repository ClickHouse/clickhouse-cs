using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

[TestFixture]
[Category("Cloud")]
public class InsertBinaryCompressionTests : AbstractConnectionTestFixture
{
    private class SimplePoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    private async Task<string> CreateTableAsync([CallerMemberName] string testName = null)
    {
        var tableName = CreateTableName(testName);
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (Id UInt64, Value String)
            ENGINE = MergeTree() ORDER BY Id");
        return tableName;
    }

    // null => uncompressed; then GZip (shared instance + level knob), Brotli, LZ4 and ZSTD (both vendored
    // codecs, the latter at its default and at a high level), exercising each Content-Encoding the
    // driver can send.
    private static IEnumerable<IClickHouseCompressor> Compressors()
    {
        yield return null;
        yield return GZipCompressor.Default;
        yield return new GZipCompressor(CompressionLevel.Optimal);
        yield return BrotliCompressor.Default;
        yield return new BrotliCompressor(CompressionLevel.Optimal);
        yield return Lz4Compressor.Default;
        yield return ZstdCompressor.Default;
        yield return new ZstdCompressor(level: 19);
    }

    [Test]
    public void InsertOptions_DefaultCompressor_IsZstd()
    {
        // Omitting Compressor compresses with ZSTD at its default level. Only the shared instance
        // will do: a per-InsertOptions compressor would allocate a fresh zstd context pool per
        // insert, which is what the static Default exists to avoid.
        Assert.That(new InsertOptions().Compressor, Is.SameAs(ZstdCompressor.Default));
    }

    // Each codec the driver can send, with the Content-Encoding it must declare for it. The first case
    // omits Compressor altogether — that is the default this fixture's flip is about; the last sets it
    // to null, the opposite contract: no compression, and therefore no header at all. The bool says
    // which of the two an argument of null means.
    private static IEnumerable<TestCaseData> DeclaredContentEncodings()
    {
        yield return new TestCaseData(null, false, "zstd").SetName("{m}(default => zstd)");
        yield return new TestCaseData(ZstdCompressor.Default, true, "zstd").SetName("{m}(zstd)");
        yield return new TestCaseData(GZipCompressor.Default, true, "gzip").SetName("{m}(gzip)");
        yield return new TestCaseData(BrotliCompressor.Default, true, "br").SetName("{m}(br)");
        yield return new TestCaseData(Lz4Compressor.Default, true, "lz4").SetName("{m}(lz4)");
        yield return new TestCaseData(null, true, null).SetName("{m}(uncompressed => no header)");
    }

    /// <summary>
    /// The <c>Content-Encoding</c> each codec declares on the wire, and — for the default — that the
    /// body really is a zstd frame carrying the caller's rows. Asserted on the outgoing request rather
    /// than inferred from a successful insert, because a body whose declared codec does not match its
    /// bytes is the one failure mode a round-trip cannot distinguish. The property assertion above
    /// pins what the default *is*; this pins that it is what actually goes out, and that switching
    /// codecs still declares the right token.
    /// </summary>
    /// <remarks>
    /// The zstd body is decoded with the upstream <c>ZstdSharp.Port</c> package rather than the
    /// driver's vendored copy, so a frame only the driver could read would not pass. Supplying
    /// <c>ColumnTypes</c> skips the schema probe, leaving the insert as the only request the stub
    /// endpoint has to answer.
    /// </remarks>
    [TestCaseSource(nameof(DeclaredContentEncodings))]
    public async Task InsertBinaryAsync_WithACompressor_DeclaresTheMatchingContentEncoding(
        IClickHouseCompressor compressor,
        bool setCompressor,
        string expectedContentEncoding)
    {
        byte[] sentBody = null;
        string sentContentEncoding = null;

        using var httpClient = MockHttpClientHelper.Create(async (request, _) =>
        {
            sentBody = await request.Content.ReadAsByteArrayAsync();
            sentContentEncoding = request.Content.Headers.TryGetValues("Content-Encoding", out var values)
                ? string.Join(", ", values)
                : null;
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };
        });

        using var stubbedClient = new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });

        var columnTypes = new Dictionary<string, string>
        {
            ["Id"] = "UInt64",
            ["Value"] = "String",
        };

        // The default case must construct InsertOptions without naming Compressor at all; every other
        // case sets it, including the explicit null that turns compression off.
        var options = setCompressor
            ? new InsertOptions { Compressor = compressor, ColumnTypes = columnTypes }
            : new InsertOptions { ColumnTypes = columnTypes };

        await stubbedClient.InsertBinaryAsync(
            TestUtilities.CreateTableName("compressor_wire"),
            new[] { "Id", "Value" },
            new List<object[]> { new object[] { 1UL, "hello" } },
            options);

        // The body is the whole request: the INSERT statement, then the rows in RowBinary —
        // (UInt64 1, String "hello") is 8 little-endian bytes followed by the string's varint length
        // and its UTF-8 bytes.
        var expectedRows = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 5, 0x68, 0x65, 0x6C, 0x6C, 0x6F };

        Assert.That(sentContentEncoding, Is.EqualTo(expectedContentEncoding));
        Assert.That(sentBody, Is.Not.Null.And.Length.GreaterThan(4));

        if (expectedContentEncoding != "zstd")
            return;

        Assert.That(
            sentBody.Take(4),
            Is.EqualTo(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }),
            "a body declared as zstd must start with the zstd frame magic");

        var decompressed = Decompressed(sentBody);
        Assert.That(decompressed, Has.Length.GreaterThan(expectedRows.Length));
        Assert.Multiple(() =>
        {
            Assert.That(
                Encoding.UTF8.GetString(decompressed, 0, decompressed.Length - expectedRows.Length),
                Does.StartWith("INSERT INTO").And.Contains("FORMAT RowBinary"));
            Assert.That(
                decompressed.Skip(decompressed.Length - expectedRows.Length),
                Is.EqualTo(expectedRows),
                "the frame must decode back to the rows the caller passed");
        });

        static byte[] Decompressed(byte[] frame)
        {
            using var source = new MemoryStream(frame);
            using var decompressing = new ZstdSharp.DecompressionStream(source);
            using var plain = new MemoryStream();
            decompressing.CopyTo(plain);
            return plain.ToArray();
        }
    }

    /// <summary>
    /// The default path against a real server: an insert with no <see cref="InsertOptions.Compressor"/>
    /// set is accepted and stores the rows. A request body's <c>Content-Encoding</c> is a declaration
    /// rather than an offer, so a server that did not understand the default codec would fail the
    /// insert outright instead of falling back — which is exactly what makes this worth a real-server
    /// case of its own rather than leaving the default covered only by the wire assertion above.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_WithDefaultOptions_RoundTripsThroughTheDefaultCodec()
    {
        var tableName = await CreateTableAsync();

        await client.InsertBinaryAsync(
            tableName,
            new[] { "Id", "Value" },
            new List<object[]>
            {
                new object[] { 1UL, "hello" },
                new object[] { 2UL, "world" },
            });

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT Id, Value FROM {tableName} ORDER BY Id");

        var readBack = new List<(ulong Id, string Value)>();
        while (reader.Read())
            readBack.Add((reader.GetFieldValue<ulong>(0), reader.GetString(1)));

        Assert.That(readBack, Is.EqualTo(new[] { (1UL, "hello"), (2UL, "world") }));
    }

    [Test]
    public async Task InsertBinaryAsync_ObjectArray_WithCompressor_ShouldRoundTripData(
        [ValueSource(nameof(Compressors))] IClickHouseCompressor compressor)
    {
        var tableName = await CreateTableAsync();
        var options = new InsertOptions
        {
            Compressor = compressor,
            ColumnTypes = new Dictionary<string, string>
            {
                ["Id"] = "UInt64",
                ["Value"] = "String",
            },
        };

        await client.InsertBinaryAsync(
            tableName,
            new[] { "Id", "Value" },
            new List<object[]>
            {
                new object[] { 1UL, "hello" },
                new object[] { 2UL, "world" },
            },
            options);

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT Id, Value FROM {tableName} ORDER BY Id");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
        Assert.That(reader.GetString(1), Is.EqualTo("hello"));

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
        Assert.That(reader.GetString(1), Is.EqualTo("world"));

        Assert.That(reader.Read(), Is.False);
    }

    [Test]
    public async Task InsertBinaryAsync_Poco_WithCompressor_ShouldRoundTripData(
        [ValueSource(nameof(Compressors))] IClickHouseCompressor compressor)
    {
        var tableName = await CreateTableAsync();
        client.RegisterBinaryInsertType<SimplePoco>();

        var options = new InsertOptions
        {
            Compressor = compressor,
        };

        await client.InsertBinaryAsync(
            tableName,
            new[]
            {
                new SimplePoco { Id = 1UL, Value = "hello" },
                new SimplePoco { Id = 2UL, Value = "world" },
            },
            options);

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT Id, Value FROM {tableName} ORDER BY Id");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
        Assert.That(reader.GetString(1), Is.EqualTo("hello"));

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
        Assert.That(reader.GetString(1), Is.EqualTo("world"));

        Assert.That(reader.Read(), Is.False);
    }

    /// <summary>
    /// A zstd-compressed insert across several batches, read back through a zstd-compressed response:
    /// both directions of the codec against a real server in one case, over enough rows that the write
    /// stream spans more than one zstd block.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_ZstdCompressed_MultipleBatches_RoundTripsThroughAZstdResponse()
    {
        const int rowCount = 2500;
        var tableName = await CreateTableAsync();
        var options = new InsertOptions
        {
            Compressor = ZstdCompressor.Default,
            BatchSize = 1000,
            ColumnTypes = new Dictionary<string, string>
            {
                ["Id"] = "UInt64",
                ["Value"] = "String",
            },
        };

        var rows = Enumerable.Range(0, rowCount)
            .Select(i => new object[] { (ulong)i, $"value_{i}" })
            .ToList();

        await client.InsertBinaryAsync(tableName, new[] { "Id", "Value" }, rows, options);

        var readBack = new List<(ulong Id, string Value)>();
        using (var reader = await client.ExecuteReaderAsync(
                   $"SELECT Id, Value FROM {tableName} ORDER BY Id",
                   options: new QueryOptions { AcceptEncoding = "zstd" }))
        {
            while (reader.Read())
                readBack.Add((reader.GetFieldValue<ulong>(0), reader.GetString(1)));
        }

        Assert.Multiple(() =>
        {
            Assert.That(readBack, Has.Count.EqualTo(rowCount));
            Assert.That(readBack[0], Is.EqualTo((0UL, "value_0")));
            Assert.That(readBack[^1], Is.EqualTo(((ulong)rowCount - 1, $"value_{rowCount - 1}")));
        });
    }

    [Test]
    public async Task InsertBinaryAsync_Uncompressed_MultipleBatches_ShouldRoundTripAllRows()
    {
        // Exercises the uncompressed path across several batches: each batch writes straight
        // to the (leave-open) memory stream, is seeked to 0, and posted without Content-Encoding.
        const int rowCount = 2500;
        var tableName = await CreateTableAsync();
        var options = new InsertOptions
        {
            Compressor = null,
            BatchSize = 1000,
            ColumnTypes = new Dictionary<string, string>
            {
                ["Id"] = "UInt64",
                ["Value"] = "String",
            },
        };

        var rows = Enumerable.Range(0, rowCount)
            .Select(i => new object[] { (ulong)i, $"value_{i}" })
            .ToList();

        await client.InsertBinaryAsync(tableName, new[] { "Id", "Value" }, rows, options);

        var count = (ulong)await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Assert.That(count, Is.EqualTo((ulong)rowCount));

        var sum = await client.ExecuteScalarAsync($"SELECT sum(Id) FROM {tableName}");
        // sum(Id) comes back as a UInt64 aggregate (ulong); it fits well within Int64 here, so
        // Convert.ToInt64 normalizes it for comparison regardless of the exact returned CLR type.
        var expected = ((long)rowCount - 1) * rowCount / 2;
        Assert.That(Convert.ToInt64(sum), Is.EqualTo(expected));
    }
}
