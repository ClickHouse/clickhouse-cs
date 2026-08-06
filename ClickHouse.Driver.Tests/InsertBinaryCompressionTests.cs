using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;
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

    // null => uncompressed; then GZip (default + level knob), Brotli, LZ4 and ZSTD (both vendored
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
    public void InsertOptions_DefaultCompressor_IsGZip()
    {
        // Guards the "unchanged by default" contract: omitting Compressor keeps GZip compression on.
        Assert.That(new InsertOptions().Compressor, Is.SameAs(GZipCompressor.Default));
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
