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
    private string CreateTestTableName([CallerMemberName] string testName = null)
        => SanitizeTableName($"test_compress_{testName}_{Guid.NewGuid():N}");

    private class SimplePoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    private async Task<string> CreateTableAsync([CallerMemberName] string testName = null)
    {
        var tableName = CreateTestTableName(testName);
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS test.{tableName}
            (Id UInt64, Value String)
            ENGINE = MergeTree() ORDER BY Id");
        return tableName;
    }

    // null => uncompressed; then GZip (default + level knob) and Brotli, exercising both Content-Encodings.
    private static IEnumerable<IClickHouseCompressor> Compressors()
    {
        yield return null;
        yield return GZipCompressor.Default;
        yield return new GZipCompressor(CompressionLevel.Optimal);
        yield return BrotliCompressor.Default;
        yield return new BrotliCompressor(CompressionLevel.Optimal);
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
        try
        {
            var options = new InsertOptions
            {
                Database = "test",
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
                $"SELECT Id, Value FROM test.{tableName} ORDER BY Id");

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
            Assert.That(reader.GetString(1), Is.EqualTo("hello"));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
            Assert.That(reader.GetString(1), Is.EqualTo("world"));

            Assert.That(reader.Read(), Is.False);
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_Poco_WithCompressor_ShouldRoundTripData(
        [ValueSource(nameof(Compressors))] IClickHouseCompressor compressor)
    {
        var tableName = await CreateTableAsync();
        try
        {
            client.RegisterBinaryInsertType<SimplePoco>();

            var options = new InsertOptions
            {
                Database = "test",
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
                $"SELECT Id, Value FROM test.{tableName} ORDER BY Id");

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(1UL));
            Assert.That(reader.GetString(1), Is.EqualTo("hello"));

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetFieldValue<ulong>(0), Is.EqualTo(2UL));
            Assert.That(reader.GetString(1), Is.EqualTo("world"));

            Assert.That(reader.Read(), Is.False);
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_Uncompressed_MultipleBatches_ShouldRoundTripAllRows()
    {
        // Exercises the uncompressed path across several batches: each batch writes straight
        // to the (leave-open) memory stream, is seeked to 0, and posted without Content-Encoding.
        const int rowCount = 2500;
        var tableName = await CreateTableAsync();
        try
        {
            var options = new InsertOptions
            {
                Database = "test",
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

            var count = (ulong)await client.ExecuteScalarAsync($"SELECT count() FROM test.{tableName}");
            Assert.That(count, Is.EqualTo((ulong)rowCount));

            var sum = await client.ExecuteScalarAsync($"SELECT sum(Id) FROM test.{tableName}");
            // sum(UInt64) comes back as a large unsigned aggregate; compare via string to stay type-agnostic
            var expected = ((long)rowCount - 1) * rowCount / 2;
            Assert.That(Convert.ToInt64(sum), Is.EqualTo(expected));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }
}
