using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Tests.Utilities;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Where a binary insert puts its <c>INSERT INTO ... FORMAT ...</c> statement:
/// <see cref="InsertQueryPlacement.Body"/> (the default) writes it ahead of the rows,
/// <see cref="InsertQueryPlacement.Url"/> sends it as the <c>query</c> URL parameter and leaves the
/// body to the rows alone.
/// </summary>
[TestFixture]
[Category("Cloud")]
public class InsertBinaryQueryPlacementTests : AbstractConnectionTestFixture
{
    private class SimplePoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    private static readonly IReadOnlyDictionary<string, string> ColumnTypes = new Dictionary<string, string>
    {
        ["Id"] = "UInt64",
        ["Value"] = "String",
    };

    // (UInt64 1, String "hello") in RowBinary: 8 little-endian bytes, then the string's varint length
    // and its UTF-8 bytes.
    private static readonly byte[] ExpectedRows = { 1, 0, 0, 0, 0, 0, 0, 0, 5, 0x68, 0x65, 0x6C, 0x6C, 0x6F };

    private async Task<string> CreateTableAsync(string prefix = null, [CallerMemberName] string testName = null)
    {
        var tableName = CreateTableName(prefix, testName: testName);
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName}
            (Id UInt64, Value String)
            ENGINE = MergeTree() ORDER BY Id");
        return tableName;
    }

    [Test]
    public void InsertOptions_DefaultQueryPlacement_IsBody()
    {
        Assert.That(new InsertOptions().QueryPlacement, Is.EqualTo(InsertQueryPlacement.Body));
    }

    // Both placements, on both insert paths, compressed with the default codec and uncompressed: the
    // placement of the statement and the encoding of the body are independent settings, so each
    // combination has to hold.
    private static IEnumerable<TestCaseData> Placements()
    {
        foreach (var poco in new[] { false, true })
        {
            foreach (var compressor in new IClickHouseCompressor[] { ZstdCompressor.Default, null })
            {
                var codec = compressor?.ContentEncoding ?? "uncompressed";
                foreach (var placement in new[] { InsertQueryPlacement.Body, InsertQueryPlacement.Url })
                {
                    yield return new TestCaseData(placement, poco, compressor)
                        .SetName($"{{m}}({placement}, {(poco ? "poco" : "object[]")}, {codec})");
                }
            }
        }
    }

    /// <summary>
    /// The statement goes to exactly one of the two places, and the body framing follows it: in URL
    /// mode the body must be the rows and nothing else — a prologue or even a stray leading newline
    /// would be read by the server as row data and corrupt the first row.
    /// </summary>
    [TestCaseSource(nameof(Placements))]
    public async Task InsertBinaryAsync_WithQueryPlacement_PutsTheStatementInOnePlaceOnly(
        InsertQueryPlacement placement,
        bool poco,
        IClickHouseCompressor compressor)
    {
        Uri sentUri = null;
        byte[] sentBody = null;

        using var httpClient = MockHttpClientHelper.Create(async (request, _) =>
        {
            sentUri = request.RequestUri;
            sentBody = await request.Content.ReadAsByteArrayAsync();
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };
        });

        using var stubbedClient = new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });
        var table = TestUtilities.CreateTableName("query_placement");
        var options = new InsertOptions
        {
            QueryPlacement = placement,
            Compressor = compressor,
            ColumnTypes = ColumnTypes,
        };

        if (poco)
        {
            stubbedClient.RegisterBinaryInsertType<SimplePoco>();
            await stubbedClient.InsertBinaryAsync(
                table,
                new[] { new SimplePoco { Id = 1UL, Value = "hello" } },
                options);
        }
        else
        {
            await stubbedClient.InsertBinaryAsync(
                table,
                new[] { "Id", "Value" },
                new List<object[]> { new object[] { 1UL, "hello" } },
                options);
        }

        var expectedStatement = $"INSERT INTO {table} (`Id`, `Value`) FORMAT RowBinary";
        var urlQuery = HttpUtility.ParseQueryString(sentUri.Query).Get("query");
        var body = Decoded(sentBody, compressor);

        if (placement == InsertQueryPlacement.Url)
        {
            Assert.Multiple(() =>
            {
                Assert.That(urlQuery, Is.EqualTo(expectedStatement));
                Assert.That(body, Is.EqualTo(ExpectedRows), "the body must carry the rows alone");
            });
        }
        else
        {
            var prologue = Encoding.UTF8.GetBytes(expectedStatement + "\n");
            Assert.Multiple(() =>
            {
                Assert.That(urlQuery, Is.Null, "the statement is in the body, so no query parameter belongs on the URL");
                Assert.That(body, Is.EqualTo(prologue.Concat(ExpectedRows).ToArray()));
            });
        }
    }

    private static byte[] Decoded(byte[] body, IClickHouseCompressor compressor)
    {
        if (compressor == null)
            return body;

        using var source = new MemoryStream(body);
        using var decompressing = new ZstdSharp.DecompressionStream(source);
        using var plain = new MemoryStream();
        decompressing.CopyTo(plain);
        return plain.ToArray();
    }

    /// <summary>
    /// A URL-mode insert against a real server, on both paths and with the body both compressed and
    /// not: the server has to accept a body that starts at the first row and read the statement off
    /// the URL.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_WithQueryInUrl_RoundTripsThroughARealServer(
        [Values(false, true)] bool poco,
        [Values(false, true)] bool compressed)
    {
        var tableName = await CreateTableAsync($"{poco}_{compressed}");
        var options = new InsertOptions
        {
            QueryPlacement = InsertQueryPlacement.Url,
            Compressor = compressed ? ZstdCompressor.Default : null,
            ColumnTypes = ColumnTypes,
        };

        if (poco)
        {
            client.RegisterBinaryInsertType<SimplePoco>();
            await client.InsertBinaryAsync(
                tableName,
                new[]
                {
                    new SimplePoco { Id = 1UL, Value = "hello" },
                    new SimplePoco { Id = 2UL, Value = "world" },
                },
                options);
        }
        else
        {
            await client.InsertBinaryAsync(
                tableName,
                new[] { "Id", "Value" },
                new List<object[]>
                {
                    new object[] { 1UL, "hello" },
                    new object[] { 2UL, "world" },
                },
                options);
        }

        Assert.That(await ReadBackAsync(tableName), Is.EqualTo(new[] { (1UL, "hello"), (2UL, "world") }));
    }

    /// <summary>
    /// Several batches sent in parallel. Each batch re-derives its options through
    /// <c>InsertOptions.WithQueryId</c>, so a placement that failed to survive that copy would send
    /// every batch the other way — with the statement then missing from both the URL and the body.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_WithQueryInUrl_MultipleParallelBatches_InsertsEveryRow()
    {
        const int rowCount = 2500;
        var tableName = await CreateTableAsync();
        var options = new InsertOptions
        {
            QueryPlacement = InsertQueryPlacement.Url,
            BatchSize = 500,
            MaxDegreeOfParallelism = 4,
            ColumnTypes = ColumnTypes,
        };

        var rows = Enumerable.Range(0, rowCount)
            .Select(i => new object[] { (ulong)i, $"value_{i}" })
            .ToList();

        await client.InsertBinaryAsync(tableName, new[] { "Id", "Value" }, rows, options);

        var count = (ulong)await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        var sum = await client.ExecuteScalarAsync($"SELECT sum(Id) FROM {tableName}");
        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo((ulong)rowCount));
            Assert.That(Convert.ToInt64(sum), Is.EqualTo(((long)rowCount - 1) * rowCount / 2));
        });
    }

    /// <summary>
    /// A row that cannot be serialized still surfaces as a
    /// <see cref="ClickHouseBulkCopySerializationException"/> carrying that row. The serializers tell a
    /// row fault from a failure writing the query line by a flag the query line sets; with no query
    /// line to write, that flag has to start out set instead.
    /// </summary>
    /// <remarks>
    /// The <c>object[]</c> path only: a POCO's property types are fixed at compile time, so it has no
    /// equivalent of a value the column's serializer cannot accept.
    /// </remarks>
    [Test]
    public void InsertBinaryAsync_WithQueryInUrl_WhenARowCannotBeSerialized_ThrowsSerializationExceptionWithTheRow()
    {
        var table = TestUtilities.CreateTableName("query_placement_bad_row");
        var badRow = new object[] { "not-a-number", "hello" };

        var ex = Assert.CatchAsync<ClickHouseBulkCopySerializationException>(() => client.InsertBinaryAsync(
            table,
            new[] { "Id", "Value" },
            new List<object[]> { new object[] { 1UL, "hello" }, badRow },
            new InsertOptions
            {
                QueryPlacement = InsertQueryPlacement.Url,
                Compressor = null,
                ColumnTypes = ColumnTypes,
            }));

        Assert.That(ex.Row, Is.EqualTo(badRow));
    }

    /// <summary>
    /// <see cref="RowBinaryFormat.RowBinaryWithDefaults"/> writes rows through a second serializer path
    /// of its own, which frames the body the same way and so must drop the prologue in URL mode too.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_WithQueryInUrlAndRowBinaryWithDefaults_RoundTripsThroughARealServer()
    {
        var tableName = await CreateTableAsync();
        client.RegisterBinaryInsertType<SimplePoco>();

        await client.InsertBinaryAsync(
            tableName,
            new[] { new SimplePoco { Id = 1UL, Value = "hello" } },
            new InsertOptions
            {
                QueryPlacement = InsertQueryPlacement.Url,
                Format = RowBinaryFormat.RowBinaryWithDefaults,
                ColumnTypes = ColumnTypes,
            });

        Assert.That(await ReadBackAsync(tableName), Is.EqualTo(new[] { (1UL, "hello") }));
    }

    private async Task<List<(ulong Id, string Value)>> ReadBackAsync(string tableName)
    {
        using var reader = await client.ExecuteReaderAsync($"SELECT Id, Value FROM {tableName} ORDER BY Id");
        var readBack = new List<(ulong Id, string Value)>();
        while (reader.Read())
            readBack.Add((reader.GetFieldValue<ulong>(0), reader.GetString(1)));
        return readBack;
    }
}
