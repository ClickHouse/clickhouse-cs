using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Web;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Tests.Utilities;
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

    private class DatePoco
    {
        public ulong Id { get; set; }
        public DateTime Value { get; set; }
    }

    private static readonly IReadOnlyDictionary<string, string> ColumnTypes = new Dictionary<string, string>
    {
        ["Id"] = "UInt64",
        ["Value"] = "String",
    };

    private static readonly IReadOnlyDictionary<string, string> DateColumnTypes = new Dictionary<string, string>
    {
        ["Id"] = "UInt64",
        ["Value"] = "DateTime",
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

    [Test]
    public void InsertBinaryAsync_WithAnUndefinedQueryPlacement_ThrowsAndSendsNothing()
    {
        var requestsSent = 0;
        using var httpClient = MockHttpClientHelper.Create((_, _) =>
        {
            requestsSent++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) });
        });

        using var stubbedClient = new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });
        var table = TestUtilities.CreateTableName("query_placement_undefined");
        var options = new InsertOptions
        {
            QueryPlacement = (InsertQueryPlacement)42,
            ColumnTypes = ColumnTypes,
        };

        var ex = Assert.CatchAsync<ArgumentOutOfRangeException>(() => stubbedClient.InsertBinaryAsync(
            table,
            new[] { "Id", "Value" },
            new List<object[]> { new object[] { 1UL, "hello" } },
            options));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("QueryPlacement"));
            Assert.That(requestsSent, Is.Zero, "no request may be sent for an unusable placement");
        });
    }

    /// <summary>
    /// URL mode must leave the body to RowBinary data alone: even a leading newline would corrupt the
    /// first row. Both serializers are checked compressed and uncompressed.
    /// </summary>
    [Test]
    public async Task InsertBinaryAsync_WithQueryInUrl_PutsStatementInUrlAndRowsAloneInBody(
        [Values(false, true)] bool poco,
        [Values(false, true)] bool compressed)
    {
        var compressor = compressed ? ZstdCompressor.Default : null;
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
            QueryPlacement = InsertQueryPlacement.Url,
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

        var expectedStatement =
            $"INSERT INTO `{TestUtilities.TestDatabase}`.`{TestUtilities.BareTableName(table)}` (`Id`, `Value`) FORMAT RowBinary";
        var urlQuery = HttpUtility.ParseQueryString(sentUri.Query).Get("query");
        var body = Decoded(sentBody, compressor);

        Assert.Multiple(() =>
        {
            Assert.That(urlQuery, Is.EqualTo(expectedStatement));
            Assert.That(body, Is.EqualTo(ExpectedRows), "the body must carry the rows alone");
        });
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

#if !NET10_0_OR_GREATER
    [Test]
    public void InsertBinaryAsync_WithQueryInUrlBeyondTheRuntimeLimit_ThrowsActionableException()
    {
        var requestsSent = 0;
        using var httpClient = MockHttpClientHelper.Create((_, _) =>
        {
            requestsSent++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) });
        });

        using var stubbedClient = new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });
        var table = TestUtilities.CreateTableName("query_placement_too_long") + new string('x', 66_000);

        var ex = Assert.CatchAsync<InvalidOperationException>(() => stubbedClient.InsertBinaryAsync(
            table,
            new[] { "Id", "Value" },
            new List<object[]> { new object[] { 1UL, "hello" } },
            new InsertOptions
            {
                QueryPlacement = InsertQueryPlacement.Url,
                Compressor = null,
                ColumnTypes = ColumnTypes,
            }));

        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.TypeOf<UriFormatException>());
            Assert.That(ex.Message, Does.Contain("InsertQueryPlacement.Body"));
            Assert.That(requestsSent, Is.Zero);
        });
    }
#endif

    [TestCase(false, true)]
    [TestCase(true, false)]
    public async Task InsertBinaryAsync_WithQueryInUrl_RoundTripsThroughARealServer(
        bool poco,
        bool compressed)
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
    /// Both serializers must classify a row-value failure as serialization after the query prologue
    /// moves out of the body.
    /// </summary>
    [Test]
    public void InsertBinaryAsync_WithQueryInUrlWhenARowCannotBeSerialized_ThrowsSerializationExceptionWithTheRow(
        [Values(false, true)] bool poco)
    {
        var badValue = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var table = TestUtilities.CreateTableName("query_placement_bad_row");
        using var httpClient = MockHttpClientHelper.Create(async (request, _) =>
        {
            await request.Content.ReadAsByteArrayAsync();
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };
        });
        using var stubbedClient = new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });

        var options = new InsertOptions
        {
            QueryPlacement = InsertQueryPlacement.Url,
            Compressor = null,
            ColumnTypes = DateColumnTypes,
        };

        ClickHouseBulkCopySerializationException ex;
        if (poco)
        {
            stubbedClient.RegisterBinaryInsertType<DatePoco>();
            ex = Assert.CatchAsync<ClickHouseBulkCopySerializationException>(() => stubbedClient.InsertBinaryAsync(
                table,
                new[] { new DatePoco { Id = 1UL, Value = badValue } },
                options));
        }
        else
        {
            ex = Assert.CatchAsync<ClickHouseBulkCopySerializationException>(() => stubbedClient.InsertBinaryAsync(
                table,
                new[] { "Id", "Value" },
                new List<object[]> { new object[] { 1UL, badValue } },
                options));
        }

        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(ex.Row[1], Is.EqualTo(badValue));
        });
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
