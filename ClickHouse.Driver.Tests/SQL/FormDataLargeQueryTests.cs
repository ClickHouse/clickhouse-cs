using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// SQL text larger than the server's default form-field limit (<c>http_max_field_value_size</c>, 128 KiB)
/// sent with <c>UseFormDataParameters=true</c>.
/// </summary>
public class FormDataLargeQueryTests : AbstractConnectionTestFixture
{
    private const int FormFieldLimit = 128 * 1024;

    private readonly ClickHouseClient formDataClient = TestUtilities.GetTestClickHouseClient(useFormDataParameters: true);

    // About 200 KB of SQL: over the form-field limit, under the default max_query_size of 256 KiB.
    // The padding moves the field boundary to each byte position of a 4-byte UTF-8 character.
    [RequiredFeature(Feature.ParamsInMultipartFormData)]
    [TestCase("x", 0)]
    [TestCase("😀", 0)]
    [TestCase("😀", 1)]
    [TestCase("😀", 2)]
    [TestCase("😀", 3)]
    public async Task ExecuteReaderAsync_FormDataWithSqlOverFieldLimit_SendsValidFieldsAndReturnsLiteralUnchanged(string unit, int padding)
    {
        var payload = new string('x', padding) + string.Concat(Enumerable.Repeat(unit, 200_000 / Encoding.UTF8.GetByteCount(unit)));
        var sql = $"SELECT {{p:Int32}}, '{payload}'";
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("p", 42);

        var capture = new QueryFieldCaptureHandler();
        using var httpClient = new HttpClient(capture);
        using var client = new ClickHouseClient(new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings(useFormDataParameters: true))
        {
            HttpClient = httpClient,
        });

        using var reader = await client.ExecuteReaderAsync(sql, parameters);
        var row = reader.GetEnsureSingleRow();

        var strictUtf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        Assert.Multiple(() =>
        {
            Assert.That(row[0], Is.EqualTo(42));
            Assert.That(row[1], Is.EqualTo(payload));
            Assert.That(capture.QueryFields.Select(field => field.Length), Has.All.LessThanOrEqualTo(FormFieldLimit));
            Assert.That(string.Concat(capture.QueryFields.Select(strictUtf8.GetString)), Is.EqualTo(sql));
        });
    }

    [Test]
    public async Task ExecuteNonQueryAsync_FormDataInsertWithInlineRowsOverFieldLimit_InsertsAllRows()
    {
        const int rows = 20_000;
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id UInt32, s String) ENGINE = Memory");

        // About 760 KiB of inline rows, which spans several form fields.
        var sql = new StringBuilder($"INSERT INTO {table} FORMAT JSONEachRow\n");
        for (var i = 0; i < rows; i++)
        {
            sql.Append("{\"id\":").Append(i).Append(",\"s\":\"row-").Append(i).Append("-0123456789\"}\n");
        }

        await formDataClient.ExecuteNonQueryAsync(sql.ToString());

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT count(), sum(id), countIf(s = concat('row-', toString(id), '-0123456789')) FROM {table}");
        var row = reader.GetEnsureSingleRow();

        Assert.That(row[0], Is.EqualTo(rows));
        Assert.That(row[1], Is.EqualTo((ulong)rows * (rows - 1) / 2));
        Assert.That(row[2], Is.EqualTo(rows));
    }

    [OneTimeTearDown]
    public void DisposeFormDataClient() => formDataClient.Dispose();

    /// <summary>
    /// Records the raw bytes of each <c>query</c> form field, then forwards the request to the server.
    /// </summary>
    private sealed class QueryFieldCaptureHandler : DelegatingHandler
    {
        public QueryFieldCaptureHandler()
            : base(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate })
        {
        }

        public List<byte[]> QueryFields { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Content is MultipartFormDataContent form)
            {
                foreach (var field in form.Where(part => part.Headers.ContentDisposition?.Name?.Trim('"') == "query"))
                {
                    QueryFields.Add(await field.ReadAsByteArrayAsync(cancellationToken));
                }
            }

            return await base.SendAsync(request, cancellationToken);
        }
    }
}
