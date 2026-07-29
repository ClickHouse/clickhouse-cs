using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Tests that <c>ClickHouseClient.HandleError</c> inspects the <c>X-ClickHouse-Exception-Code</c>
/// response header. Some ClickHouse server versions (23.x-24.8) can return a success HTTP status
/// while signalling a processing error (e.g. a truncated insert stream or a timeout) only through
/// that header; the client must surface it as an error instead of reporting success (silent data
/// loss). Uses a fake HTTP handler, so no ClickHouse server is required.
/// </summary>
[TestFixture]
public class HandleErrorExceptionCodeHeaderTests
{
    private const string ExceptionCodeHeader = "X-ClickHouse-Exception-Code";

    private static ClickHouseClient CreateClient(HttpStatusCode status, string exceptionCode, string body, bool asTrailer = false)
    {
        var handler = new TrackingHandler(_ =>
        {
            var response = new HttpResponseMessage(status)
            {
                Content = new StringContent(body ?? string.Empty),
            };
            if (exceptionCode != null)
            {
                // The server sets the code as a leading header before the response starts, or as a
                // trailer when the error is discovered after the status line was already sent.
                var headers = asTrailer ? response.TrailingHeaders : response.Headers;
                headers.TryAddWithoutValidation(ExceptionCodeHeader, exceptionCode);
            }

            return response;
        });
        var httpClient = new HttpClient(handler);
        return new ClickHouseClient(new ClickHouseClientSettings { HttpClient = httpClient });
    }

    [TestCase("159", 159)]
    [TestCase("241", 241)]
    public void HandleError_When200WithNonZeroExceptionCodeHeader_ThrowsWithHeaderErrorCode(string headerValue, int expectedCode)
    {
        using var client = CreateClient(HttpStatusCode.OK, headerValue, body: string.Empty);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)"));

        Assert.That(ex.ErrorCode, Is.EqualTo(expectedCode));
    }

    [Test]
    public void HandleError_When200WithExceptionCodeHeaderOnStreamInsert_ThrowsClickHouseServerException()
    {
        // The stream/bulk-insert path (PostStreamAsync) also funnels through HandleError, so it is
        // exposed to the same silent-failure bug as the query path.
        using var client = CreateClient(HttpStatusCode.OK, exceptionCode: "159", body: string.Empty);
        using var data = new MemoryStream(new byte[] { 1, 2, 3 });

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.InsertRawStreamAsync("t", data, "RowBinary", useCompression: false));

        Assert.That(ex.ErrorCode, Is.EqualTo(159));
    }

    [Test]
    public void HandleError_When200WithExceptionCodeTrailer_ThrowsClickHouseServerException()
    {
        // When the server discovers the error only after the 200 status line has been sent it can
        // report the code as an HTTP trailer rather than a leading header. The stream/insert path
        // reads the whole response, so the trailer is available and must be surfaced too.
        using var client = CreateClient(HttpStatusCode.OK, exceptionCode: "159", body: string.Empty, asTrailer: true);
        using var data = new MemoryStream(new byte[] { 1, 2, 3 });

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.InsertRawStreamAsync("t", data, "RowBinary", useCompression: false));

        Assert.That(ex.ErrorCode, Is.EqualTo(159));
    }

    [Test]
    public void HandleError_When200WithNonZeroCodeAndBodyWithoutCodePrefix_UsesHeaderErrorCode()
    {
        // The body has no parseable "Code: N" prefix, so the authoritative header code must be used
        // rather than falling back to -1.
        const string serverMessage = "processing timed out";
        using var client = CreateClient(HttpStatusCode.OK, exceptionCode: "159", body: serverMessage);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(159));
            Assert.That(ex.Message, Does.Contain("processing timed out"));
        });
    }

    [Test]
    public void HandleError_When200WithExceptionCodeHeaderAndBody_PreservesServerMessage()
    {
        const string serverMessage = "Code: 159. DB::Exception: Timeout exceeded";
        using var client = CreateClient(HttpStatusCode.OK, exceptionCode: "159", body: serverMessage);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(159));
            Assert.That(ex.Message, Does.Contain("Timeout exceeded"));
        });
    }

    [Test]
    public void HandleError_When200WithExceptionCodeHeaderAndEmptyBody_ThrowsWithNonEmptyMessage()
    {
        // An empty body must not regress into a blank exception message (cf. issue #440); the
        // synthesized message carries the header-provided error code.
        using var client = CreateClient(HttpStatusCode.OK, exceptionCode: "159", body: string.Empty);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(159));
            Assert.That(ex.Message, Is.Not.Empty);
            Assert.That(ex.Message, Does.Contain("159"));
        });
    }

    // Contrast: a success response carrying no error signal (header absent, or the "0" that
    // ClickHouse reports on success) must NOT be turned into an error. Proves the fix is targeted.
    [TestCase(null)]
    [TestCase("0")]
    public async Task HandleError_When200WithoutErrorSignal_DoesNotThrow(string headerValue)
    {
        using var client = CreateClient(HttpStatusCode.OK, headerValue, body: string.Empty);

        var affected = await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.That(affected, Is.EqualTo(0));
    }

    [Test]
    public void HandleError_WhenNonSuccessStatus_StillThrowsFromBody()
    {
        // Contrast: the pre-existing non-success error path is unchanged by the success-header check.
        const string serverMessage = "Code: 62. DB::Exception: Syntax error";
        using var client = CreateClient(HttpStatusCode.InternalServerError, exceptionCode: null, body: serverMessage);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT bad"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(62));
            Assert.That(ex.Message, Does.Contain("Syntax error"));
        });
    }
}
