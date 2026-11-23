using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;
using ClickHouse.Driver.Tests.Logging;
using ClickHouse.Driver.Tests.Utilities;

namespace ClickHouse.Driver.Tests.Authentication;

/// <summary>
/// How the HTTP client uses <see cref="ClickHouseClientSettings.CredentialsProvider"/>. A fake handler
/// answers each request, so that a rejection can be set up for chosen credentials. The rejection has the
/// shape that the server sends for wrong credentials (HTTP 403, <c>X-ClickHouse-Exception-Code: 516</c>,
/// a <c>Code: 516</c> body); <see cref="CredentialsProviderIntegrationTests"/> gets it from a real server.
/// </summary>
public class CredentialsProviderHttpTests
{
    private const string RejectedToken = "rejected-token";
    private const string AuthenticationFailedBody =
        "Code: 516. DB::Exception: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)";

    /// <summary>
    /// A provider that records the reason of each call and returns the next credentials of a list (the last
    /// one again when the list is used up).
    /// </summary>
    private sealed class ScriptedProvider
    {
        private readonly ClickHouseCredentials[] script;
        private readonly ConcurrentQueue<ClickHouseCredentialsRefreshReason> reasons = new();

        public ScriptedProvider(params ClickHouseCredentials[] script) => this.script = script;

        public IReadOnlyList<ClickHouseCredentialsRefreshReason> Reasons => reasons.ToList();

        public ValueTask<ClickHouseCredentials> InvokeAsync(ClickHouseCredentialsRequest request, CancellationToken token)
        {
            reasons.Enqueue(request.Reason);
            return new ValueTask<ClickHouseCredentials>(script[Math.Min(reasons.Count, script.Length) - 1]);
        }
    }

    private static HttpResponseMessage Respond(HttpRequestMessage request, bool rejectionHeader = true, List<ResponseDisposalTrackingHandler.DisposalTrackingContent> rejections = null)
    {
        var authorization = request.Headers.Authorization;
        var rejected = authorization?.Parameter == RejectedToken ||
            authorization?.Parameter == Convert.ToBase64String(Encoding.UTF8.GetBytes($"user:{RejectedToken}"));
        if (!rejected)
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };

        var content = new ResponseDisposalTrackingHandler.DisposalTrackingContent(new MemoryStream(Encoding.UTF8.GetBytes(AuthenticationFailedBody)));
        rejections?.Add(content);
        var response = new HttpResponseMessage(HttpStatusCode.Forbidden) { Content = content };
        if (rejectionHeader)
            response.Headers.Add("X-ClickHouse-Exception-Code", "516");
        return response;
    }

    private static (ClickHouseClient Client, TrackingHandler Handler) CreateClient(
        ClickHouseCredentialsProvider provider,
        Func<HttpRequestMessage, HttpResponseMessage> respond = null,
        string bearerToken = null,
        bool useFormDataParameters = false)
    {
        var handler = new TrackingHandler(respond ?? (r => Respond(r)));
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Username = "static-user",
            Password = "static-password",
            BearerToken = bearerToken,
            CredentialsProvider = provider,
            UseFormDataParameters = useFormDataParameters,
            HttpClient = new HttpClient(handler),
        };
        return (new ClickHouseClient(settings), handler);
    }

    private static string[] Tokens(TrackingHandler handler) => handler.Requests.Select(r => r.Headers.Authorization?.Parameter).ToArray();

    [TestCase(false, TestName = "ExecuteNonQueryAsync_QueryParametersAndProviderTokenRejected_SendsOnceMoreWithNewToken")]
    [TestCase(true, TestName = "ExecuteNonQueryAsync_FormDataParametersAndProviderTokenRejected_SendsOnceMoreWithNewToken")]
    public async Task ExecuteNonQueryAsync_ProviderTokenRejected_SendsOnceMoreWithNewToken(bool useFormDataParameters)
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken), ClickHouseCredentials.CreateBearer("new-token"));
        var rejections = new List<ResponseDisposalTrackingHandler.DisposalTrackingContent>();
        var (client, handler) = CreateClient(provider.InvokeAsync, r => Respond(r, rejections: rejections), useFormDataParameters: useFormDataParameters);
        using var _ = client;
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("id", 42UL);

        await client.ExecuteNonQueryAsync("SELECT {id:UInt64}", parameters, new QueryOptions { QueryId = "rotation-query" });

        Assert.Multiple(() =>
        {
            Assert.That(Tokens(handler), Is.EqualTo(new[] { RejectedToken, "new-token" }));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
            Assert.That(handler.Requests.Select(r => r.Headers.GetValues("User-Agent").Count()), Is.All.GreaterThan(0));
            Assert.That(handler.Requests[1].RequestUri, Is.EqualTo(handler.Requests[0].RequestUri));
            Assert.That(handler.Requests[1].RequestUri.Query, Does.Contain("query_id=rotation-query"));
            if (!useFormDataParameters)
                Assert.That(handler.Requests[1].RequestUri.Query, Does.Contain("param_id=42"));
            Assert.That(rejections.Single().IsDisposed, Is.True);
        });
    }

    [Test]
    public async Task ExecuteNonQueryAsync_RejectionCodeOnlyInTheBody_SendsOnceMoreWithNewCredentials()
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBasic("user", RejectedToken), ClickHouseCredentials.CreateBasic("user", "rotated"));
        var (client, handler) = CreateClient(provider.InvokeAsync, r => Respond(r, rejectionHeader: false));
        using var _ = client;

        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(handler.Requests.Select(r => r.Headers.Authorization?.Scheme), Is.All.EqualTo("Basic"));
            Assert.That(Tokens(handler)[1], Is.EqualTo(Convert.ToBase64String(Encoding.UTF8.GetBytes("user:rotated"))));
            Assert.That(provider.Reasons[1], Is.EqualTo(ClickHouseCredentialsRefreshReason.Rejected));
        });
    }

    [Test]
    public void ExecuteNonQueryAsync_NewTokenRejectedToo_ThrowsAfterOneRetry()
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken));
        var (client, handler) = CreateClient(provider.InvokeAsync);
        using var _ = client;

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));
        Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(516));
            Assert.That(ex.Message, Does.Contain("AUTHENTICATION_FAILED"));
            Assert.That(handler.RequestCount, Is.EqualTo(4));
            // The second rejection counts too: the next query asks for new credentials again.
            Assert.That(provider.Reasons, Is.EqualTo(new[]
            {
                ClickHouseCredentialsRefreshReason.Initial,
                ClickHouseCredentialsRefreshReason.Rejected,
                ClickHouseCredentialsRefreshReason.Rejected,
                ClickHouseCredentialsRefreshReason.Rejected,
            }));
        });
    }

    [Test]
    public void ExecuteNonQueryAsync_OtherServerErrorWithProviderToken_ThrowsWithoutRetry()
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer("token"));
        var (client, handler) = CreateClient(
            provider.InvokeAsync,
            _ => new HttpResponseMessage(HttpStatusCode.NotFound)
            {
                Content = new StringContent("Code: 60. DB::Exception: Unknown table expression identifier 'missing'. (UNKNOWN_TABLE)"),
                Headers = { { "X-ClickHouse-Exception-Code", "60" } },
            });
        using var _ = client;

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT * FROM missing"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(60));
            Assert.That(ex.Message, Does.Contain("UNKNOWN_TABLE"));
            Assert.That(handler.RequestCount, Is.EqualTo(1));
            Assert.That(provider.Reasons, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void ExecuteNonQueryAsync_StaticBearerTokenRejected_ThrowsWithoutRetry()
    {
        var (client, handler) = CreateClient(provider: null, bearerToken: RejectedToken);
        using var _ = client;

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(516));
            Assert.That(handler.RequestCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task PingAsync_ProviderTokenRejected_SendsOnceMoreWithNewToken()
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken), ClickHouseCredentials.CreateBearer("new-token"));
        var (client, handler) = CreateClient(provider.InvokeAsync);
        using var _ = client;

        var alive = await client.PingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(alive, Is.True);
            Assert.That(Tokens(handler), Is.EqualTo(new[] { RejectedToken, "new-token" }));
        });
    }

    [Test]
    public async Task PingAsync_NewTokenRejectedToo_ReturnsFalseAfterOneRetry()
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken));
        var (client, handler) = CreateClient(provider.InvokeAsync);
        using var _ = client;

        var alive = await client.PingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(alive, Is.False);
            Assert.That(handler.RequestCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ExecuteNonQueryAsync_ProviderTokenRejectedWithDebugLogs_LogsNoToken()
    {
        var loggerFactory = new CapturingLoggerFactory();
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken), ClickHouseCredentials.CreateBearer("new-token"));
        var handler = new TrackingHandler(r => Respond(r));
        using var client = new ClickHouseClient(new ClickHouseClientSettings
        {
            Host = "localhost",
            CredentialsProvider = provider.InvokeAsync,
            HttpClient = new HttpClient(handler),
            // Debug level is on by default in the capturing factory. EnableDebugMode is not set: it turns on
            // a process-wide network trace that other tests check.
            LoggerFactory = loggerFactory,
        });

        await client.ExecuteNonQueryAsync("SELECT 1");

        var messages = loggerFactory.Loggers.Values.SelectMany(l => l.Logs).Select(e => e.Message + e.Exception).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(messages, Has.Some.Contains("rejected the credentials"));
            Assert.That(messages, Has.None.Contains(RejectedToken).And.None.Contains("new-token"));
        });
    }

    private static IEnumerable<TestCaseData> StreamedRequests()
    {
        yield return new TestCaseData((Func<ClickHouseClient, Stream, Task>)((c, s) => c.InsertRawStreamAsync("t", s, "CSV")))
            .SetName("InsertRawStreamAsync_{m}");
        yield return new TestCaseData((Func<ClickHouseClient, Stream, Task>)((c, s) => c.PostStreamAsync("INSERT INTO t FORMAT CSV", s, false, CancellationToken.None)))
            .SetName("PostStreamAsync_Stream_{m}");
        yield return new TestCaseData((Func<ClickHouseClient, Stream, Task>)((c, s) => c.PostStreamAsync("INSERT INTO t FORMAT CSV", (body, ct) => s.CopyToAsync(body, ct), false, CancellationToken.None)))
            .SetName("PostStreamAsync_Callback_{m}");
    }

    [TestCaseSource(nameof(StreamedRequests))]
    public async Task ProviderTokenRejected_DropsTokenWithoutRetry(Func<ClickHouseClient, Stream, Task> send)
    {
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1);
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken, expiresAt), ClickHouseCredentials.CreateBearer("new-token", expiresAt));
        var (client, handler) = CreateClient(provider.InvokeAsync);
        using var _ = client;

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => send(client, new MemoryStream(Encoding.UTF8.GetBytes("1\n"))));
        var requestsAfterRejection = handler.RequestCount;
        // The rejected token was cached (it has an expiry); the next request must not use it again.
        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(516));
            Assert.That(requestsAfterRejection, Is.EqualTo(1));
            Assert.That(Tokens(handler), Is.EqualTo(new[] { RejectedToken, "new-token" }));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
        });
    }

    private sealed class DisposeCountingStream : MemoryStream
    {
        public DisposeCountingStream() : base(Encoding.UTF8.GetBytes("1\n")) { }

        public int DisposeCount { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                DisposeCount++;
            base.Dispose(disposing);
        }
    }

    [TestCase(true, TestName = "InsertRawStreamAsync_ProviderThrows_DisposesStreamOnceAndThrows")]
    [TestCase(false, TestName = "PostStreamAsync_ProviderThrows_DisposesStreamOnceAndThrows")]
    public void StreamedRequest_ProviderThrows_DisposesStreamOnceAndThrows(bool insertRawStream)
    {
        var failure = new InvalidOperationException("identity provider down");
        var (client, handler) = CreateClient((_, _) => throw failure);
        using var _ = client;
        var stream = new DisposeCountingStream();

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(() => insertRawStream
            ? client.InsertRawStreamAsync("t", stream, "CSV")
            : client.PostStreamAsync("INSERT INTO t FORMAT CSV", stream, false, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(failure));
            Assert.That(stream.DisposeCount, Is.EqualTo(1));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public void ExecuteNonQueryAsync_ProviderThrows_ThrowsProviderExceptionWithoutRequest()
    {
        var failure = new InvalidOperationException("identity provider down");
        var (client, handler) = CreateClient((_, _) => throw failure);
        using var _ = client;

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(failure));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    private static IEnumerable<TestCaseData> PrecedenceCases()
    {
        var provided = ClickHouseCredentials.CreateBearer("provider-token");
        yield return new TestCaseData("query-token", provided, "settings-token", "Bearer query-token")
            .SetName("Authorization_PerQueryToken_HasPrecedenceOverProvider");
        yield return new TestCaseData(string.Empty, provided, "settings-token", "Bearer provider-token")
            .SetName("Authorization_EmptyPerQueryToken_UsesProvider");
        yield return new TestCaseData(null, null, "settings-token", "Bearer settings-token")
            .SetName("Authorization_ProviderReturnsNull_UsesSettingsBearerToken");
        yield return new TestCaseData(null, null, null, "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("static-user:static-password")))
            .SetName("Authorization_ProviderReturnsNullAndNoBearerToken_UsesUsernameAndPassword");
        yield return new TestCaseData(null, ClickHouseCredentials.CreateBasic("alice", "pw"), "settings-token", "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("alice:pw")))
            .SetName("Authorization_ProviderReturnsBasic_HasPrecedenceOverSettingsBearerToken");
    }

    [TestCaseSource(nameof(PrecedenceCases))]
    public async Task Authorization_Precedence(string perQueryToken, ClickHouseCredentials provided, string settingsToken, string expected)
    {
        var (client, handler) = CreateClient(
            (_, _) => new ValueTask<ClickHouseCredentials>(provided),
            bearerToken: settingsToken);
        using var _ = client;

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { BearerToken = perQueryToken });

        Assert.That(handler.Requests.Single().Headers.Authorization?.ToString(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ExecuteNonQueryAsync_EmptyPerQueryTokenWithoutProvider_UsesSettingsBearerToken()
    {
        var (client, handler) = CreateClient(provider: null, bearerToken: "settings-token");
        using var _ = client;

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { BearerToken = string.Empty });

        Assert.That(handler.Requests.Single().Headers.Authorization?.ToString(), Is.EqualTo("Bearer settings-token"));
    }

    [Test]
    public async Task Command_EmptyBearerToken_UsesProvider()
    {
        var (client, handler) = CreateClient((_, _) => new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBearer("provider-token")));
        using var _ = client;
        using var connection = client.CreateConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.BearerToken = string.Empty;

        await command.ExecuteNonQueryAsync();

        Assert.That(handler.Requests.Single().Headers.Authorization?.ToString(), Is.EqualTo("Bearer provider-token"));
    }

    [Test]
    public async Task Connection_SettingsWithProvider_UsesProviderToken()
    {
        var handler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("25.10\tUTC") });
        using var httpClient = new HttpClient(handler);
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            HttpClient = httpClient,
            CredentialsProvider = (_, _) => new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBearer("provider-token")),
        };
        using var connection = new ClickHouseConnection(settings);

        await connection.OpenAsync();
        await connection.ExecuteStatementAsync("SELECT 1");

        Assert.That(handler.Requests.Select(r => r.Headers.Authorization?.ToString()), Is.All.EqualTo("Bearer provider-token"));
    }

    [Test]
    public async Task Dispose_ProviderCallRuns_CancelsTheProviderToken()
    {
        var tokenCancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var providerStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (client, _) = CreateClient(async (_, token) =>
        {
            providerStarted.TrySetResult(true);
            try
            {
                await Task.Delay(System.Threading.Timeout.Infinite, token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Not a token.Register callback: the cancelled delay can resume this method inside Cancel(), before
                // a callback registered earlier runs, and leaving a using scope would then remove that callback.
                tokenCancelled.TrySetResult(true);
                throw;
            }

            return null;
        });
        var query = client.ExecuteNonQueryAsync("SELECT 1");

        await providerStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        client.Dispose();

        Assert.That(await tokenCancelled.Task.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        Assert.ThrowsAsync<TaskCanceledException>(() => query);
    }

    [Test]
    public void Settings_CopyConstructor_KeepsProvider()
    {
        ClickHouseCredentialsProvider provider = (_, _) => default;
        var original = new ClickHouseClientSettings { CredentialsProvider = provider };

        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.CredentialsProvider, Is.SameAs(provider));
    }

    [Test]
    public void Settings_Equals_ComparesProviderByReference()
    {
        var instance = new ScriptedProvider();
        ClickHouseCredentialsProvider provider = instance.InvokeAsync;
        ClickHouseCredentialsProvider sameMethodOtherDelegate = instance.InvokeAsync;

        var a = new ClickHouseClientSettings { CredentialsProvider = provider };
        var b = new ClickHouseClientSettings { CredentialsProvider = provider };
        var c = new ClickHouseClientSettings { CredentialsProvider = sameMethodOtherDelegate };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
            Assert.That(a, Is.Not.EqualTo(c));
        });
    }
}
