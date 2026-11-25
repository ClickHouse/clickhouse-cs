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
/// a <c>Code: 516</c> body; HTTP 401 and code 194 for the user <c>default</c> with HTTP Basic);
/// <see cref="CredentialsProviderIntegrationTests"/> gets both from a real server.
/// </summary>
public class CredentialsProviderHttpTests
{
    private const string RejectedToken = "rejected-token";
    private const string AuthenticationFailedBody =
        "Code: 516. DB::Exception: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)";
    private const string RequiredPasswordBody =
        "Code: 194. DB::Exception: default: Authentication failed: password is incorrect, or there is no user with such name. (REQUIRED_PASSWORD)";

    /// <summary>
    /// A provider that records the reason of each call and returns the next credentials of a list (the last
    /// one again when the list is used up).
    /// </summary>
    private sealed class ScriptedProvider : IClickHouseCredentialsProvider
    {
        private readonly ClickHouseCredentials[] script;
        private readonly ConcurrentQueue<ClickHouseCredentialsRefreshReason> reasons = new();

        public ScriptedProvider(params ClickHouseCredentials[] script) => this.script = script;

        public IReadOnlyList<ClickHouseCredentialsRefreshReason> Reasons => reasons.ToList();

        public ValueTask<ClickHouseCredentials> GetCredentialsAsync(ClickHouseCredentialsRequest request, CancellationToken token)
        {
            reasons.Enqueue(request.Reason);
            return new ValueTask<ClickHouseCredentials>(script[Math.Min(reasons.Count, script.Length) - 1]);
        }
    }

    /// <summary>
    /// Rejects a bearer token <see cref="RejectedToken"/> and HTTP Basic credentials whose password is
    /// <see cref="RejectedToken"/>, with <paramref name="code"/> 516 (HTTP 403) or 194 (HTTP 401).
    /// </summary>
    private static HttpResponseMessage Respond(HttpRequestMessage request, bool rejectionHeader = true, List<ResponseDisposalTrackingHandler.DisposalTrackingContent> rejections = null, int code = 516)
    {
        var authorization = request.Headers.Authorization;
        var rejected = authorization?.Scheme == "Bearer"
            ? authorization.Parameter == RejectedToken
            : authorization != null && Encoding.UTF8.GetString(Convert.FromBase64String(authorization.Parameter)).EndsWith(":" + RejectedToken, StringComparison.Ordinal);
        if (!rejected)
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };

        var body = code == 194 ? RequiredPasswordBody : AuthenticationFailedBody;
        var content = new ResponseDisposalTrackingHandler.DisposalTrackingContent(new MemoryStream(Encoding.UTF8.GetBytes(body)));
        rejections?.Add(content);
        var response = new HttpResponseMessage(code == 194 ? HttpStatusCode.Unauthorized : HttpStatusCode.Forbidden) { Content = content };
        if (rejectionHeader)
            response.Headers.Add("X-ClickHouse-Exception-Code", code.ToString(System.Globalization.CultureInfo.InvariantCulture));
        return response;
    }

    private static (ClickHouseClient Client, TrackingHandler Handler) CreateClient(
        IClickHouseCredentialsProvider provider,
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

    /// <summary>Counts the parameters it is asked to format, and leaves the formatting to the client.</summary>
    private sealed class CountingFormatter : IParameterFormatter
    {
        public int Calls { get; private set; }

        public string Format(object value, string typeName, string parameterName)
        {
            Calls++;
            return null;
        }
    }

    [TestCase(false, TestName = "ExecuteNonQueryAsync_QueryParametersAndProviderTokenRejected_SendsOneRequestAndNextCallGetsNewToken")]
    [TestCase(true, TestName = "ExecuteNonQueryAsync_FormDataParametersAndProviderTokenRejected_SendsOneRequestAndNextCallGetsNewToken")]
    public async Task ExecuteNonQueryAsync_ProviderTokenRejected_SendsOneRequestAndNextCallGetsNewToken(bool useFormDataParameters)
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken), ClickHouseCredentials.CreateBearer("new-token"));
        var rejections = new List<ResponseDisposalTrackingHandler.DisposalTrackingContent>();
        var (client, handler) = CreateClient(provider, r => Respond(r, rejections: rejections), useFormDataParameters: useFormDataParameters);
        using var _ = client;
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("id", 42UL);
        var formatter = new CountingFormatter();

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT {id:UInt64}", parameters, new QueryOptions { QueryId = "rotation-query", ParameterFormatter = formatter }));
        var requestsAfterRejection = handler.RequestCount;
        await client.ExecuteNonQueryAsync("SELECT {id:UInt64}", parameters);

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(516));
            Assert.That(ex.Message, Does.Contain(AuthenticationFailedBody));
            Assert.That(requestsAfterRejection, Is.EqualTo(1));
            // The rejected request was built once: its parameter was formatted once.
            Assert.That(formatter.Calls, Is.EqualTo(1));
            Assert.That(handler.Requests[0].RequestUri.Query, Does.Contain("query_id=rotation-query"));
            if (!useFormDataParameters)
                Assert.That(handler.Requests[0].RequestUri.Query, Does.Contain("param_id=42"));
            Assert.That(Tokens(handler), Is.EqualTo(new[] { RejectedToken, "new-token" }));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
            Assert.That(rejections.Single().IsDisposed, Is.True);
        });
    }

    private static IEnumerable<TestCaseData> Rejections()
    {
        yield return new TestCaseData(ClickHouseCredentials.CreateBearer(RejectedToken), 516, true)
            .SetName("ExecuteNonQueryAsync_BearerTokenRejectedWith516_ThrowsAndNextCallGetsNewCredentials");
        yield return new TestCaseData(ClickHouseCredentials.CreateBasic("user", RejectedToken), 516, false)
            .SetName("ExecuteNonQueryAsync_PasswordRejectedWith516OnlyInTheBody_ThrowsAndNextCallGetsNewCredentials");
        yield return new TestCaseData(ClickHouseCredentials.CreateBasic("default", RejectedToken), 194, true)
            .SetName("ExecuteNonQueryAsync_DefaultUserPasswordRejectedWith194_ThrowsAndNextCallGetsNewCredentials");
        yield return new TestCaseData(ClickHouseCredentials.CreateBasic("default", RejectedToken), 194, false)
            .SetName("ExecuteNonQueryAsync_DefaultUserPasswordRejectedWith194OnlyInTheBody_ThrowsAndNextCallGetsNewCredentials");
    }

    [TestCaseSource(nameof(Rejections))]
    public async Task ExecuteNonQueryAsync_ServerRejectsProviderCredentials_ThrowsAndNextCallGetsNewCredentials(ClickHouseCredentials rejected, int code, bool rejectionHeader)
    {
        // Credentials without an expiry are not cached, but the next provider call must still say that the
        // server rejected the last ones: a provider that caches a password reads its store again only then.
        var provider = new ScriptedProvider(rejected, ClickHouseCredentials.CreateBasic("user", "rotated"));
        var (client, handler) = CreateClient(provider, r => Respond(r, rejectionHeader, code: code));
        using var _ = client;

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));
        var requestsAfterRejection = handler.RequestCount;
        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(code));
            Assert.That(requestsAfterRejection, Is.EqualTo(1));
            Assert.That(handler.Requests[1].Headers.Authorization?.ToString(), Is.EqualTo("Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("user:rotated"))));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
        });
    }

    [Test]
    public void ExecuteNonQueryAsync_EveryCallRejected_SendsOneRequestPerCall()
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken));
        var (client, handler) = CreateClient(provider);
        using var _ = client;

        for (var i = 0; i < 3; i++)
            Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(handler.RequestCount, Is.EqualTo(3));
            // The second rejection counts too: the third query asks for new credentials again.
            Assert.That(provider.Reasons, Is.EqualTo(new[]
            {
                ClickHouseCredentialsRefreshReason.Initial,
                ClickHouseCredentialsRefreshReason.Rejected,
                ClickHouseCredentialsRefreshReason.Rejected,
            }));
        });
    }

    [Test]
    public async Task ExecuteNonQueryAsync_OtherServerErrorWithProviderToken_ThrowsAndKeepsTheCachedToken()
    {
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1);
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer("token", expiresAt), ClickHouseCredentials.CreateBearer("other-token", expiresAt));
        var requests = 0;
        var (client, handler) = CreateClient(
            provider,
            r => ++requests == 1
                ? new HttpResponseMessage(HttpStatusCode.NotFound)
                {
                    Content = new StringContent("Code: 60. DB::Exception: Unknown table expression identifier 'missing'. (UNKNOWN_TABLE)"),
                    Headers = { { "X-ClickHouse-Exception-Code", "60" } },
                }
                : Respond(r));
        using var _ = client;

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT * FROM missing"));
        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(60));
            Assert.That(ex.Message, Does.Contain("UNKNOWN_TABLE"));
            Assert.That(Tokens(handler), Is.EqualTo(new[] { "token", "token" }));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial }));
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

    [TestCase(516, TestName = "PingAsync_ProviderTokenRejectedWith516_ReturnsFalseAndNextPingGetsNewToken")]
    [TestCase(194, TestName = "PingAsync_ProviderTokenRejectedWith194_ReturnsFalseAndNextPingGetsNewToken")]
    public async Task PingAsync_ProviderTokenRejected_ReturnsFalseAndNextPingGetsNewToken(int code)
    {
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1);
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken, expiresAt), ClickHouseCredentials.CreateBearer("new-token", expiresAt));
        var (client, handler) = CreateClient(provider, r => Respond(r, code: code));
        using var _ = client;

        var first = await client.PingAsync();
        var requestsAfterRejection = handler.RequestCount;
        // The rejected token was cached (it has an expiry); the next ping must not use it again.
        var second = await client.PingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.False);
            Assert.That(second, Is.True);
            Assert.That(requestsAfterRejection, Is.EqualTo(1));
            Assert.That(Tokens(handler), Is.EqualTo(new[] { RejectedToken, "new-token" }));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
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
            CredentialsProvider = provider,
            HttpClient = new HttpClient(handler),
            // Debug level is on by default in the capturing factory. EnableDebugMode is not set: it turns on
            // a process-wide network trace that other tests check.
            LoggerFactory = loggerFactory,
        });

        Assert.ThrowsAsync<ClickHouseServerException>(() => client.ExecuteNonQueryAsync("SELECT 1"));
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
        var rejections = new List<ResponseDisposalTrackingHandler.DisposalTrackingContent>();
        var (client, handler) = CreateClient(provider, r => Respond(r, rejections: rejections));
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
            Assert.That(rejections.Single().IsDisposed, Is.True);
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
        var (client, handler) = CreateClient(new FuncCredentialsProvider((_, _) => throw failure));
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
        var (client, handler) = CreateClient(new FuncCredentialsProvider((_, _) => throw failure));
        using var _ = client;

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(() => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(failure));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public void ExecuteNonQueryAsync_ProviderBlocksBeforeItsFirstAwait_StopsAtTheCallersCancellation()
    {
        var blockFor = TimeSpan.FromSeconds(10);
        using var release = new ManualResetEventSlim();
        var (client, handler) = CreateClient(new FuncCredentialsProvider((_, _) =>
        {
            release.Wait(blockFor);
            return new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBearer("provider-token"));
        }));
        using var _ = client;
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        try
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            Assert.CatchAsync<OperationCanceledException>(() => client.ExecuteNonQueryAsync("SELECT 1", cancellationToken: cancellation.Token));

            Assert.Multiple(() =>
            {
                Assert.That(stopwatch.Elapsed, Is.LessThan(blockFor / 2));
                Assert.That(handler.RequestCount, Is.Zero);
            });
        }
        finally
        {
            release.Set();
        }
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
            new FuncCredentialsProvider((_, _) => new ValueTask<ClickHouseCredentials>(provided)),
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
        var (client, handler) = CreateClient(new FuncCredentialsProvider((_, _) => new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBearer("provider-token"))));
        using var _ = client;
        using var connection = client.CreateConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.BearerToken = string.Empty;

        await command.ExecuteNonQueryAsync();

        Assert.That(handler.Requests.Single().Headers.Authorization?.ToString(), Is.EqualTo("Bearer provider-token"));
    }

    [Test]
    public async Task Command_ProviderTokenRejected_ThrowsAfterOneRequestAndNextCommandGetsNewToken()
    {
        var provider = new ScriptedProvider(ClickHouseCredentials.CreateBearer(RejectedToken), ClickHouseCredentials.CreateBearer("new-token"));
        var (client, handler) = CreateClient(provider);
        using var _ = client;
        using var connection = client.CreateConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(() => command.ExecuteNonQueryAsync());
        var requestsAfterRejection = handler.RequestCount;
        await command.ExecuteNonQueryAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(516));
            Assert.That(requestsAfterRejection, Is.EqualTo(1));
            Assert.That(Tokens(handler), Is.EqualTo(new[] { RejectedToken, "new-token" }));
            Assert.That(provider.Reasons, Is.EqualTo(new[] { ClickHouseCredentialsRefreshReason.Initial, ClickHouseCredentialsRefreshReason.Rejected }));
        });
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
            CredentialsProvider = new FuncCredentialsProvider((_, _) => new ValueTask<ClickHouseCredentials>(ClickHouseCredentials.CreateBearer("provider-token"))),
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
        var (client, _) = CreateClient(new FuncCredentialsProvider(async (_, token) =>
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
        }));
        var query = client.ExecuteNonQueryAsync("SELECT 1");

        await providerStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        client.Dispose();

        Assert.That(await tokenCancelled.Task.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        Assert.ThrowsAsync<TaskCanceledException>(() => query);
    }

    [Test]
    public void Settings_CopyConstructor_KeepsProvider()
    {
        var provider = new FuncCredentialsProvider((_, _) => default);
        var original = new ClickHouseClientSettings { CredentialsProvider = provider };

        var copy = new ClickHouseClientSettings(original);

        Assert.That(copy.CredentialsProvider, Is.SameAs(provider));
    }

    [Test]
    public void Settings_Equals_ComparesProviderByReference()
    {
        var provider = new EqualByValueProvider();
        var equalOtherInstance = new EqualByValueProvider();

        var a = new ClickHouseClientSettings { CredentialsProvider = provider };
        var b = new ClickHouseClientSettings { CredentialsProvider = provider };
        var c = new ClickHouseClientSettings { CredentialsProvider = equalOtherInstance };

        Assert.Multiple(() =>
        {
            Assert.That(equalOtherInstance, Is.EqualTo(provider));
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
            Assert.That(a, Is.Not.EqualTo(c));
        });
    }

    /// <summary>A provider whose instances are all equal, so that only a reference comparison tells two apart.</summary>
    private sealed class EqualByValueProvider : IClickHouseCredentialsProvider
    {
        public ValueTask<ClickHouseCredentials> GetCredentialsAsync(ClickHouseCredentialsRequest request, CancellationToken cancellationToken)
            => default;

        public override bool Equals(object obj) => obj is EqualByValueProvider;

        public override int GetHashCode() => 0;
    }
}
