using System;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tests.Utilities;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class AcceptEncodingTests
{
    private static HttpResponseMessage CreateFakeSuccessResponse()
    {
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(string.Empty),
        };
    }

    private static (ClickHouseClient client, TrackingHandler handler) CreateClient(
        bool useCompression = false, string acceptEncoding = null, string customHeaderAcceptEncoding = null)
    {
        var trackingHandler = new TrackingHandler(CreateFakeSuccessResponse());
        var httpClient = new HttpClient(trackingHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
            UseCompression = useCompression,
            AcceptEncoding = acceptEncoding,
            CustomHeaders = customHeaderAcceptEncoding is null
                ? new Dictionary<string, string>()
                : new Dictionary<string, string> { ["Accept-Encoding"] = customHeaderAcceptEncoding },
        };
        return (new ClickHouseClient(settings), trackingHandler);
    }

    /// <summary>
    /// The header and <c>enable_http_compression</c> together. They are produced by different code —
    /// <c>AddDefaultHttpHeaders</c> and <c>CreateUriBuilder</c> — each carrying its own copy of the
    /// per-query-then-client-level precedence, so a cell that asserts only one of them cannot catch the
    /// two drifting apart. Drift is silent in the worst direction: advertising a codec the server was
    /// never told to honour just yields uncompressed responses.
    /// </summary>
    private static (string[] Header, string Flag) NegotiationOf(TrackingHandler handler)
    {
        var request = handler.Requests.Single();
        var query = request.RequestUri.Query;
        var flag = query.Contains("enable_http_compression=true") ? "true"
            : query.Contains("enable_http_compression=false") ? "false"
            : "<absent>";
        return (request.Headers.AcceptEncoding.Select(e => e.Value).ToArray(), flag);
    }

    private static string[] AcceptEncodingOf(TrackingHandler handler)
        => handler.Requests.Single().Headers.AcceptEncoding.Select(e => e.Value).ToArray();

    [Test]
    public async Task QueryOptionsAcceptEncoding_WhenSet_ReplacesDefaultAcceptEncodingHeader()
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "zstd" });

        var request = handler.Requests.Single();
        var encodings = request.Headers.AcceptEncoding.Select(e => e.Value).ToArray();
        Assert.That(encodings, Is.EqualTo(new[] { "zstd" }));
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WhenSet_ForcesEnableHttpCompressionEvenWithoutClientCompression()
    {
        var (client, handler) = CreateClient(useCompression: false);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "gzip" });

        var request = handler.Requests.Single();
        Assert.That(request.RequestUri.Query, Does.Contain("enable_http_compression=true"));
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WhenNullOrEmpty_PreservesDefaultAcceptEncodingHeader()
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = null });

        Assert.That(
            AcceptEncodingOf(handler),
            Is.EqualTo(new[] { "zstd", "lz4", "gzip", "deflate" }),
            "the default advertises every codec the driver can decode, except br — see ResponseDecompression.DefaultAcceptEncoding");
    }

    // ---------------------------------------------------------------------------------------------
    // Client-level Accept-Encoding, and which requests get the default.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public async Task SettingsAcceptEncoding_WhenSet_ReplacesTheDefaultCodecList()
    {
        var (client, handler) = CreateClient(useCompression: true, acceptEncoding: "br, gzip");

        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.That(AcceptEncodingOf(handler), Is.EqualTo(new[] { "br", "gzip" }));
    }

    [Test]
    public async Task SettingsAcceptEncoding_WhenSet_ForcesEnableHttpCompressionEvenWithoutClientCompression()
    {
        var (client, handler) = CreateClient(useCompression: false, acceptEncoding: "lz4");

        await client.ExecuteNonQueryAsync("SELECT 1");

        var request = handler.Requests.Single();
        Assert.Multiple(() =>
        {
            Assert.That(AcceptEncodingOf(handler), Is.EqualTo(new[] { "lz4" }), "an explicit codec beats UseCompression=false");
            Assert.That(request.RequestUri.Query, Does.Contain("enable_http_compression=true"));
        });
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WhenSet_OverridesTheClientLevelSetting()
    {
        var (client, handler) = CreateClient(useCompression: true, acceptEncoding: "gzip");

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "br" });

        Assert.That(AcceptEncodingOf(handler), Is.EqualTo(new[] { "br" }));
    }

    [Test]
    public async Task AcceptEncoding_WithCompressionDisabledAndNothingExplicit_IsNotSentAtAll()
    {
        var (client, handler) = CreateClient(useCompression: false);

        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.That(AcceptEncodingOf(handler), Is.Empty);
    }

    /// <summary>
    /// A raw result hands its body to the caller untouched, so it advertises no codec at all: nothing in
    /// the driver decodes it, and — now that the driver's handler leaves <c>AutomaticDecompression</c> off
    /// — nothing in the framework does either, so any codec offered here would reach the caller still
    /// compressed and silently change what an export writes to disk. A caller who wants compressed bytes
    /// asks for a codec explicitly; see
    /// <see cref="ExecuteRawResultAsync_WithExplicitAcceptEncoding_StillSendsIt"/>.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_WithNoExplicitAcceptEncoding_OffersNoCodec()
    {
        var (client, handler) = CreateClient(useCompression: true);

        using var result = await client.ExecuteRawResultAsync("SELECT 1 FORMAT TSV");

        Assert.That(AcceptEncodingOf(handler), Is.Empty);
    }

    /// <summary>
    /// Accept-Encoding is not a blocked custom header, so it can be injected that way. Only the per-query
    /// property outranks it — the precedence that applied before client-level configuration existed.
    /// </summary>
    [Test]
    public async Task CustomHeaderAcceptEncoding_OutranksTheClientLevelSetting()
    {
        var (client, handler) = CreateClient(useCompression: true, acceptEncoding: "lz4");

        await client.ExecuteNonQueryAsync(
            "SELECT 1",
            options: new QueryOptions
            {
                CustomHeaders = new Dictionary<string, string> { ["Accept-Encoding"] = "gzip" },
            });

        Assert.That(AcceptEncodingOf(handler), Is.EqualTo(new[] { "gzip" }));
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_OutranksACustomHeader()
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync(
            "SELECT 1",
            options: new QueryOptions
            {
                AcceptEncoding = "br",
                CustomHeaders = new Dictionary<string, string> { ["Accept-Encoding"] = "gzip" },
            });

        Assert.That(AcceptEncodingOf(handler), Is.EqualTo(new[] { "br" }));
    }

    /// <summary>
    /// PostStreamAsync and InsertRawStreamAsync are public and return the HttpResponseMessage itself, so
    /// their bodies belong to the caller and get the same treatment as a raw result.
    /// </summary>
    [Test]
    public async Task PostStreamAsync_WithNoExplicitAcceptEncoding_OffersNoCodec()
    {
        var (client, handler) = CreateClient(useCompression: true);

        using var response = await client.PostStreamAsync(
            "INSERT INTO t FORMAT RowBinary", new MemoryStream([1]), isCompressed: false, CancellationToken.None);

        Assert.That(AcceptEncodingOf(handler), Is.Empty);
    }

    /// <summary>
    /// A value that names no codec counts as "not set" at either level, so the default still applies. The
    /// alternative — clearing the header and putting nothing back — reads as "no compression", since the
    /// server sends identity when nothing is offered, which is not what an accidentally-blank config means.
    /// </summary>
    [TestCase("", TestName = "{m}(empty)")]
    [TestCase("   ", TestName = "{m}(whitespace)")]
    [TestCase(",", TestName = "{m}(separator only)")]
    [TestCase(" , ", TestName = "{m}(separators and whitespace)")]
    public async Task AcceptEncoding_WithAValueThatNamesNoCodec_FallsBackToTheDefault(string acceptEncoding)
    {
        var (client, handler) = CreateClient(useCompression: true, acceptEncoding: acceptEncoding);

        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.That(AcceptEncodingOf(handler), Is.EqualTo(new[] { "zstd", "lz4", "gzip", "deflate" }));
    }

    [TestCase("   ", TestName = "{m}(whitespace)")]
    [TestCase(",", TestName = "{m}(separator only)")]
    public async Task QueryOptionsAcceptEncoding_WithAValueThatNamesNoCodec_FallsBackToTheDefault(string acceptEncoding)
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = acceptEncoding });

        Assert.That(AcceptEncodingOf(handler), Is.EqualTo(new[] { "zstd", "lz4", "gzip", "deflate" }));
    }

    [Test]
    public async Task AcceptEncoding_WithAValueThatNamesNoCodec_DoesNotForceHttpCompression()
    {
        var (client, handler) = CreateClient(useCompression: false, acceptEncoding: "  ");

        await client.ExecuteNonQueryAsync("SELECT 1");

        Assert.That(handler.Requests.Single().RequestUri.Query, Does.Contain("enable_http_compression=false"));
    }

    /// <summary>
    /// The cell that actually pins the precedence: a per-query value naming no codec falls through
    /// <b>one level</b>, to the client-level value. The two neighbouring tests cover a blank at each level
    /// while the other is unset, which leaves this distinguishable only here — falling back to the driver
    /// default, or clearing the header outright, would both pass those and neither is intended.
    /// <para>
    /// <c>UseCompression</c> is deliberately <b>off</b>. It is the only way the flag assertion carries
    /// weight: with compression on, <c>Settings.UseCompression</c> forces
    /// <c>enable_http_compression</c> by itself, so the flag would read <c>true</c> however the
    /// precedence in <c>CreateUriBuilder</c> resolved and the assertion would be vacuous. With it off,
    /// the flag can only be on because the blank per-query value fell through to the client-level codec —
    /// which is exactly the drift this cell exists to catch. (Verified by mutation: rewriting
    /// <c>ExplicitAcceptEncoding</c> to <c>perQuery ?? Settings.AcceptEncoding</c> is caught here and
    /// nowhere else in the suite.)
    /// </para>
    /// </summary>
    [TestCase("", TestName = "{m}(empty)")]
    [TestCase("   ", TestName = "{m}(whitespace)")]
    [TestCase(",", TestName = "{m}(separator only)")]
    public async Task QueryOptionsAcceptEncoding_WithAValueThatNamesNoCodec_FallsBackToTheClientLevelSetting(string perQuery)
    {
        var (client, handler) = CreateClient(useCompression: false, acceptEncoding: "lz4");

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = perQuery });

        var negotiation = NegotiationOf(handler);
        Assert.Multiple(() =>
        {
            Assert.That(negotiation.Header, Is.EqualTo(new[] { "lz4" }), "a blank per-query value means 'not set', not 'use the default' and not 'send nothing'");
            Assert.That(negotiation.Flag, Is.EqualTo("true"), "the client-level codec it fell back to still has to force enable_http_compression");
        });
    }

    /// <summary>
    /// Both levels set, asserting the header <i>and</i> the URI flag together — the header alone is
    /// covered by <see cref="QueryOptionsAcceptEncoding_WhenSet_OverridesTheClientLevelSetting"/>.
    /// </summary>
    [Test]
    public async Task QueryOptionsAcceptEncoding_OverridingTheClientLevel_AlsoForcesHttpCompression()
    {
        var (client, handler) = CreateClient(useCompression: false, acceptEncoding: "gzip");

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "br" });

        var negotiation = NegotiationOf(handler);
        Assert.Multiple(() =>
        {
            Assert.That(negotiation.Header, Is.EqualTo(new[] { "br" }));
            Assert.That(negotiation.Flag, Is.EqualTo("true"), "the winning codec is what has to be honoured, so the flag must follow it");
        });
    }

    /// <summary>
    /// A blank per-query value with nothing at the client level and compression off stays off — the
    /// mirror of <see cref="AcceptEncoding_WithAValueThatNamesNoCodec_DoesNotForceHttpCompression"/>,
    /// which covers the client-level side.
    /// </summary>
    [Test]
    public async Task QueryOptionsAcceptEncoding_WithAValueThatNamesNoCodec_DoesNotForceHttpCompression()
    {
        var (client, handler) = CreateClient(useCompression: false);

        await client.ExecuteNonQueryAsync("SELECT 1", options: new QueryOptions { AcceptEncoding = "  " });

        var negotiation = NegotiationOf(handler);
        Assert.Multiple(() =>
        {
            Assert.That(negotiation.Header, Is.Empty);
            Assert.That(negotiation.Flag, Is.EqualTo("false"));
        });
    }

    /// <summary>
    /// An <c>Accept-Encoding</c> injected through <b>client-level</b> CustomHeaders outranks the
    /// client-level property, the same way a per-query custom header does in
    /// <see cref="CustomHeaderAcceptEncoding_OutranksTheClientLevelSetting"/>: both are attached after
    /// the property. The URI flag still follows the property, since a custom header is opaque to the
    /// driver — asserted so that asymmetry is a decision on record rather than a surprise.
    /// </summary>
    [Test]
    public async Task ClientLevelCustomHeaderAcceptEncoding_OutranksTheClientLevelSetting()
    {
        var (client, handler) = CreateClient(useCompression: true, acceptEncoding: "lz4", customHeaderAcceptEncoding: "gzip");

        await client.ExecuteNonQueryAsync("SELECT 1");

        var negotiation = NegotiationOf(handler);
        Assert.Multiple(() =>
        {
            Assert.That(negotiation.Header, Is.EqualTo(new[] { "gzip" }));
            Assert.That(negotiation.Flag, Is.EqualTo("true"));
        });
    }

    /// <summary>
    /// Compression off but a codec named explicitly, on the verbatim path: the likeliest shape of
    /// "I don't want compressed reads, but I do want a compressed export". The explicit codec beats both
    /// <c>UseCompression=false</c> and the suppression that
    /// <see cref="ExecuteRawResultAsync_WithNoExplicitAcceptEncoding_OffersNoCodec"/> pins, and still
    /// forces <c>enable_http_compression</c> so the server actually honours it.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_WithExplicitAcceptEncodingAndCompressionDisabled_StillSendsItAndForcesHttpCompression()
    {
        var (client, handler) = CreateClient(useCompression: false, acceptEncoding: "gzip");

        using var result = await client.ExecuteRawResultAsync("SELECT 1 FORMAT TSV");

        var negotiation = NegotiationOf(handler);
        Assert.Multiple(() =>
        {
            Assert.That(negotiation.Header, Is.EqualTo(new[] { "gzip" }));
            Assert.That(negotiation.Flag, Is.EqualTo("true"));
        });
    }

    /// <summary>
    /// The same on the verbatim path from the per-query level, which reaches the header through a
    /// different call site than the client-level property.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_WithPerQueryAcceptEncoding_SendsItAndForcesHttpCompression()
    {
        var (client, handler) = CreateClient(useCompression: false);

        using var result = await client.ExecuteRawResultAsync(
            "SELECT 1 FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "lz4" });

        var negotiation = NegotiationOf(handler);
        Assert.Multiple(() =>
        {
            Assert.That(negotiation.Header, Is.EqualTo(new[] { "lz4" }));
            Assert.That(negotiation.Flag, Is.EqualTo("true"));
        });
    }

    /// <summary>
    /// The distinction that matters: a parsing request advertises what the driver can decode, a verbatim
    /// one advertises nothing, because nobody would decode it. Asserted against each other so neither can
    /// silently drift into the other.
    /// </summary>
    [Test]
    public async Task AcceptEncoding_DiffersBetweenParsedAndVerbatimBodies()
    {
        var (parsing, parsingHandler) = CreateClient(useCompression: true);
        await parsing.ExecuteNonQueryAsync("SELECT 1");

        var (raw, rawHandler) = CreateClient(useCompression: true);
        using var result = await raw.ExecuteRawResultAsync("SELECT 1 FORMAT TSV");

        Assert.Multiple(() =>
        {
            Assert.That(AcceptEncodingOf(parsingHandler), Is.EqualTo(new[] { "zstd", "lz4", "gzip", "deflate" }));
            Assert.That(AcceptEncodingOf(rawHandler), Is.Empty);
        });
    }

    [Test]
    public async Task ExecuteRawResultAsync_WithExplicitAcceptEncoding_StillSendsIt()
    {
        var (client, handler) = CreateClient(useCompression: true, acceptEncoding: "lz4");

        using var result = await client.ExecuteRawResultAsync("SELECT 1 FORMAT TSV");

        Assert.That(
            AcceptEncodingOf(handler),
            Is.EqualTo(new[] { "lz4" }),
            "the caller asked for it themselves, so honouring it is not a silent change");
    }

    [Test]
    public async Task QueryOptionsAcceptEncoding_WithMultipleValues_ParsesEachEntryWithQualityWeights()
    {
        var (client, handler) = CreateClient(useCompression: true);

        await client.ExecuteNonQueryAsync(
            "SELECT 1",
            options: new QueryOptions { AcceptEncoding = "zstd, gzip;q=0.5" });

        var request = handler.Requests.Single();
        var encodings = request.Headers.AcceptEncoding.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(encodings.Select(e => e.Value).ToArray(), Is.EqualTo(new[] { "zstd", "gzip" }));
            Assert.That(encodings[1].Quality, Is.EqualTo(0.5));
        });
    }

    [Test]
    public async Task CommandAcceptEncoding_WhenSet_FlowsThroughToHttpHeader()
    {
        var (client, handler) = CreateClient(useCompression: false);
        using var connection = client.CreateConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.AcceptEncoding = "br";

        await command.ExecuteNonQueryAsync(CancellationToken.None);

        var request = handler.Requests.Single();
        var encodings = request.Headers.AcceptEncoding.Select(e => e.Value).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(encodings, Is.EqualTo(new[] { "br" }));
            Assert.That(request.RequestUri.Query, Does.Contain("enable_http_compression=true"));
        });
    }

    [TestCase("gzip")]
    [TestCase("deflate")]
    [TestCase("br")]
    [TestCase("brotli")]
    public async Task HandleError_WithSupportedContentEncoding_DecompressesIntoExceptionMessage(string contentEncoding)
    {
        var serverMessage = "Code: 62. DB::Exception: Syntax error: failed at position 1";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = BuildCompressedContent(serverMessage, contentEncoding),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT bad_syntax",
                options: new QueryOptions { AcceptEncoding = "gzip" }));

        Assert.That(ex.Message, Does.Contain("Syntax error"));
    }

    [Test]
    public async Task HandleError_WithCompressedNonUtf8Body_UsesContentTypeCharset()
    {
        var serverMessage = "Code: 62. DB::Exception: Неверный синтаксис";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = BuildCompressedContent(serverMessage, "gzip", Encoding.Unicode, "utf-16"),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT bad_syntax",
                options: new QueryOptions { AcceptEncoding = "gzip" }));

        Assert.That(ex.Message, Does.Contain("Неверный синтаксис"));
    }

    [Test]
    public async Task HandleError_WithCompressedBodyAndInvalidCharset_FallsBackToUtf8()
    {
        var serverMessage = "Code: 62. DB::Exception: Syntax error";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = BuildCompressedContent(serverMessage, "gzip", charset: "invalid-charset"),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT bad_syntax",
                options: new QueryOptions { AcceptEncoding = "gzip" }));

        Assert.That(ex.Message, Does.Contain("Syntax error"));
    }

    [Test]
    public void HandleError_WithUnsupportedContentEncoding_ThrowsExceptionWithPlaceholderMessage()
    {
        var fakeHandler = new TrackingHandler(_ =>
        {
            var content = new ByteArrayContent(new byte[] { 0xFF, 0x06, 0x00, 0x00 }); // arbitrary snappy-looking bytes
            content.Headers.ContentEncoding.Add("snappy");
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                ReasonPhrase = "Internal Server Error",
                Content = content,
            };
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1",
                options: new QueryOptions { AcceptEncoding = "snappy" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("unsupported Content-Encoding: snappy"));
            Assert.That(ex.Message, Does.Contain("system.query_log"));
        });
    }

    [Test]
    public void HandleError_WithUnsupportedContentEncoding_DrainsBodyBeforeReturningPlaceholderMessage()
    {
        var body = new byte[] { 0xFF, 0x06, 0x00, 0x00, 0x00, 0x01 };
        var trackingStream = new TrackingReadStream(body);
        var fakeHandler = new TrackingHandler(_ =>
        {
            var content = new StreamContent(trackingStream);
            content.Headers.ContentEncoding.Add("snappy");
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                ReasonPhrase = "Internal Server Error",
                Content = content,
            };
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1",
                options: new QueryOptions { AcceptEncoding = "snappy" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("unsupported Content-Encoding: snappy"));
            Assert.That(trackingStream.BytesRead, Is.EqualTo(body.Length));
            Assert.That(trackingStream.IsDisposed, Is.True);
        });
    }

    [Test]
    public void HandleError_WithoutContentEncoding_ReadsBodyVerbatim()
    {
        var serverMessage = "Code: 62. DB::Exception: plain text";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = new StringContent(serverMessage, Encoding.UTF8),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.That(ex.Message, Does.Contain("plain text"));
    }

    [TestCase("")]
    [TestCase("   ")]
    [TestCase("\r\n\t ")]
    public void HandleError_WithEmptyOrWhitespaceBody_ThrowsExceptionWithHttpStatusAndReasonPhrase(string body)
    {
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            ReasonPhrase = "Internal Server Error",
            Content = new StringContent(body),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("500"));
            Assert.That(ex.Message, Does.Contain("Internal Server Error"));
            Assert.That(ex.ErrorCode, Is.EqualTo(-1));
        });
    }

    [Test]
    public void HandleError_WithEmptyBodyAndExceptionCodeHeader_UsesHeaderValueAsErrorCode()
    {
        var fakeHandler = new TrackingHandler(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            {
                ReasonPhrase = "Service Unavailable",
                Content = new StringContent(string.Empty),
            };
            response.Headers.TryAddWithoutValidation("X-ClickHouse-Exception-Code", "241");
            return response;
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(241));
            Assert.That(ex.Message, Does.Contain("503"));
            Assert.That(ex.Message, Does.Contain("241"));
        });
    }

    [Test]
    public void HandleError_WithNonEmptyBody_PreservesBodyMessageAndParsedErrorCode()
    {
        var serverMessage = "Code: 62. DB::Exception: Syntax error";
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            ReasonPhrase = "Internal Server Error",
            Content = new StringContent(serverMessage, Encoding.UTF8),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo(serverMessage));
            Assert.That(ex.ErrorCode, Is.EqualTo(62));
        });
    }

    [Test]
    public void HandleError_WithEmptyBodyAndNoReasonPhrase_MessageStillIncludesHttpStatus()
    {
        var fakeHandler = new TrackingHandler(_ => new HttpResponseMessage(HttpStatusCode.BadGateway)
        {
            ReasonPhrase = string.Empty,
            Content = new StringContent(string.Empty),
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("502"));
            Assert.That(ex.Message, Does.Not.Contain("("));
            Assert.That(ex.ErrorCode, Is.EqualTo(-1));
        });
    }

    [Test]
    public void HandleError_WithEmptyBodyAndNonNumericExceptionCodeHeader_FallsBackToMinusOne()
    {
        var fakeHandler = new TrackingHandler(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                ReasonPhrase = "Internal Server Error",
                Content = new StringContent(string.Empty),
            };
            response.Headers.TryAddWithoutValidation("X-ClickHouse-Exception-Code", "not-a-number");
            return response;
        });
        using var httpClient = new HttpClient(fakeHandler);
        var settings = new ClickHouseClientSettings
        {
            HttpClient = httpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync("SELECT 1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(-1));
            Assert.That(ex.Message, Does.Contain("500"));
        });
    }

    [Test]
    public void ContentEncoding_WhenHeaderPresent_ReturnsHeaderValue()
    {
        var content = new StringContent("body");
        content.Headers.ContentEncoding.Add("zstd");
        using var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
        using var raw = new ClickHouseRawResult(response);

        Assert.That(raw.ContentEncoding, Is.EqualTo("zstd"));
    }

    [Test]
    public void ContentEncoding_WhenHeaderAbsent_ReturnsNull()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("body"),
        };
        using var raw = new ClickHouseRawResult(response);

        Assert.That(raw.ContentEncoding, Is.Null);
    }

    [Test]
    public void ContentEncoding_WhenIdentity_NormalizedToNull()
    {
        var content = new StringContent("body");
        content.Headers.ContentEncoding.Add("identity");
        using var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
        using var raw = new ClickHouseRawResult(response);

        Assert.That(raw.ContentEncoding, Is.Null);
    }

    [Test]
    public async Task AcceptEncodingGzip_AgainstRealServer_ResponseIsActuallyGzipCompressed()
    {
        using var rawHandler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var rawHttpClient = new HttpClient(rawHandler);
        var settings = new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = rawHttpClient,
        };
        using var client = new ClickHouseClient(settings);

        using var result = await client.ExecuteRawResultAsync(
            "SELECT number FROM numbers(5) FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "gzip" });

        Assert.That(result.ContentEncoding, Is.EqualTo("gzip"));

        await using var compressed = await result.ReadAsStreamAsync();
        await using var decompressor = new GZipStream(compressed, CompressionMode.Decompress);
        using var reader = new StreamReader(decompressor, Encoding.UTF8);
        var body = await reader.ReadToEndAsync();

        Assert.That(body, Is.EqualTo("0\n1\n2\n3\n4\n"));
    }

    /// <summary>
    /// The server compresses error bodies with the same codec it would have used for a result, so a
    /// zstd-compressed error must arrive readable rather than as the placeholder this case asserted
    /// before the driver could decode zstd. Uses a handler that decodes nothing, so the text can only
    /// be there because the driver decoded it.
    /// </summary>
    [Test]
    public void AcceptEncodingZstd_AgainstRealServer_ErrorBodyIsDecodedAndReadable()
    {
        using var rawHandler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var rawHttpClient = new HttpClient(rawHandler);
        var settings = new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = rawHttpClient,
        };
        using var client = new ClickHouseClient(settings);

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(
            () => client.ExecuteNonQueryAsync(
                "SELECT * FROM no_such_table_for_acceptencoding_test",
                options: new QueryOptions { AcceptEncoding = "zstd" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("no_such_table_for_acceptencoding_test"));
            Assert.That(ex.Message, Does.Not.Contain("unsupported Content-Encoding"));
        });
    }

    /// <summary>
    /// The write-side counterpart of <see cref="AcceptEncodingGzip_AgainstRealServer_ResponseIsActuallyGzipCompressed"/>
    /// for zstd: the body on the wire is a real zstd frame, decoded here by the vendored codec rather
    /// than by anything in the framework (which cannot decode zstd at all).
    /// </summary>
    [Test]
    public async Task AcceptEncodingZstd_AgainstRealServer_ResponseIsActuallyZstdCompressed()
    {
        using var rawHandler = new HttpClientHandler { AutomaticDecompression = DecompressionMethods.None };
        using var rawHttpClient = new HttpClient(rawHandler);
        var settings = new ClickHouseClientSettings(TestUtilities.GetTestClickHouseClientSettings())
        {
            HttpClient = rawHttpClient,
        };
        using var client = new ClickHouseClient(settings);

        using var result = await client.ExecuteRawResultAsync(
            "SELECT number FROM numbers(5) FORMAT TSV",
            options: new QueryOptions { AcceptEncoding = "zstd" });

        Assert.That(result.ContentEncoding, Is.EqualTo("zstd"));

        var compressed = await result.ReadAsByteArrayAsync();
        Assert.That(compressed[..4], Is.EqualTo(new byte[] { 0x28, 0xB5, 0x2F, 0xFD }), "expected a zstd frame magic");

        using var source = new MemoryStream(compressed);
        await using var plaintext = ZstdCompressor.Default.Decompress(source, leaveOpen: true);
        using var reader = new StreamReader(plaintext, Encoding.UTF8);

        Assert.That(await reader.ReadToEndAsync(), Is.EqualTo("0\n1\n2\n3\n4\n"));
    }

    private static ByteArrayContent BuildCompressedContent(
        string text,
        string contentEncoding,
        Encoding textEncoding = null,
        string charset = null)
    {
        textEncoding ??= Encoding.UTF8;
        using var buffer = new MemoryStream();
        using (var compressed = CreateCompressionStream(buffer, contentEncoding))
        {
            var bytes = textEncoding.GetBytes(text);
            compressed.Write(bytes, 0, bytes.Length);
        }

        var content = new ByteArrayContent(buffer.ToArray());
        if (charset != null)
        {
            content.Headers.ContentType = new MediaTypeHeaderValue("text/plain")
            {
                CharSet = charset,
            };
        }

        content.Headers.ContentEncoding.Add(contentEncoding);
        return content;
    }

    private static Stream CreateCompressionStream(Stream stream, string contentEncoding)
    {
        return contentEncoding switch
        {
            "gzip" => new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true),
            "deflate" => new DeflateStream(stream, CompressionLevel.Fastest, leaveOpen: true),
            "br" or "brotli" => new BrotliStream(stream, CompressionLevel.Fastest, leaveOpen: true),
            _ => throw new ArgumentOutOfRangeException(nameof(contentEncoding), contentEncoding, null),
        };
    }

    private sealed class TrackingReadStream : Stream
    {
        private readonly MemoryStream inner;

        public TrackingReadStream(byte[] bytes)
        {
            inner = new MemoryStream(bytes);
        }

        public int BytesRead { get; private set; }

        public bool IsDisposed { get; private set; }

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => inner.Length;

        public override long Position
        {
            get => inner.Position;
            set => inner.Position = value;
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count)
        {
            var bytesRead = inner.Read(buffer, offset, count);
            BytesRead += bytesRead;
            return bytesRead;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var bytesRead = inner.Read(buffer.Span);
            BytesRead += bytesRead;
            return ValueTask.FromResult(bytesRead);
        }

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value) => inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            IsDisposed = true;
            inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
