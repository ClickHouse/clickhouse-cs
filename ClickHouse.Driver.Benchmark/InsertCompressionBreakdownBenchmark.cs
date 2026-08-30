using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Decomposes the binary-insert compression tradeoff into its two competing costs, measured
/// independently so the end-to-end numbers from <see cref="BinaryInsertCompressionBenchmark"/> can be
/// attributed: <b>client-side compression CPU</b> vs <b>time on the wire</b>.
///
/// A real RowBinary payload is built once (via the public <see cref="ExtendedBinaryWriter"/>) and, from
/// it, pre-compressed buffers — each one paired with the <see cref="IClickHouseCompressor.ContentEncoding"/>
/// of the very codec that produced it (see <see cref="PreparedPayload"/>), so a body can never be sent
/// under a codec name it was not compressed with. Then:
///  - <see cref="Compress"/> runs the codec in-memory (no network) — pure compression CPU.
///  - <see cref="Wire"/> uploads a pre-built buffer under its own <c>Content-Encoding</c> without
///    re-compressing — so it measures wire+server for that exact payload size, with no compression work
///    overlapping the send.
///
/// The decision reduces to: is <c>Compress</c> time less than <c>Wire[Raw] - Wire[GzipFastest]</c>
/// (the wire time compression buys back)? Expected: yes on cloud (wire dominates), no on loopback.
///
/// <see cref="Wire"/> posts the body itself rather than through the driver (see <see cref="wireClient"/>),
/// so it isolates transport and server cost — and, in baseline-vs-PR comparison runs, it is the one arm
/// that cannot move because of a driver change. The driver's own insert path is measured end-to-end by
/// <see cref="BinaryInsertCompressionBenchmark"/>.
///
/// Run against both endpoints via CLICKHOUSE_CONNECTION; the payload sizes are printed at setup.
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class InsertCompressionBreakdownBenchmark
{
    public enum Payload
    {
        Raw,
        GzipFastest,
        GzipOptimal,
#if ZSTD_AVAILABLE
        ZstdDefault,
#endif
    }

    private const string Table = "default.benchmark_compression_breakdown";
    private const string InsertSql =
        "INSERT INTO " + Table + " (Id, Name, Value, Payload) FORMAT RowBinary";

    /// <summary>Set by the server when a request failed, even on a status line already flushed as 2xx.</summary>
    private const string ExceptionCodeHeader = "X-ClickHouse-Exception-Code";

    private ClickHouseClient client;

    /// <summary>
    /// Poster for <see cref="Wire"/>. The driver's public stream API —
    /// <c>PostStreamAsync(sql, stream, bool isCompressed, …)</c> — can only declare
    /// <c>Content-Encoding: gzip</c> (it maps the bool to the literal <c>"gzip"</c>), so it cannot upload
    /// an already-zstd body: the server would try to inflate it and fail with
    /// <c>ZLIB_INFLATE_FAILED</c>. Posting the pre-built buffer directly lets every arm declare its own
    /// codec, and keeps the measured region identical across arms (one POST, no compression work) instead
    /// of measuring gzip through the driver and zstd through something else.
    ///
    /// Only the <i>request construction</i> is hand-rolled; the <b>transport</b> is the same one the driver
    /// would use for these settings (see <see cref="WirePoster"/>), so this arm reaches the endpoint under
    /// the same connection conditions the driver's own arms do.
    /// </summary>
    private WirePoster poster;

    private byte[] rawBytes;
    private Dictionary<Payload, PreparedPayload> payloads;

    [Params(500_000)]
    public int Count { get; set; }

    // For Wire: which pre-built payload to upload. For Compress: which level to run (Raw is ignored).
    [Params(
        Payload.Raw,
        Payload.GzipFastest,
        Payload.GzipOptimal
#if ZSTD_AVAILABLE
        ,
        Payload.ZstdDefault
#endif
        )]
    public Payload Kind { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION")
            ?? "Host=localhost";

        client = new ClickHouseClient(connectionString);
        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE IF NOT EXISTS {Table} (Id Int64, Name String, Value Float64, Payload String) ENGINE Null");

        rawBytes = BuildRowBinary(Count);
        payloads = new Dictionary<Payload, PreparedPayload>();
        foreach (var kind in Enum.GetValues<Payload>())
            payloads[kind] = PreparedPayload.For(rawBytes, CompressorFor(kind));

        poster = WirePoster.For(client.Settings);

        var sizes = new List<string>();
        foreach (var pair in payloads)
        {
            // Prove every arm's body is accepted under the codec it declares before anything is timed:
            // a codec the server rejects would otherwise surface as a failed measurement mid-run.
            await Insert(pair.Value);

            sizes.Add(
                $"{pair.Key}={pair.Value.Bytes.Length / 1024.0 / 1024:F1} MiB " +
                $"({100.0 * pair.Value.Bytes.Length / rawBytes.Length:F1}%, " +
                $"Content-Encoding: {pair.Value.ContentEncoding ?? "none"})");
        }

        Console.WriteLine($"[breakdown] payload sizes: {string.Join(", ", sizes)}");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        poster?.Dispose();
        client?.Dispose();
    }

    /// <summary>Client-side compression CPU only — compress the raw buffer in-memory, discard output.</summary>
    [Benchmark]
    public void Compress()
    {
        // Raw has no codec of its own; it measures nothing here, so it borrows the cheapest one and is
        // simply ignored when reading the results (as it always was).
        var compressor = CompressorFor(Kind) ?? new GZipCompressor(CompressionLevel.Fastest);

        using var sink = compressor.Compress(Stream.Null, leaveOpen: true);
        sink.Write(rawBytes, 0, rawBytes.Length);
    }

    /// <summary>Wire + server time for a given pre-built payload — no compression work in the measured region.</summary>
    [Benchmark]
    public Task Wire() => Insert(payloads[Kind]);

    /// <summary>
    /// One INSERT of a pre-built body, declared under the codec that produced it. Failures throw: a body
    /// the server cannot decode (a codec mismatch) or an insert it rejects would otherwise be timed as if
    /// it had succeeded, quietly reporting an error round-trip as this codec's wire cost.
    /// </summary>
    private async Task Insert(PreparedPayload payload)
    {
        var content = new ByteArrayContent(payload.Bytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        if (payload.ContentEncoding != null)
            content.Headers.ContentEncoding.Add(payload.ContentEncoding);

        using var request = new HttpRequestMessage(HttpMethod.Post, poster.Uri) { Content = content };

        // Per-request, never on the client's DefaultRequestHeaders: the client may be one the settings
        // supplied, and a benchmark must not mutate a caller's client. Same precedence the driver applies
        // (bearer token when configured, Basic otherwise).
        request.Headers.Authorization = poster.Authorization;

        using var response = await poster.Client().SendAsync(
            request, HttpCompletionOption.ResponseContentRead, CancellationToken.None);

        response.EnsureSuccessStatusCode();

        // The status line can be flushed before the server hits the error, so a 2xx alone is not proof;
        // the driver reads this same header when it reports server errors.
        if (response.Headers.TryGetValues(ExceptionCodeHeader, out var codes))
        {
            throw new InvalidOperationException(
                $"Insert reported {ExceptionCodeHeader}: {string.Join(", ", codes)} " +
                $"(Content-Encoding: {payload.ContentEncoding ?? "none"}).");
        }
    }

    /// <summary>The codec an arm exercises, or <c>null</c> for the uncompressed arm.</summary>
    private static IClickHouseCompressor CompressorFor(Payload kind) => kind switch
    {
        Payload.Raw => null,
        Payload.GzipFastest => new GZipCompressor(CompressionLevel.Fastest),
        Payload.GzipOptimal => new GZipCompressor(CompressionLevel.Optimal),
#if ZSTD_AVAILABLE
        Payload.ZstdDefault => ZstdCompressor.Default,
#endif
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
    };

    /// <summary>
    /// Where and how <see cref="Wire"/> posts: the endpoint, the <c>Authorization</c> header, and the
    /// <see cref="HttpClient"/> to send through.
    ///
    /// The <b>request</b> is hand-rolled (the driver's public stream API cannot declare a non-gzip
    /// <c>Content-Encoding</c>) but the <b>transport must not be</b>: if this arm connected differently from
    /// the arms it is compared against, it would measure a different connection — and against an HTTPS
    /// endpoint with <see cref="ClickHouseClientSettings.SkipServerCertificateValidation"/> set it would not
    /// reach the server at all while every driver arm did. So the client is obtained the way
    /// <c>ClickHouseClient</c> obtains its own (see <c>ClickHouseClient.HttpClient</c> and its factory
    /// selection):
    /// <list type="bullet">
    ///   <item>a settings-supplied <see cref="ClickHouseClientSettings.HttpClient"/> is reused as-is — its
    ///   handler is by definition the transport the caller configured — and is never disposed or mutated
    ///   here, which is why auth goes on the request rather than on <c>DefaultRequestHeaders</c>;</item>
    ///   <item>a settings-supplied <see cref="ClickHouseClientSettings.HttpClientFactory"/> is asked for a
    ///   client <i>per request</i>, as the driver does, so handler rotation applies to this arm too instead
    ///   of pinning one handler for the whole run;</item>
    ///   <item>otherwise the handler is built to match the driver's default
    ///   (<c>HttpHandlerProvider.CreateHandler</c>): <c>AutomaticDecompression</c> off — the driver decodes
    ///   responses itself, and a mask would also widen <c>Accept-Encoding</c> at send time — the same
    ///   pooled-connection idle timeout, <see cref="ClickHouseClientSettings.Timeout"/>, and certificate
    ///   validation bypassed only when the settings ask for it.</item>
    /// </list>
    ///
    /// The <b>URI</b> is the driver's base URI plus this INSERT as <c>query</c> and the <c>database</c> only
    /// when one is set. It deliberately carries nothing else — none of the base parameters
    /// <c>ClickHouseUriBuilder</c> always appends (<c>default_format</c>, <c>query_id</c>,
    /// <c>enable_http_compression</c>, …), no custom settings, roles, session or custom headers. This arm
    /// measures transport for a payload size, not the driver's request construction, so those are out of its
    /// scope — the end-to-end arms in <see cref="BinaryInsertCompressionBenchmark"/> are where the driver's
    /// own request path is measured.
    /// </summary>
    private sealed class WirePoster : IDisposable
    {
        private readonly HttpClient client;
        private readonly bool ownsClient;
        private readonly IHttpClientFactory factory;
        private readonly string factoryClientName;

        private WirePoster(
            Uri uri,
            AuthenticationHeaderValue authorization,
            HttpClient client,
            bool ownsClient,
            IHttpClientFactory factory,
            string factoryClientName)
        {
            Uri = uri;
            Authorization = authorization;
            this.client = client;
            this.ownsClient = ownsClient;
            this.factory = factory;
            this.factoryClientName = factoryClientName;
        }

        /// <summary>The INSERT endpoint to post to.</summary>
        public Uri Uri { get; }

        /// <summary>
        /// The <c>Authorization</c> header the driver would send for these settings: bearer token when one is
        /// configured, Basic otherwise (the driver's own precedence, minus the per-query override this arm
        /// has no equivalent of).
        /// </summary>
        public AuthenticationHeaderValue Authorization { get; }

        [SuppressMessage(
            "Security",
            "CA5359:Do Not Disable Certificate Validation",
            Justification = "Mirrors the driver's own SkipServerCertificateValidation handling, opt-in via settings.")]
        public static WirePoster For(ClickHouseClientSettings settings)
        {
            var query = $"query={Uri.EscapeDataString(InsertSql)}";
            if (!string.IsNullOrEmpty(settings.Database))
                query += $"&database={Uri.EscapeDataString(settings.Database)}";

            var uri = new UriBuilder(settings.Protocol, settings.Host, settings.Port, settings.Path ?? string.Empty)
            {
                Query = query,
            }.Uri;

            var authorization = string.IsNullOrEmpty(settings.BearerToken)
                ? new AuthenticationHeaderValue(
                    "Basic",
                    Convert.ToBase64String(Encoding.UTF8.GetBytes($"{settings.Username}:{settings.Password}")))
                : new AuthenticationHeaderValue("Bearer", settings.BearerToken);

            if (settings.HttpClient != null)
                return new WirePoster(uri, authorization, settings.HttpClient, false, null, null);

            if (settings.HttpClientFactory != null)
            {
                return new WirePoster(
                    uri, authorization, null, false, settings.HttpClientFactory, settings.HttpClientName ?? string.Empty);
            }

            var handler = new SocketsHttpHandler
            {
                AutomaticDecompression = DecompressionMethods.None,
                PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5),
            };

            if (settings.SkipServerCertificateValidation)
            {
                handler.SslOptions = new SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = (_, _, _, _) => true,
                };
            }

            var owned = new HttpClient(handler, disposeHandler: true) { Timeout = settings.Timeout };
            return new WirePoster(uri, authorization, owned, true, null, null);
        }

        /// <summary>The client to send through — from the settings' factory when there is one, as the driver does.</summary>
        public HttpClient Client() => factory == null ? client : factory.CreateClient(factoryClientName);

        /// <summary>Disposes only a client this poster created; a caller's client and its factory are left alone.</summary>
        public void Dispose()
        {
            if (ownsClient)
                client.Dispose();
        }
    }

    /// <summary>
    /// A pre-compressed body and the <c>Content-Encoding</c> it must be declared under — produced together
    /// from one compressor so the two cannot drift apart.
    /// </summary>
    private readonly struct PreparedPayload
    {
        private PreparedPayload(byte[] bytes, string contentEncoding)
        {
            Bytes = bytes;
            ContentEncoding = contentEncoding;
        }

        /// <summary>The body to upload.</summary>
        public byte[] Bytes { get; }

        /// <summary>The codec <see cref="Bytes"/> was compressed with, or <c>null</c> when uncompressed.</summary>
        public string ContentEncoding { get; }

        public static PreparedPayload For(byte[] raw, IClickHouseCompressor compressor)
        {
            if (compressor == null)
                return new PreparedPayload(raw, null);

            using var ms = new MemoryStream();
            using (var sink = compressor.Compress(ms, leaveOpen: true))
                sink.Write(raw, 0, raw.Length);

            return new PreparedPayload(ms.ToArray(), compressor.ContentEncoding);
        }
    }

    // Builds a valid ClickHouse RowBinary body for (Int64, String, Float64, String) rows using the
    // same primitives the driver uses: little-endian fixed ints/floats and LEB128-prefixed UTF-8 strings.
    private static byte[] BuildRowBinary(int count)
    {
        using var ms = new MemoryStream();
        using (var writer = new ExtendedBinaryWriter(ms, leaveOpen: true))
        {
            for (int i = 0; i < count; i++)
            {
                writer.Write((long)i);
                WriteString(writer, MakeName(i));
                writer.Write(i * 0.5);
                WriteString(writer, MakePayload(i));
            }
        }
        return ms.ToArray();
    }

    private static void WriteString(ExtendedBinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        writer.Write7BitEncodedInt(bytes.Length);
        writer.Write(bytes);
    }

    private static string MakeName(int i)
    {
        uint h = unchecked((uint)i * 2654435761u);
        return $"row-{i}-{h & 0xffffff:x6}";
    }

    private static string MakePayload(int i)
        => $"event=purchase;status=ok;region=us-east-1;user_id={i};session={((ulong)i * 6364136223846793005UL):x}";
}
