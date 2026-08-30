using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Apples-to-apples comparison of the two ways an HTTP <b>response</b> body can be decompressed:
/// <list type="number">
///   <item><b>Framework</b> — <c>SocketsHttpHandler.AutomaticDecompression</c> (the historical path).</item>
///   <item><b>Driver</b> — <c>ClickHouse.Driver.Http.ResponseDecompression.Wrap(...)</c> on a handler with
///         <c>AutomaticDecompression = None</c> (the path that also unlocks lz4).</item>
/// </list>
/// <para>
/// The compressed bytes are held constant: the payload is compressed exactly once in
/// <see cref="Setup"/> and the whole HTTP response (status line, headers, body) is precomputed into a
/// single <c>byte[]</c> that a bare <see cref="TcpListener"/> replays verbatim for every request. So no
/// iteration pays for compression, ClickHouse, or a real network — what is measured is client-side
/// decompression plus the (identical) loopback transport both arms share. A custom
/// <c>HttpMessageHandler</c> would not do for arm 1: <c>AutomaticDecompression</c> lives in the socket
/// handler, so the request has to travel through a real one.
/// </para>
/// <para>
/// The payload is TSV-shaped tabular text (short numeric + string columns), i.e. what a ClickHouse
/// <c>SELECT</c> actually returns. Random bytes would be incompressible and would make the comparison
/// meaningless.
/// </para>
/// <para>
/// <see cref="Setup"/> is also the correctness gate: both arms are run once and their output must be
/// byte-identical to the original plaintext, and the <c>Content-Encoding</c> header must be stripped in
/// arm 1 / still present in arm 2. A fast-but-not-decoding arm therefore cannot silently "win".
/// </para>
/// <para>Run (all axes), optionally dialing the job down for a quick pass:</para>
/// <code>
/// dotnet run -c Release -- --filter *ResponseDecompression*
/// BENCH_WARMUP=1 BENCH_ITERATIONS=10 BENCH_LAUNCHES=1 dotnet run -c Release -- --filter *ResponseDecompression*
/// </code>
/// </summary>
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class ResponseDecompressionBenchmark
{
    public enum Arm
    {
        /// <summary>SocketsHttpHandler.AutomaticDecompression decodes the body.</summary>
        Framework,

        /// <summary>The driver's ResponseDecompression.Wrap decodes the body.</summary>
        Driver,
    }

    /// <summary>Concurrent in-flight reads for <see cref="ConcurrentReadToEnd"/>.</summary>
    private const int Concurrency = 8;

    /// <summary>Bytes read by <see cref="TimeToFirstRows"/> before it walks away from the stream.</summary>
    private const int FirstRowsBytes = 512;

    /// <summary>
    /// ResponseDecompression is internal and the benchmark project is not an InternalsVisibleTo friend
    /// (only ClickHouse.Driver.Tests is, and the assembly is signed). Bind the real driver method once
    /// as a delegate so the measured region calls the shipping code with no reflection overhead — rather
    /// than re-implementing its GZipStream/ZLibOrDeflateStream choice here, which would risk measuring a
    /// look-alike instead of the code under review.
    /// </summary>
    private static readonly Func<Stream, string, bool, Stream> DriverWrap = BindDriverWrap();

    private HttpClient frameworkClient;
    private HttpClient driverClient;
    private LoopbackServer server;
    private string url;
    private byte[] plaintext;

    [Params(
        PayloadCodec.Gzip,
        PayloadCodec.Deflate
#if ZSTD_AVAILABLE
        ,
        PayloadCodec.Zstd
#endif
        )]
    public PayloadCodec Codec { get; set; }

    /// <summary>
    /// 4 KiB ≈ a small result set (a handful of rows, one TCP segment's worth); 8 MiB ≈ a big export,
    /// large enough that decode CPU dominates per-request fixed costs.
    /// </summary>
    [Params(4 * 1024, 8 * 1024 * 1024)]
    public int PayloadBytes { get; set; }

    [Params(Arm.Framework, Arm.Driver)]
    public Arm Path { get; set; }

    public enum PayloadCodec
    {
        Gzip,

        /// <summary>
        /// HTTP <c>deflate</c>, emitted in the zlib framing (RFC 1950) that ClickHouse actually sends —
        /// not the bare RFC 1951 stream.
        /// </summary>
        Deflate,
#if ZSTD_AVAILABLE

        /// <summary>
        /// <c>zstd</c>, which <see cref="DecompressionMethods"/> cannot decode at all: for this codec
        /// there is no framework arm to compare against, so both arms run the driver's own decoder (see
        /// <see cref="FrameworkCanDecode"/>) and the rows are worth reading along the <i>codec</i> axis —
        /// what zstd's decode costs next to gzip's — rather than the arm axis.
        /// </summary>
        Zstd,
#endif
    }

    /// <summary>
    /// Whether <c>AutomaticDecompression</c> can decode this codec. When it cannot, the framework arm is
    /// not a real alternative and is configured to behave exactly like the driver arm rather than
    /// pretending to decode.
    /// </summary>
    private bool FrameworkCanDecode =>
#if ZSTD_AVAILABLE
        Codec != PayloadCodec.Zstd;
#else
        true;
#endif

    [GlobalSetup]
    public async Task Setup()
    {
        plaintext = MakeTabularPayload(PayloadBytes);
        var body = Compress(plaintext, Codec);
        var token = Token(Codec);

        server = new LoopbackServer(BuildResponse(body, token));
        url = $"http://127.0.0.1:{server.Port}/";

        frameworkClient = new HttpClient(new SocketsHttpHandler
        {
            AutomaticDecompression = FrameworkCanDecode
                ? DecompressionMethods.GZip | DecompressionMethods.Deflate
                : DecompressionMethods.None,
            MaxConnectionsPerServer = Concurrency * 2,
        });
        driverClient = new HttpClient(new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
            MaxConnectionsPerServer = Concurrency * 2,
        });

        // Correctness gate: both arms must reproduce the plaintext exactly, and each must be exercising
        // the decoder we think it is.
        foreach (var arm in new[] { Arm.Framework, Arm.Driver })
        {
            var (bytes, encodingHeader) = await ReadAll(arm);
            if (!bytes.SequenceEqual(plaintext))
                throw new InvalidOperationException($"{arm} arm produced {bytes.Length} bytes, expected {plaintext.Length} identical bytes.");

            var stripped = string.IsNullOrEmpty(encodingHeader);
            if (arm == Arm.Framework && FrameworkCanDecode && !stripped)
                throw new InvalidOperationException("Framework arm still reports Content-Encoding; AutomaticDecompression did not decode.");
            if (arm == Arm.Framework && !FrameworkCanDecode && stripped)
                throw new InvalidOperationException($"Framework arm decoded {Codec}, which AutomaticDecompression cannot do.");
            if (arm == Arm.Driver && stripped)
                throw new InvalidOperationException("Driver arm saw no Content-Encoding; the handler decoded the body instead of ResponseDecompression.");
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        frameworkClient?.Dispose();
        driverClient?.Dispose();
        server?.Dispose();
    }

    /// <summary>Full-stream throughput: decode every byte of the response.</summary>
    [Benchmark]
    public Task<long> ReadToEnd() => Read(Path, int.MaxValue);

    /// <summary>
    /// Streaming latency ("time to first row"): stop after the first <see cref="FirstRowsBytes"/> decoded
    /// bytes. Abandoning the body costs a connection on both arms alike, so the ratio stays meaningful.
    /// </summary>
    [Benchmark]
    public Task<long> TimeToFirstRows() => Read(Path, FirstRowsBytes);

    /// <summary>The case a regression would disqualify: <see cref="Concurrency"/> simultaneous decodes.</summary>
    [Benchmark]
    public async Task<long> ConcurrentReadToEnd()
    {
        var tasks = new Task<long>[Concurrency];
        for (int i = 0; i < tasks.Length; i++)
            tasks[i] = Read(Path, int.MaxValue);

        var totals = await Task.WhenAll(tasks);
        return totals.Sum();
    }

    /// <summary>
    /// One request, read through the arm's decoder until <paramref name="maxBytes"/> decoded bytes have
    /// been seen (or the stream ends). Returns the byte count so nothing can be optimized away.
    /// </summary>
    private async Task<long> Read(Arm arm, int maxBytes)
    {
        var client = arm == Arm.Framework ? frameworkClient : driverClient;
        using var response = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
        response.EnsureSuccessStatusCode();

        var raw = await response.Content.ReadAsStreamAsync();
        var body = arm == Arm.Framework && FrameworkCanDecode
            ? raw
            : DriverWrap(raw, string.Join(", ", response.Content.Headers.ContentEncoding), true);

        var buffer = ArrayPool<byte>.Shared.Rent(81920);
        try
        {
            long total = 0;
            while (total < maxBytes)
            {
                var read = await body.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, maxBytes - total)));
                if (read == 0)
                    break;
                total += read;
            }

            return total;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            if (!ReferenceEquals(body, raw))
                body.Dispose();
        }
    }

    /// <summary>Setup-only helper: the whole decoded body plus the Content-Encoding the arm observed.</summary>
    private async Task<(byte[] Bytes, string ContentEncoding)> ReadAll(Arm arm)
    {
        var client = arm == Arm.Framework ? frameworkClient : driverClient;
        using var response = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
        var encoding = string.Join(", ", response.Content.Headers.ContentEncoding);

        var raw = await response.Content.ReadAsStreamAsync();
        var body = arm == Arm.Framework && FrameworkCanDecode ? raw : DriverWrap(raw, encoding, true);
        try
        {
            using var decoded = new MemoryStream();
            await body.CopyToAsync(decoded);
            return (decoded.ToArray(), encoding);
        }
        finally
        {
            if (!ReferenceEquals(body, raw))
                body.Dispose();
        }
    }

    private static Func<Stream, string, bool, Stream> BindDriverWrap()
    {
        var type = typeof(ClickHouseClient).Assembly.GetType("ClickHouse.Driver.Http.ResponseDecompression", throwOnError: true);
        var method = type.GetMethod(
            "Wrap",
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            new[] { typeof(Stream), typeof(string), typeof(bool) },
            modifiers: null)
            ?? throw new MissingMethodException("ResponseDecompression.Wrap(Stream, string, bool) not found.");

        return method.CreateDelegate<Func<Stream, string, bool, Stream>>();
    }

    /// <summary>TSV rows shaped like a ClickHouse result set, sized to roughly <paramref name="targetBytes"/>.</summary>
    private static byte[] MakeTabularPayload(int targetBytes)
    {
        var builder = new StringBuilder(targetBytes + 128);
        for (int i = 0; builder.Length < targetBytes; i++)
        {
            builder.Append(i).Append('\t')
                .Append("row-").Append(i).Append('\t')
                .Append("us-east-1").Append('\t')
                .Append(i * 0.5).Append('\t')
                .Append("2024-01-01 00:00:").Append((i % 60).ToString("00")).Append('\t')
                .Append("event=purchase;status=ok;session=").Append(((ulong)i * 6364136223846793005UL).ToString("x"))
                .Append('\n');
        }

        return Encoding.UTF8.GetBytes(builder.ToString(0, targetBytes));
    }

    private static string Token(PayloadCodec codec) => codec switch
    {
        PayloadCodec.Gzip => "gzip",
#if ZSTD_AVAILABLE
        PayloadCodec.Zstd => "zstd",
#endif
        _ => "deflate",
    };

    private static byte[] Compress(byte[] source, PayloadCodec codec)
    {
        using var buffer = new MemoryStream();
        using (var encoder = CreateEncoder(buffer, codec))
        {
            encoder.Write(source, 0, source.Length);
        }

        return buffer.ToArray();
    }

    private static Stream CreateEncoder(Stream destination, PayloadCodec codec) => codec switch
    {
        PayloadCodec.Gzip => new GZipStream(destination, CompressionLevel.Fastest, leaveOpen: true),
#if ZSTD_AVAILABLE
        // The driver's own codec, so the body is exactly what the server's zstd writer produces.
        PayloadCodec.Zstd => ClickHouse.Driver.Compression.ZstdCompressor.Default.Compress(destination, leaveOpen: true),
#endif
        _ => new ZLibStream(destination, CompressionLevel.Fastest, leaveOpen: true),
    };

    private static byte[] BuildResponse(byte[] body, string contentEncoding)
    {
        var head = Encoding.ASCII.GetBytes(
            "HTTP/1.1 200 OK\r\n" +
            "Content-Type: text/tab-separated-values; charset=UTF-8\r\n" +
            $"Content-Encoding: {contentEncoding}\r\n" +
            $"Content-Length: {body.Length}\r\n" +
            "Connection: keep-alive\r\n\r\n");

        var response = new byte[head.Length + body.Length];
        head.CopyTo(response, 0);
        body.CopyTo(response, head.Length);
        return response;
    }

    /// <summary>
    /// Minimal keep-alive HTTP/1.1 server that replays one precomputed response for any request. It does
    /// no parsing beyond finding the end of the request head and no per-request allocation of the body,
    /// so server-side cost is a socket write and is identical for both arms.
    /// </summary>
    private sealed class LoopbackServer : IDisposable
    {
        private static readonly byte[] HeadTerminator = Encoding.ASCII.GetBytes("\r\n\r\n");

        private readonly TcpListener listener;
        private readonly byte[] response;
        private readonly CancellationTokenSource cts = new();

        public LoopbackServer(byte[] response)
        {
            this.response = response;
            listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            Port = ((IPEndPoint)listener.LocalEndpoint).Port;
            _ = Task.Run(AcceptLoop);
        }

        public int Port { get; }

        public void Dispose()
        {
            cts.Cancel();
            listener.Stop();
            cts.Dispose();
        }

        private async Task AcceptLoop()
        {
            while (!cts.IsCancellationRequested)
            {
                TcpClient connection;
                try
                {
                    connection = await listener.AcceptTcpClientAsync(cts.Token);
                }
                catch
                {
                    return;
                }

                _ = Task.Run(() => Serve(connection));
            }
        }

        private async Task Serve(TcpClient connection)
        {
            using (connection)
            {
                connection.NoDelay = true;
                var stream = connection.GetStream();
                var buffer = new byte[4096];
                try
                {
                    // Requests are bodyless GETs, so "head fully received" is the only framing needed.
                    // A client that walks away mid-body (the time-to-first-row case) surfaces here as a
                    // write/read failure, which just ends this connection.
                    while (!cts.IsCancellationRequested)
                    {
                        int matched = 0;
                        while (matched < HeadTerminator.Length)
                        {
                            var read = await stream.ReadAsync(buffer, cts.Token);
                            if (read == 0)
                                return;

                            for (int i = 0; i < read; i++)
                                matched = buffer[i] == HeadTerminator[matched] ? matched + 1 : (buffer[i] == HeadTerminator[0] ? 1 : 0);
                        }

                        await stream.WriteAsync(response, cts.Token);
                    }
                }
                catch
                {
                    // Client hung up or the server is shutting down; nothing to report.
                }
            }
        }
    }
}
