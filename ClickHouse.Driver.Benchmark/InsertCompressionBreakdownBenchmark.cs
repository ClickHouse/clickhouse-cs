using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Decomposes the binary-insert compression tradeoff into its two competing costs, measured
/// independently so the end-to-end numbers from <see cref="BinaryInsertCompressionBenchmark"/> can be
/// attributed: <b>client-side compression CPU</b> vs <b>time on the wire</b>.
///
/// A real RowBinary payload is built once (via the public <see cref="ExtendedBinaryWriter"/>) and, from
/// it, pre-compressed buffers. Then:
///  - <see cref="Compress"/> runs the codec in-memory (no network) — pure compression CPU.
///  - <see cref="Wire"/> uploads a pre-built buffer via <c>PostStreamAsync</c>, which sets
///    <c>Content-Encoding</c> without re-compressing — so it measures wire+server for that exact
///    payload size, with no compression work overlapping the send.
///
/// The decision reduces to: is <c>Compress</c> time less than <c>Wire[Raw] - Wire[GzipFastest]</c>
/// (the wire time compression buys back)? Expected: yes on cloud (wire dominates), no on loopback.
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
    }

    private const string Table = "default.benchmark_compression_breakdown";
    private const string InsertSql =
        "INSERT INTO " + Table + " (Id, Name, Value, Payload) FORMAT RowBinary";

    private ClickHouseClient client;

    private byte[] rawBytes;
    private byte[] gzipFastestBytes;
    private byte[] gzipOptimalBytes;

    [Params(500_000)]
    public int Count { get; set; }

    // For Wire: which pre-built payload to upload. For Compress: which level to run (Raw is ignored).
    [Params(Payload.Raw, Payload.GzipFastest, Payload.GzipOptimal)]
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
        gzipFastestBytes = GzipTo(rawBytes, CompressionLevel.Fastest);
        gzipOptimalBytes = GzipTo(rawBytes, CompressionLevel.Optimal);

        Console.WriteLine(
            $"[breakdown] payload sizes: raw={rawBytes.Length / 1024.0 / 1024:F1} MiB, " +
            $"gzipFastest={gzipFastestBytes.Length / 1024.0 / 1024:F1} MiB " +
            $"({100.0 * gzipFastestBytes.Length / rawBytes.Length:F1}%), " +
            $"gzipOptimal={gzipOptimalBytes.Length / 1024.0 / 1024:F1} MiB " +
            $"({100.0 * gzipOptimalBytes.Length / rawBytes.Length:F1}%)");
    }

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>Client-side compression CPU only — compress the raw buffer in-memory, discard output.</summary>
    [Benchmark]
    public void Compress()
    {
        var level = Kind == Payload.GzipOptimal ? CompressionLevel.Optimal : CompressionLevel.Fastest;
        var compressor = new GZipCompressor(level);
        using var sink = compressor.Compress(Stream.Null, leaveOpen: true);
        sink.Write(rawBytes, 0, rawBytes.Length);
    }

    /// <summary>Wire + server time for a given pre-built payload — no compression work in the measured region.</summary>
    [Benchmark]
    public async Task Wire()
    {
        var (bytes, compressed) = Kind switch
        {
            Payload.Raw => (rawBytes, false),
            Payload.GzipFastest => (gzipFastestBytes, true),
            Payload.GzipOptimal => (gzipOptimalBytes, true),
            _ => throw new ArgumentOutOfRangeException(),
        };

        using var body = new MemoryStream(bytes, writable: false);
        using var response = await client.PostStreamAsync(InsertSql, body, compressed, CancellationToken.None);
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

    private static byte[] GzipTo(byte[] data, CompressionLevel level)
    {
        using var ms = new MemoryStream();
        using (var gz = new GZipStream(ms, level, leaveOpen: true))
            gz.Write(data, 0, data.Length);
        return ms.ToArray();
    }

    private static string MakeName(int i)
    {
        uint h = unchecked((uint)i * 2654435761u);
        return $"row-{i}-{h & 0xffffff:x6}";
    }

    private static string MakePayload(int i)
        => $"event=purchase;status=ok;region=us-east-1;user_id={i};session={((ulong)i * 6364136223846793005UL):x}";
}
