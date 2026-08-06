using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Http;

/// <summary>
/// Asserts <c>Accept-Encoding</c> as it appears <b>on the wire</b>, i.e. after the driver's own
/// <c>SocketsHttpHandler</c> has processed the request — not as it appears on the
/// <c>HttpRequestMessage</c> the driver hands over.
/// <para>
/// The distinction is the whole point of this fixture. <c>AutomaticDecompression</c> is not only a
/// response-side setting: at send time the handler also <i>adds</i> every algorithm in its mask that is
/// missing from <c>Accept-Encoding</c>. While the driver's handler used <c>GZip | Deflate</c>, an
/// explicit <c>identity</c> left as <c>identity, gzip, deflate</c> and <c>deflate</c> as
/// <c>deflate, gzip</c>; ClickHouse resolved those by its own fixed preference order and answered
/// <c>gzip</c>, which the handler decoded and stripped — so an exact codec choice was silently
/// overridden and the driver could not even see it happen. Tests that inspect an
/// <c>HttpRequestMessage</c> through a stub handler cannot catch that, because the mutation happens
/// below them.
/// </para>
/// <para>
/// These tests therefore talk to a loopback <see cref="TcpListener"/> and read the literal request
/// bytes. No ClickHouse server is involved: what is under test is the header the driver plus the
/// framework produce, which is exactly where the defect lived.
/// </para>
/// </summary>
[TestFixture]
public class AcceptEncodingWireTests
{
    [TestCase("identity", ExpectedResult = "identity")]
    [TestCase("deflate", ExpectedResult = "deflate")]
    [TestCase("gzip", ExpectedResult = "gzip")]
    [TestCase("lz4", ExpectedResult = "lz4")]
    [TestCase("zstd", ExpectedResult = "zstd")]
    [TestCase("br", ExpectedResult = "br")]
    [TestCase("lz4, gzip", ExpectedResult = "lz4, gzip")]
    public async Task<string> ExplicitAcceptEncoding_ReachesTheServerExactly(string acceptEncoding)
        => await WireAcceptEncodingAsync(acceptEncoding: acceptEncoding);

    /// <summary>
    /// The same guarantee for the <i>per-query</i> property, which is a separate code path: the
    /// client-level value is attached before custom headers, while this one replaces whatever is there
    /// afterwards. Worth its own wire coverage rather than being assumed from the client-level cases —
    /// a stub handler cannot see the widening this fixture exists to catch, since it replaces the socket
    /// handler that performs it.
    /// </summary>
    [TestCase("identity", ExpectedResult = "identity")]
    [TestCase("deflate", ExpectedResult = "deflate")]
    [TestCase("lz4", ExpectedResult = "lz4")]
    [TestCase("br, gzip", ExpectedResult = "br, gzip")]
    public async Task<string> PerQueryAcceptEncoding_ReachesTheServerExactly(string acceptEncoding)
        => await WireAcceptEncodingAsync(perQueryAcceptEncoding: acceptEncoding);

    /// <summary>
    /// Per-query beats client-level on the wire too, and the loser leaves nothing behind: the winning
    /// codec arrives alone rather than appended to what the client-level value had already attached.
    /// </summary>
    [Test]
    public async Task PerQueryAcceptEncoding_OverridesTheClientLevelValue_OnTheWire()
    {
        var wire = await WireAcceptEncodingAsync(acceptEncoding: "gzip", perQueryAcceptEncoding: "lz4");

        Assert.That(wire, Is.EqualTo("lz4"));
    }

    /// <summary>
    /// The default advertisement, wire-verified: the codecs the driver can decode, and nothing else. The
    /// exact equality carries a second decision — <c>br</c> is absent although the driver decodes it,
    /// because ClickHouse's fixed preference scan puts brotli ahead of everything the default names, so
    /// the token's mere presence would make it the codec for every default query. Asserted on the wire
    /// because that is where the omission has to survive: a handler mask is what silently added tokens
    /// back before #490.
    /// </summary>
    [Test]
    public async Task DefaultAcceptEncoding_ReachesTheServerExactly()
    {
        var wire = await WireAcceptEncodingAsync();

        Assert.That(wire, Is.EqualTo("zstd, lz4, gzip, deflate"));
    }

    /// <summary>
    /// The uncompressed baseline, wire-verified rather than assumed: with compression off and nothing
    /// configured, no codec is offered at all, so the server has nothing to compress with. Previously the
    /// handler put <c>gzip, deflate</c> back on a request the driver had deliberately left bare.
    /// </summary>
    [Test]
    public async Task WithCompressionDisabled_NoCodecReachesTheServer()
    {
        var wire = await WireAcceptEncodingAsync(useCompression: false);

        Assert.That(wire, Is.Null, "the driver must not offer a codec it did not ask for");
    }

    /// <summary>
    /// A raw/verbatim body offers nothing: the driver does not decode it, and the handler no longer does
    /// either, so any codec advertised here would reach the caller still compressed.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_WithNothingConfigured_OffersNoCodec()
    {
        var wire = await WireAcceptEncodingAsync(
            send: static client => client.ExecuteRawResultAsync("SELECT 1 FORMAT TSV"));

        Assert.That(wire, Is.Null);
    }

    /// <summary>
    /// …but an explicitly named codec still reaches the server on the raw path, unwidened — that is how a
    /// caller exports compressed bytes on purpose.
    /// </summary>
    [Test]
    public async Task ExecuteRawResultAsync_WithAnExplicitCodec_SendsExactlyThatCodec()
    {
        var wire = await WireAcceptEncodingAsync(
            acceptEncoding: "lz4",
            send: static client => client.ExecuteRawResultAsync("SELECT 1 FORMAT TSV"));

        Assert.That(wire, Is.EqualTo("lz4"));
    }

    /// <summary>
    /// Runs one request through the driver's own handler against a loopback socket and returns the
    /// <c>Accept-Encoding</c> header value as it arrived, or <see langword="null"/> when the request
    /// carried none.
    /// </summary>
    private static async Task<string> WireAcceptEncodingAsync(
        string acceptEncoding = null,
        bool useCompression = true,
        Func<ClickHouseClient, Task> send = null,
        string perQueryAcceptEncoding = null)
    {
        using var server = new CapturingServer();
        var captured = server.CaptureOneRequestAsync();

        var settings = new ClickHouseClientSettings
        {
            Host = "127.0.0.1",
            Port = server.Port,
            Protocol = "http",
            UseCompression = useCompression,
            AcceptEncoding = acceptEncoding,
        };

        using (var client = new ClickHouseClient(settings))
        {
            if (send is null)
            {
                var options = perQueryAcceptEncoding is null
                    ? null
                    : new QueryOptions { AcceptEncoding = perQueryAcceptEncoding };
                await client.ExecuteNonQueryAsync("SELECT 1", options: options);
            }
            else
            {
                await send(client);
            }
        }

        return HeaderValue(await captured, "Accept-Encoding");
    }

    private static string HeaderValue(IReadOnlyList<string> headerLines, string name)
    {
        foreach (var line in headerLines)
        {
            var separator = line.IndexOf(':');
            if (separator > 0 &&
                line.AsSpan(0, separator).Trim().Equals(name.AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                return line.Substring(separator + 1).Trim();
            }
        }

        return null;
    }

    /// <summary>
    /// A one-shot loopback HTTP endpoint: accepts a single connection, records the request's header
    /// lines, drains the body, and answers an empty <c>200</c>.
    /// </summary>
    private sealed class CapturingServer : IDisposable
    {
        private readonly TcpListener listener;

        public CapturingServer()
        {
            listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
        }

        public ushort Port => (ushort)((IPEndPoint)listener.LocalEndpoint).Port;

        public async Task<IReadOnlyList<string>> CaptureOneRequestAsync()
        {
            using var connection = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
            using var stream = connection.GetStream();

            var (headerLines, buffered) = await ReadHeaderLinesAsync(stream).ConfigureAwait(false);
            await DrainBodyAsync(stream, headerLines, buffered).ConfigureAwait(false);

            var response = Encoding.ASCII.GetBytes(
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            await stream.WriteAsync(response, 0, response.Length).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);

            return headerLines;
        }

        /// <summary>
        /// Reads up to the blank line that ends the request headers, one byte at a time so that no part of
        /// the body is consumed. Returns the header lines plus however many body bytes were already read.
        /// </summary>
        private static async Task<(List<string> HeaderLines, int BufferedBodyBytes)> ReadHeaderLinesAsync(Stream stream)
        {
            var raw = new MemoryStream();
            var one = new byte[1];
            var consecutiveNewlines = 0;

            while (consecutiveNewlines < 2)
            {
                var read = await stream.ReadAsync(one, 0, 1).ConfigureAwait(false);
                if (read == 0)
                    break;

                raw.WriteByte(one[0]);

                if (one[0] == (byte)'\n')
                    consecutiveNewlines++;
                else if (one[0] != (byte)'\r')
                    consecutiveNewlines = 0;
            }

            var text = Encoding.ASCII.GetString(raw.ToArray());
            var lines = text.Split(new[] { "\r\n" }, StringSplitOptions.None)
                .Where(static line => line.Length > 0)
                .ToList();

            return (lines, 0);
        }

        private static async Task DrainBodyAsync(Stream stream, List<string> headerLines, int alreadyRead)
        {
            var lengthHeader = headerLines
                .FirstOrDefault(static line => line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase));
            if (lengthHeader is null)
                return;

            if (!int.TryParse(
                    lengthHeader.Substring("Content-Length:".Length).Trim(),
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out var contentLength))
            {
                return;
            }

            var remaining = contentLength - alreadyRead;
            var buffer = new byte[8192];
            while (remaining > 0)
            {
                var read = await stream.ReadAsync(buffer, 0, Math.Min(buffer.Length, remaining)).ConfigureAwait(false);
                if (read == 0)
                    break;

                remaining -= read;
            }
        }

        public void Dispose() => listener.Stop();
    }
}
