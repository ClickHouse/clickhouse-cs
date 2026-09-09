using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

// The OpenTelemetry field of ClientInfo, asserted byte-for-byte. Only a unit test can pin the encoding: a server
// round-trip proves the server accepted *something*, and the byte order of the two ids is exactly what a
// self-consistent client would get wrong invisibly. The end-to-end check that the server reads the same trace id
// we sent lives in ClickHouseTcpTracingIntegrationTests.
[TestFixture]
public class ClientInfoTraceContextTests
{
    // Chosen so every byte is distinct: a reversal, a half-swap and a straight copy all produce different output.
    private const string TraceIdHex = "000102030405060708090a0b0c0d0e0f";

    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task WriteTraceContext_NoActivity_WritesTheAbsentForm()
    {
        byte[] written = await WriteAsync(null);

        Assert.That(written, Is.EqualTo(new byte[] { 0 }), "has_trace = 0 and nothing after it");
    }

    [Test]
    public async Task WriteTraceContext_HierarchicalActivity_WritesTheAbsentForm()
    {
        // A hierarchical id has no trace id to send, so there is nothing to propagate even though a span exists.
        using var activity = new Activity("test");
        activity.SetIdFormat(ActivityIdFormat.Hierarchical);
        activity.Start();

        byte[] written = await WriteAsync(activity);

        Assert.That(written, Is.EqualTo(new byte[] { 0 }));
    }

    [Test]
    public async Task WriteTraceContext_W3CActivity_WritesEachTraceIdHalfLittleEndian()
    {
        using Activity activity = StartW3C(traceState: null);

        byte[] written = await WriteAsync(activity);

        // The trace id is a UUID on the wire: the high half then the low half, each little-endian. So the 16
        // big-endian W3C bytes 00..0f come back as two separately reversed runs.
        Assert.That(
            written[1..17],
            Is.EqualTo(new byte[] { 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, 0x08 }));
        Assert.That(written[0], Is.EqualTo(1), "has_trace = 1");
    }

    [Test]
    public async Task WriteTraceContext_W3CActivity_WritesSpanIdAsLittleEndianUInt64()
    {
        using Activity activity = StartW3C(traceState: null);

        byte[] written = await WriteAsync(activity);

        // Read back as the UInt64 the server decodes, and compared against the hex id read as one big-endian
        // number — an independent formulation of the same claim, rather than a copy of the writer's steps.
        ulong onWire = BinaryPrimitives.ReadUInt64LittleEndian(written[17..25]);
        ulong expected = ulong.Parse(activity.SpanId.ToHexString(), NumberStyles.HexNumber, CultureInfo.InvariantCulture);

        Assert.That(onWire, Is.EqualTo(expected));
    }

    [Test]
    public async Task WriteTraceContext_W3CActivityWithTraceState_WritesStateThenFlags()
    {
        using Activity activity = StartW3C(traceState: "vendor=abc");

        byte[] written = await WriteAsync(activity);

        // A length-prefixed string, then the flags byte. Recorded is 1.
        Assert.Multiple(() =>
        {
            Assert.That(written[25], Is.EqualTo(10), "the trace state's length prefix");
            Assert.That(System.Text.Encoding.UTF8.GetString(written[26..36]), Is.EqualTo("vendor=abc"));
            Assert.That(written[36], Is.EqualTo((byte)ActivityTraceFlags.Recorded));
            Assert.That(written, Has.Length.EqualTo(37), "nothing follows the flags");
        });
    }

    [Test]
    public async Task WriteTraceContext_W3CActivityWithoutTraceState_WritesAnEmptyString()
    {
        using Activity activity = StartW3C(traceState: null);

        byte[] written = await WriteAsync(activity);

        Assert.Multiple(() =>
        {
            Assert.That(written[25], Is.EqualTo(0), "an empty trace state is a zero length prefix");
            Assert.That(written, Has.Length.EqualTo(27));
        });
    }

    private static Activity StartW3C(string traceState)
    {
        var activity = new Activity("test");
        activity.SetIdFormat(ActivityIdFormat.W3C);
        activity.SetParentId(
            ActivityTraceId.CreateFromString(TraceIdHex),
            ActivitySpanId.CreateFromString("1011121314151617"),
            ActivityTraceFlags.Recorded);
        if (traceState is not null)
        {
            activity.TraceStateString = traceState;
        }

        activity.Start();
        return activity;
    }

    private static async Task<byte[]> WriteAsync(Activity activity)
    {
        using var stream = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(stream))
        {
            ClientInfo.WriteTraceContext(writer, activity);
            await writer.FlushAsync(None);
        }

        return stream.ToArray();
    }
}
