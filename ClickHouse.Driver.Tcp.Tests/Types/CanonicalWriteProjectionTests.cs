using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class CanonicalWriteProjectionTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    private static IEnumerable<TestCaseData> Cases()
    {
        long ticks = new DateTime(2024, 1, 15, 12, 0, 0).Ticks;
        yield return Case(
            "DateTime('America/New_York')",
            new ArrayColumn<DateTime>("c", "DateTime('America/New_York')", new[]
            {
                new DateTime(ticks, DateTimeKind.Utc),
                new DateTime(ticks, DateTimeKind.Unspecified),
            }));
        yield return Case(
            "DateTime64(3, 'UTC')",
            new ArrayColumn<DateTimeOffset>("c", "DateTime64(3, 'UTC')", new[]
            {
                DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_001),
                DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_999),
            }));
        yield return Case("Time", new ArrayColumn<TimeSpan>("c", "Time", new[] { TimeSpan.Zero, TimeSpan.FromTicks(19_999_999) }));
        yield return Case("Time64(3)", new ArrayColumn<TimeSpan>("c", "Time64(3)", new[] { TimeSpan.Zero, TimeSpan.FromTicks(19_999) }));
        yield return Case("Float32", new ArrayColumn<float>("c", "Float32", new[] { 0f, -0f, float.NaN }));
        yield return Case("Float64", new ArrayColumn<double>("c", "Float64", new[] { 0d, -0d, double.NaN }));
        yield return Case("BFloat16", new ArrayColumn<float>("c", "BFloat16", new[] { 0f, -0f, 1.0001f }));
        yield return Case("FixedString(4)", new ArrayColumn<byte[]>("c", "FixedString(4)", new[]
        {
            new byte[] { 1, 2, 3, 4 },
            new byte[] { 4, 3, 2, 1 },
        }));
        yield return Case("IPv4", new ArrayColumn<IPAddress>("c", "IPv4", new[]
        {
            IPAddress.Parse("192.0.2.1"),
            IPAddress.Parse("203.0.113.2"),
        }));
        yield return Case("IPv6", new ArrayColumn<IPAddress>("c", "IPv6", new[]
        {
            IPAddress.Parse("2001:db8::1"),
            IPAddress.Parse("192.0.2.1"),
        }));
    }

    private static TestCaseData Case(string type, IColumn column)
        => new TestCaseData(type, column).SetName($"CanonicalWriteProjection({type})");

    [TestCaseSource(nameof(Cases))]
    public async Task WriteCanonicalColumn_ErgonomicSource_ProducesTheSameBytes(string type, IColumn source)
    {
        IColumnCodec codec = Resolve(type);
        IColumn canonical = codec.ToCanonicalWriteColumn(source);

        byte[] direct = await CodecTestHarness.WriteAsync(writer => codec.WriteColumn(writer, source));
        byte[] projected = await CodecTestHarness.WriteAsync(writer =>
            codec.WriteCanonicalColumn(writer, canonical, 0, canonical.RowCount));

        Assert.Multiple(() =>
        {
            Assert.That(canonical.ElementType, Is.EqualTo(codec.CanonicalWriteElementType));
            Assert.That(projected, Is.EqualTo(direct));
        });
    }
}
