namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// The category names the CI workflows filter on with <c>--anyCategories</c>.
/// </summary>
/// <remarks>
/// <para>
/// <c>benchmark-compare.yml</c> picks the set from the files a PR changed: a change under
/// <c>ClickHouse.Driver.Tcp/</c> selects <see cref="TcpRegression"/>, one under
/// <c>ClickHouse.Driver/</c> selects <see cref="HttpRegression"/>, and one under
/// <c>ClickHouse.Driver.Common/</c> selects both plus <see cref="Compression"/>. When both
/// transports move, <see cref="Cross"/> is added.
/// </para>
/// <para>
/// A benchmark class with no category matches no <c>--anyCategories</c> run, so it would appear
/// only in the nightly full sweep. <c>Program</c> fails the run rather than let that pass silently.
/// </para>
/// </remarks>
public static class BenchmarkCategories
{
    /// <summary>HTTP transport, standing throughput and allocation coverage.</summary>
    public const string HttpRegression = "http-regression";

    /// <summary>Native TCP transport, standing throughput and allocation coverage.</summary>
    public const string TcpRegression = "tcp-regression";

    /// <summary>Runs the same workload over both transports in one process.</summary>
    public const string Cross = "cross";

    /// <summary>HTTP, pinned to one issue or one past optimization.</summary>
    public const string HttpInvestigation = "http-investigation";

    /// <summary>Native TCP, pinned to one issue or one past optimization.</summary>
    public const string TcpInvestigation = "tcp-investigation";

    /// <summary>Compression codecs and framing, shared by both transports.</summary>
    public const string Compression = "compression";
}
