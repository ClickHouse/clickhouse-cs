namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The names a tracing exporter or a logging filter needs to pick this client out.
/// </summary>
/// <example>
/// <code>
/// Sdk.CreateTracerProviderBuilder()
///    .AddSource(ClickHouseTcpDiagnostics.ActivitySourceName)
///    .AddConsoleExporter()
///    .Build();
/// </code>
/// </example>
public static class ClickHouseTcpDiagnostics
{
    /// <summary>
    /// The <see cref="System.Diagnostics.ActivitySource"/> name the native-protocol client emits spans under.
    /// It is separate from the HTTP transport's source, so either can be collected on its own.
    /// </summary>
    public const string ActivitySourceName = "ClickHouse.Driver.Tcp";

    /// <summary>
    /// The logger category for operations run through the client or a session: what ran, how long it took, and
    /// how it ended. The statement text appears here at <c>Debug</c>.
    /// </summary>
    public const string ClientLogCategory = "ClickHouse.Driver.Tcp.Client";

    /// <summary>
    /// The logger category for opening a connection: the dial, the TLS negotiation, and the handshake result.
    /// </summary>
    public const string ConnectionLogCategory = "ClickHouse.Driver.Tcp.Connection";

    /// <summary>
    /// The logger category for the connection pool: checkouts, retirement, exhaustion, and the background work
    /// no caller is awaiting — a failed top-up dial or sweep is reported nowhere else.
    /// </summary>
    public const string PoolLogCategory = "ClickHouse.Driver.Tcp.Pool";
}
