using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Diagnostic;
using ClickHouse.Driver.Tcp.Logging;
using ClickHouse.Driver.Tcp.Protocol;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// Opens connections for the pool: dial, handshake, and whatever else a connection needs before it can carry an
/// operation. A seam rather than a direct call so the pool's own behaviour — queueing, reuse, retirement — can
/// be exercised over connections that need no server.
/// </summary>
/// <remarks>
/// Disposable because a factory may hold resources for the client's whole life — the TLS certificate authorities
/// are the case that exists today. The pool disposes it at the very end of its teardown, once no connection can
/// still be handshaking against them.
/// </remarks>
internal interface IConnectionFactory : IDisposable
{
    /// <summary>Opens a connection and returns it Ready. On failure nothing is left open.</summary>
    /// <param name="cancellationToken">A token to observe while connecting.</param>
    /// <returns>A connected, handshaken connection.</returns>
    ValueTask<ClickHouseTcpConnection> CreateAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Opens real connections to the configured endpoint, bounding connect plus handshake with
/// <see cref="ClickHouseTcpClientOptions.DialTimeout"/>.
/// </summary>
internal sealed class TcpConnectionFactory : IConnectionFactory
{
    private readonly ClickHouseTcpClientOptions options;
    private readonly TlsParameters tls;
    private readonly ILogger logger;

    /// <summary>
    /// Initializes the factory over the client's validated options, resolving the TLS configuration once. A
    /// certificate authority file is read here, so a missing or malformed one fails at client construction
    /// rather than on every connect.
    /// </summary>
    /// <param name="options">The endpoint, credentials, TLS configuration, and dial timeout to open connections with.</param>
    internal TcpConnectionFactory(ClickHouseTcpClientOptions options)
    {
        this.options = options;
        tls = BuildTlsParameters(options);
        logger = options.LoggerFactory?.CreateLogger(ClickHouseTcpDiagnostics.ConnectionLogCategory);
    }

    /// <inheritdoc/>
    public async ValueTask<ClickHouseTcpConnection> CreateAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(options.DialTimeout);

        // One span over the socket connect, the TLS negotiation and the handshake, which is what a slow first
        // operation is usually waiting on.
        using Activity activity = TcpActivity.StartConnect(options);
        long startedAt = Stopwatch.GetTimestamp();
        if (logger is not null)
        {
            ConnectionLog.Opening(logger, options.Host, options.ResolvedPort, options.Username, options.UseTls);
        }

        try
        {
            ClickHouseTcpConnection connection = await ClickHouseTcpConnection.ConnectAsync(
                options.Host, options.ResolvedPort, options.ToHandshakeParameters(), tls, linked.Token, options.Compressor).ConfigureAwait(false);

            activity?.SetSuccess();
            if (logger is not null)
            {
                ServerHandshake server = connection.Server;
                ConnectionLog.Opened(
                    logger,
                    server.ServerName,
                    server.VersionMajor,
                    server.VersionMinor,
                    server.VersionPatch,
                    Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds,
                    server.Revision,
                    server.Timezone);
            }

            return connection;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && linked.IsCancellationRequested)
        {
            // The linked token, not the caller's, fired: the dial deadline elapsed. Surface it as a timeout so a
            // hung connect is distinguishable from a caller cancellation. The deadline covers the TLS handshake
            // too, which is one more round trip a wedged server can stall on.
            var timeout = new TimeoutException(
                $"Connecting to {options.Host}:{options.ResolvedPort} timed out after {options.DialTimeout.TotalSeconds:0.###}s (DialTimeout).");
            ReportFailure(activity, startedAt, timeout);
            throw timeout;
        }
        catch (OperationCanceledException e)
        {
            // The pool dials on its shutdown token, so a client being disposed cancels any top-up in flight.
            // That is disposal working, not a server that cannot be reached, and it is not worth a warning.
            activity?.SetError(e);
            if (logger is not null)
            {
                ConnectionLog.OpenCancelled(logger, options.Host, options.ResolvedPort, Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds);
            }

            throw;
        }
        catch (Exception e)
        {
            ReportFailure(activity, startedAt, e);
            throw;
        }
    }

    // Marks the connect span failed and logs it. A dial the pool started in the background has no caller to
    // report to, so without this line it fails invisibly.
    private void ReportFailure(Activity activity, long startedAt, Exception exception)
    {
        activity?.SetError(exception);
        if (logger is not null)
        {
            ConnectionLog.OpenFailed(logger, options.Host, options.ResolvedPort, Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds, exception);
        }
    }

    /// <summary>
    /// Releases the TLS configuration this factory built, and with it the certificate authorities it loaded. Safe
    /// to call more than once, and a no-op for a plaintext client, which built none.
    /// </summary>
    public void Dispose() => tls?.Dispose();

    /// <summary>The TLS configuration these options describe, or null when the client connects in the clear.</summary>
    /// <param name="options">The client's validated options.</param>
    /// <returns>The parameters a connect uses to wrap its socket, or null.</returns>
    private static TlsParameters BuildTlsParameters(ClickHouseTcpClientOptions options)
        => options.UseTls
            ? new TlsParameters
            {
                // The certificate names the server, which is Host unless the caller says the certificate
                // names something else (an internal alias, or a Host given as an address).
                TargetHost = string.IsNullOrEmpty(options.TlsServerName) ? options.Host : options.TlsServerName,
                AllowInvalidCertificates = options.TlsAllowInvalidCertificates,
                CaCertificates = options.TlsCaCertificatePath is null
                    ? null
                    : TlsParameters.LoadCaCertificates(options.TlsCaCertificatePath),
                Configure = options.ConfigureTls,
            }
            : null;
}
