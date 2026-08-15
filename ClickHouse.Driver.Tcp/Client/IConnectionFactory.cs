using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Client;

/// <summary>
/// Opens connections for the pool: dial, handshake, and whatever else a connection needs before it can carry an
/// operation. A seam rather than a direct call so the pool's own behaviour — queueing, reuse, retirement — can
/// be exercised over connections that need no server.
/// </summary>
internal interface IConnectionFactory
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

    /// <summary>Initializes the factory over the client's validated options.</summary>
    /// <param name="options">The endpoint, credentials, and dial timeout to open connections with.</param>
    internal TcpConnectionFactory(ClickHouseTcpClientOptions options)
    {
        this.options = options;
    }

    /// <inheritdoc/>
    public async ValueTask<ClickHouseTcpConnection> CreateAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(options.DialTimeout);
        try
        {
            return await ClickHouseTcpConnection.ConnectAsync(
                options.Host, options.Port, options.ToHandshakeParameters(), linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && linked.IsCancellationRequested)
        {
            // The linked token, not the caller's, fired: the dial deadline elapsed. Surface it as a timeout so a
            // hung connect is distinguishable from a caller cancellation.
            throw new TimeoutException(
                $"Connecting to {options.Host}:{options.Port} timed out after {options.DialTimeout.TotalSeconds:0.###}s (DialTimeout).");
        }
    }
}
