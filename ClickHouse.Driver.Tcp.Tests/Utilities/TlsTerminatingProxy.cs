using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A loopback TLS endpoint in front of a plaintext ClickHouse server: it terminates the tunnel and copies the
/// bytes on to the real server, in both directions, one connection at a time as the client opens them.
/// </summary>
/// <remarks>
/// This is how the default integration suite can do a native handshake over TLS. Enabling TLS on the server
/// itself needs a certificate and a configuration file inside the container, which the suite cannot do when
/// <c>CLICKHOUSE_TCP_HOST</c> points it at a server somebody else set up. The client side is what the driver
/// owns, and it is complete here: the real <c>SslStream</c> handshake, then every protocol packet, block and
/// insert inside the tunnel, answered by a real server.
/// </remarks>
internal sealed class TlsTerminatingProxy : IAsyncDisposable
{
    private readonly TcpListener listener;
    private readonly X509Certificate2 certificate;
    private readonly string targetHost;
    private readonly int targetPort;
    private readonly CancellationTokenSource shutdown = new();
    private readonly Task accepting;

    /// <summary>Starts listening, and connects to the target only when a client arrives.</summary>
    /// <param name="targetHost">The plaintext server's host.</param>
    /// <param name="targetPort">The plaintext server's native port.</param>
    /// <param name="certificate">The certificate to present, with its private key.</param>
    internal TlsTerminatingProxy(string targetHost, int targetPort, X509Certificate2 certificate)
    {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.certificate = certificate;

        listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Port = ((IPEndPoint)listener.LocalEndpoint).Port;
        accepting = AcceptAsync();
    }

    /// <summary>The loopback port a TLS client connects to.</summary>
    internal int Port { get; }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        shutdown.Cancel();
        listener.Stop();
        await accepting.ConfigureAwait(false);
        shutdown.Dispose();
    }

    private async Task AcceptAsync()
    {
        try
        {
            while (!shutdown.IsCancellationRequested)
            {
                TcpClient accepted = await listener.AcceptTcpClientAsync(shutdown.Token).ConfigureAwait(false);

                // Not awaited: the pool opens several connections, and each lives as long as its client keeps it.
                _ = ServeAsync(accepted);
            }
        }
        catch (Exception e) when (IsExpectedTeardown(e))
        {
        }
    }

    private async Task ServeAsync(TcpClient downstream)
    {
        try
        {
            using (downstream)
            using (var tunnel = new SslStream(downstream.GetStream(), leaveInnerStreamOpen: false))
            using (var upstream = new TcpClient())
            {
                downstream.NoDelay = true;
                await tunnel.AuthenticateAsServerAsync(
                    new SslServerAuthenticationOptions { ServerCertificate = certificate },
                    shutdown.Token).ConfigureAwait(false);

                await upstream.ConnectAsync(targetHost, targetPort, shutdown.Token).ConfigureAwait(false);
                upstream.NoDelay = true;
                NetworkStream plaintext = upstream.GetStream();

                // Whichever side stops first ends the connection, and disposal below unblocks the other copy.
                Task toServer = tunnel.CopyToAsync(plaintext, shutdown.Token);
                Task toClient = plaintext.CopyToAsync(tunnel, shutdown.Token);
                await Task.WhenAny(toServer, toClient).ConfigureAwait(false);
            }
        }
        catch (Exception e) when (IsExpectedTeardown(e))
        {
        }
    }

    // A client that drops its connection, and this proxy's own disposal, are the normal ways a pump ends.
    private static bool IsExpectedTeardown(Exception e)
        => e is OperationCanceledException or IOException or SocketException or ObjectDisposedException
            or AuthenticationException or InvalidOperationException;
}
