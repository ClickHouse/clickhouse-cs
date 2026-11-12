using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A loopback TLS endpoint that forwards each connection to a plaintext ClickHouse server.
/// </summary>
/// <remarks>
/// Lets the integration suite exercise the client's <see cref="SslStream"/> path without reconfiguring the
/// target server. It records the TLS server name (SNI) of each connection, and, when asked, the plaintext the client
/// sent on it, so a test can tell which connection carried which query.
/// </remarks>
internal sealed class TlsTerminatingProxy : IAsyncDisposable
{
    private readonly TcpListener listener;
    private readonly X509Certificate2 certificate;
    private readonly string targetHost;
    private readonly int targetPort;
    private readonly CancellationTokenSource shutdown = new();
    private readonly Task accepting;
    private readonly bool recordClientBytes;
    private readonly ConcurrentQueue<ProxiedConnection> connections = new();

    /// <summary>Starts listening, and connects to the target only when a client arrives.</summary>
    /// <param name="targetHost">The plaintext server's host.</param>
    /// <param name="targetPort">The plaintext server's native port.</param>
    /// <param name="certificate">The certificate to present, with its private key.</param>
    /// <param name="recordClientBytes">Whether to keep the plaintext each client sends. Off by default.</param>
    internal TlsTerminatingProxy(string targetHost, int targetPort, X509Certificate2 certificate, bool recordClientBytes = false)
    {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.certificate = certificate;
        this.recordClientBytes = recordClientBytes;

        listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Port = ((IPEndPoint)listener.LocalEndpoint).Port;
        accepting = AcceptAsync();
    }

    /// <summary>The loopback port a TLS client connects to.</summary>
    internal int Port { get; }

    /// <summary>Every connection that completed the TLS handshake, in the order they did.</summary>
    internal IReadOnlyCollection<ProxiedConnection> Connections => connections.ToArray();

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

                // Serve pool connections concurrently for their full client-owned lifetime.
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

                // On the server side this is the SNI the client sent.
                var connection = new ProxiedConnection(tunnel.TargetHostName, recordClientBytes);
                connections.Enqueue(connection);

                await upstream.ConnectAsync(targetHost, targetPort, shutdown.Token).ConfigureAwait(false);
                upstream.NoDelay = true;
                NetworkStream plaintext = upstream.GetStream();

                // Closing either direction ends the connection and unblocks the other copy.
                Task toServer = recordClientBytes
                    ? CopyRecordingAsync(tunnel, plaintext, connection, shutdown.Token)
                    : tunnel.CopyToAsync(plaintext, shutdown.Token);
                Task toClient = plaintext.CopyToAsync(tunnel, shutdown.Token);
                await Task.WhenAny(toServer, toClient).ConfigureAwait(false);
            }
        }
        catch (Exception e) when (IsExpectedTeardown(e))
        {
        }
    }

    private static async Task CopyRecordingAsync(Stream from, Stream to, ProxiedConnection connection, CancellationToken cancellationToken)
    {
        var buffer = new byte[81920];
        int read;
        while ((read = await from.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
        {
            connection.Record(buffer.AsSpan(0, read));
            await to.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
        }
    }

    // Client disconnects and proxy disposal are normal pump termination.
    private static bool IsExpectedTeardown(Exception e)
        => e is OperationCanceledException or IOException or SocketException or ObjectDisposedException
            or AuthenticationException or InvalidOperationException;
}

/// <summary>One connection the proxy accepted.</summary>
internal sealed class ProxiedConnection
{
    private readonly MemoryStream sent;
    private readonly object gate = new();

    internal ProxiedConnection(string serverName, bool recordClientBytes)
    {
        ServerName = serverName;
        sent = recordClientBytes ? new MemoryStream() : null;
    }

    /// <summary>The TLS server name (SNI) the client sent, or empty when it sent none.</summary>
    internal string ServerName { get; }

    /// <summary>
    /// Whether the client sent <paramref name="text"/> on this connection. The native protocol sends a query id and the
    /// query text uncompressed, so either one identifies the connection a query ran on.
    /// </summary>
    /// <param name="text">The text to look for, as UTF-8.</param>
    /// <returns>True when the plaintext the client sent contains it.</returns>
    /// <exception cref="InvalidOperationException">The proxy was not asked to record client bytes.</exception>
    internal bool Sent(string text)
    {
        if (sent is null)
        {
            throw new InvalidOperationException("The proxy does not record client bytes. Construct it with recordClientBytes: true.");
        }

        byte[] needle = Encoding.UTF8.GetBytes(text);
        lock (gate)
        {
            return sent.GetBuffer().AsSpan(0, (int)sent.Length).IndexOf(needle) >= 0;
        }
    }

    internal void Record(ReadOnlySpan<byte> bytes)
    {
        lock (gate)
        {
            sent.Write(bytes);
        }
    }
}
