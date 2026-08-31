using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The connection to the server failed: the socket could not be opened, TLS could not be negotiated, or
/// an established connection broke while the client was reading or writing.
/// </summary>
/// <remarks>
/// <para>
/// The cause the runtime raised is kept as <see cref="Exception.InnerException"/> — usually a
/// <see cref="System.Net.Sockets.SocketException"/>, an <see cref="System.IO.IOException"/>, an
/// <see cref="System.IO.EndOfStreamException"/> when the server closed the connection mid-message, or a
/// <see cref="System.Security.Authentication.AuthenticationException"/> for a TLS failure. Match on that
/// when the distinction matters.
/// </para>
/// <para>
/// The connection is terminated and never reused.
/// </para>
/// <para>
/// <b>An insert that ends this way has an unknown outcome</b>: the rows may have been applied before the
/// connection broke, or not at all, and nothing the client can read says which. So retrying one duplicates the
/// rows as often as it succeeds. Set <see cref="ClickHouseTcpInsertOptions.DeduplicationToken"/> to make the
/// retry safe — the server drops a second attempt carrying a token it has already seen.
/// </para>
/// </remarks>
public sealed class ClickHouseTcpTransportException : ClickHouseTcpException
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpTransportException"/> class.</summary>
    /// <param name="message">What the client was doing when the connection failed.</param>
    /// <param name="innerException">The failure the runtime raised.</param>
    public ClickHouseTcpTransportException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
