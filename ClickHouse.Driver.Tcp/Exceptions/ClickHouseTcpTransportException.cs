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
