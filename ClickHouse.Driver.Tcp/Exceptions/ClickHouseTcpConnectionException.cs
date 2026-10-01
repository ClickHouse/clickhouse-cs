using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Reports a socket, TLS, or established-connection failure, or an expired DialTimeout or ReadTimeout, and retains
/// its cause as the inner exception (a <see cref="TimeoutException"/> for a timeout). The connection is discarded.
/// An interrupted insert has an unknown outcome; retry it only when the original attempt and every retry use the
/// same deduplication token.
/// </summary>
public sealed class ClickHouseTcpConnectionException : ClickHouseTcpException
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpConnectionException"/> class.</summary>
    /// <param name="message">What the client was doing when the connection failed.</param>
    /// <param name="innerException">The failure the runtime raised.</param>
    public ClickHouseTcpConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
