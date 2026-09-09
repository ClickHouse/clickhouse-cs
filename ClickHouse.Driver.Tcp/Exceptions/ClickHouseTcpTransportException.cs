using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Reports a socket, TLS, or established-connection failure and retains its cause as the inner exception. The
/// connection is discarded. An interrupted insert has an unknown outcome; retry it only when the original attempt
/// and every retry use the same deduplication token.
/// </summary>
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
