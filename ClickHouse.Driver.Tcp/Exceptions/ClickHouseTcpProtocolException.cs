using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The bytes on the wire did not match the protocol: an unexpected packet, a field the client cannot
/// interpret, or a declared length or geometry that contradicts what followed.
/// </summary>
/// <remarks>
/// A connection that raises this is terminated and never reused. The client cannot tell how much of the
/// stream it misread, so there is no safe point to resume from.
/// </remarks>
public sealed class ClickHouseTcpProtocolException : ClickHouseTcpException
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpProtocolException"/> class.</summary>
    /// <param name="message">What the client expected and what it found instead.</param>
    public ClickHouseTcpProtocolException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpProtocolException"/> class.</summary>
    /// <param name="message">What the client expected and what it found instead.</param>
    /// <param name="innerException">The failure that revealed it.</param>
    public ClickHouseTcpProtocolException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
