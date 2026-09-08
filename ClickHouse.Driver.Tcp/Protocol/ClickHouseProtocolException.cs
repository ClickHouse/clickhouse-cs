using System;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Thrown when the server's response violates the protocol for the current connection state — an unexpected
/// packet type, or a packet arriving at a point in the exchange where it is not allowed. Distinct from
/// <see cref="ClickHouseServerException"/> (an error the server deliberately reported). A connection that
/// raises this is terminated and never reused.
/// </summary>
internal sealed class ClickHouseProtocolException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="ClickHouseProtocolException"/> class.</summary>
    /// <param name="message">A description of the protocol violation.</param>
    public ClickHouseProtocolException(string message)
        : base(message)
    {
    }
}
