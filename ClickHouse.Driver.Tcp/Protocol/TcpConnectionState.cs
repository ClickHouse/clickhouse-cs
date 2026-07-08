namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// The lifecycle state of a single raw connection. A connection advances Handshaking → Ready, then cycles
/// Ready → ReadingResponse → Ready for each request/response exchange, and moves to Terminated on close,
/// cancellation, or any transport or protocol failure. Terminated is final: such a connection is discarded,
/// never returned to the pool.
/// </summary>
internal enum TcpConnectionState
{
    /// <summary>The initial handshake exchange is in progress; no request may be sent yet.</summary>
    Handshaking,

    /// <summary>Idle and owned by no operation; ready to send the next request.</summary>
    Ready,

    /// <summary>A request has been sent and the server's reply is being read.</summary>
    ReadingResponse,

    /// <summary>Closed or failed. Final — the connection is discarded, never reused.</summary>
    Terminated,
}
