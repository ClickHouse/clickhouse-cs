using System;
using System.IO;
using System.Net.Sockets;
using System.Security.Authentication;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Turns the runtime's socket and stream failures into <see cref="ClickHouseTcpTransportException"/>.
/// </summary>
/// <remarks>
/// The client never raises those types itself, so every one of them enters through a handful of calls into
/// the socket: the two reads in <see cref="ReadBuffer"/>, the write in <see cref="ClickHouseBinaryWriter"/>,
/// and connect plus the TLS handshake in <see cref="ClickHouseTcpConnection"/>. Wrapping there keeps the
/// cause intact while giving callers one type to catch.
/// </remarks>
internal static class TransportFailure
{
    /// <summary>Whether an exception is the transport failing rather than the client misbehaving.</summary>
    /// <remarks>
    /// Two types are deliberately excluded, because both mean the client did this to itself and retrying is
    /// the wrong response. <see cref="OperationCanceledException"/> belongs to the caller's token.
    /// <see cref="ObjectDisposedException"/> from a socket or stream only happens because this process
    /// disposed it — the pool aborting a connection, a session disposing under a live read, or the caller
    /// disposing the client — never because the peer went away, which arrives as one of the types below.
    /// </remarks>
    internal static bool IsTransportFailure(Exception exception) =>
        exception is IOException or SocketException or AuthenticationException;

    /// <summary>Wraps a failure that broke a read.</summary>
    internal static ClickHouseTcpTransportException Read(Exception cause) =>
        new("Reading from the ClickHouse connection failed.", cause);

    /// <summary>Wraps a failure that broke a write.</summary>
    internal static ClickHouseTcpTransportException Write(Exception cause) =>
        new("Writing to the ClickHouse connection failed.", cause);

    /// <summary>Wraps a failure that broke the connect or the TLS handshake.</summary>
    /// <param name="host">The host that was dialled.</param>
    /// <param name="port">The port that was dialled.</param>
    /// <param name="cause">The failure the runtime raised.</param>
    internal static ClickHouseTcpTransportException Connect(string host, int port, Exception cause) =>
        new($"Connecting to ClickHouse at {host}:{port} failed.", cause);

    /// <summary>
    /// Reports a server that closed the connection mid-message. The zero-byte read is not an exception the
    /// runtime raises, so the <see cref="EndOfStreamException"/> is built here to carry the distinction.
    /// </summary>
    internal static ClickHouseTcpTransportException EndOfStream() =>
        new(
            "The ClickHouse server closed the connection before the response was complete.",
            new EndOfStreamException("Unexpected end of stream while reading from ClickHouse."));
}
