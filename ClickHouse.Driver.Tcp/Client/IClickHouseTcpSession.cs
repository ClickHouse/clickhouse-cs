using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A run of operations over one pinned connection, opened with
/// <see cref="IClickHouseTcpClient.OpenSessionAsync"/>. Everything a single connection remembers is therefore
/// still there for the next operation: temporary tables, and the settings a <c>SET</c> statement changed.
///
/// <para>
/// <b>One operation at a time.</b> The protocol carries one query per connection, so a session serves one owner
/// and does not multiplex: a second operation started while the first is still running is refused rather than
/// interleaved on the wire, and a streamed result holds the connection until it is read to the end or its
/// enumerator is disposed.
/// </para>
///
/// <para>
/// <b>Disposal closes the connection</b> instead of returning it to the pool. A connection that has been in a
/// session carries the state the session put there, and handing that to an unrelated caller would leak temporary
/// tables and altered settings into their queries — so the socket is closed and the next caller dials a fresh one.
/// </para>
///
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </summary>
[Experimental("CHTCP0001")]
public interface IClickHouseTcpSession : IClickHouseTcpOperations
{
    /// <summary>
    /// Whether anything has been seen to end the session: false once it is disposed, and false once an operation
    /// has left its connection unusable — which takes the session's server-side state with it, so the answer is to
    /// open another session, not to retry on this one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>True is not a promise that the next operation will work.</b> This reports what the session has already
    /// observed, and it observes only at the end of an operation; a connection the server drops while the session
    /// sits idle is still reported as open, and no answer could be better than that one, since the connection may
    /// be dropped in the moment between the answer and its use. Treat false as certain and true as "nothing known
    /// to be wrong".
    /// </para>
    /// <para>
    /// An operation leaves the connection unusable by failing at the transport or protocol level, by being
    /// cancelled, and by streaming a result that is abandoned part-way — the rest of that result is still coming,
    /// so the connection cannot carry anything else. A server-side error in a query the server accepted, by
    /// contrast, leaves the connection, and so the session, usable.
    /// </para>
    /// </remarks>
    bool IsOpen { get; }
}
