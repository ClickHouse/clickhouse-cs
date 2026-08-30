using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A run of operations over one pinned connection, opened with
/// <see cref="IClickHouseTcpClient.OpenSessionAsync"/>. State a single connection holds — temporary tables, and what
/// a <c>SET</c> changed — therefore survives from one operation to the next.
///
/// <para>
/// <b>One operation at a time.</b> The protocol carries one query per connection, so a second operation started
/// while the first is still running is refused rather than interleaved, and a streamed result holds the connection
/// until it is read to the end or its enumerator is disposed.
/// </para>
///
/// <para>
/// <b>Disposal closes the connection</b> instead of pooling it, so an unrelated caller cannot inherit the session's
/// temporary tables and altered settings. A streamed result left suspended mid-enumeration and never disposed is the
/// exception: disposal cannot free its pool slot either, and the pool stays a slot short until the client is disposed.
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
    /// Whether anything has been seen to end the session: false once it is disposed, and false once its connection is
    /// found unusable — which takes the session's server-side state with it, so the answer is to open another session,
    /// not to retry on this one.
    /// </summary>
    /// <remarks>
    /// The connection is tested when an operation ends and again before the next one starts, so a drop while the
    /// session sat idle is caught. True is still not a promise that the next operation will work, since nothing rules
    /// out a drop between this answer and its use: treat false as certain and true as "nothing known to be wrong". An
    /// operation leaves the connection unusable by failing at the transport or protocol level, by receiving a
    /// server-side error, by being cancelled, or by streaming a result abandoned part-way.
    /// </remarks>
    bool IsOpen { get; }
}
