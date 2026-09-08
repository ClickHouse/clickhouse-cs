using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Sequential operations on one connection, preserving temporary tables and <c>SET</c> settings.
/// Created by <see cref="IClickHouseTcpClient.OpenSessionAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// Concurrent operations are rejected. A streamed result holds the session until fully read or its enumerator
/// is disposed. Use <c>await foreach</c> to ensure the enumerator is disposed.
/// </para>
///
/// <para>
/// Disposal closes the connection to prevent other callers from inheriting session state. It aborts an active
/// operation without waiting; the pool slot is released when that operation exits. An undisposed enumerator
/// suspended mid-stream keeps the slot occupied even after session disposal, until the client is disposed.
/// </para>
///
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </remarks>
[Experimental("CHTCP0001")]
public interface IClickHouseTcpSession : IClickHouseTcpOperations
{
    /// <summary>
    /// Whether the session is undisposed and its connection is not known to be unusable.
    /// </summary>
    /// <remarks>
    /// Once false, it remains false; open a new session to continue. True does not guarantee the next operation
    /// will succeed. Transport, protocol, or server errors, cancellation, and incomplete result streams can make
    /// the connection unusable. Connection health is checked between operations, not while one is running.
    /// </remarks>
    bool IsOpen { get; }
}
