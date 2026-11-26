using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Owns a shared TCP client and its pool. Disposing the data source also disposes the client returned by
/// <see cref="GetClient"/>, so callers must not dispose that client themselves.
/// </summary>
[Experimental("CHTCP0001")]
public interface IClickHouseTcpDataSource : IAsyncDisposable, IDisposable
{
    /// <summary>The configuration every operation from this data source runs under.</summary>
    ClickHouseTcpClientOptions Options { get; }

    /// <summary>
    /// Returns the shared client: the same instance on every call, and the data source's to dispose, not the
    /// caller's.
    /// </summary>
    /// <returns>The client this data source owns.</returns>
    IClickHouseTcpClient GetClient();

    /// <summary>Opens a caller-owned session pinned to one connection for preserving server-side state.</summary>
    /// <param name="cancellationToken">A token to observe while waiting for and establishing the connection.</param>
    /// <returns>A session pinned to one connection.</returns>
    ValueTask<IClickHouseTcpSession> OpenSessionAsync(CancellationToken cancellationToken = default);
}
