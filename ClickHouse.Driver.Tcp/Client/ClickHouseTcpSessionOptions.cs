namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Options for a session opened with <see cref="IClickHouseTcpClient.OpenSessionAsync(ClickHouseTcpSessionOptions, System.Threading.CancellationToken)"/>.
/// All members are optional; a null member falls back to the client-level default.
/// </summary>
public sealed record ClickHouseTcpSessionOptions
{
    /// <summary>
    /// The replica tag of the session's connection, for ClickHouse Cloud replica-aware routing. Null or empty uses
    /// <see cref="ClickHouseTcpClientOptions.ReplicaTag"/>.
    /// </summary>
    /// <remarks>
    /// The session gets a connection opened with this tag: an idle pooled one when there is one, otherwise a new
    /// one. Every operation of the session runs on the replica that connection reached. Sessions with the same tag
    /// reach the same replica while the number of replicas does not change. See
    /// <see cref="ClickHouseTcpClientOptions.ReplicaTag"/> for the rules a tag must follow.
    /// </remarks>
    public string ReplicaTag { get; init; }
}
