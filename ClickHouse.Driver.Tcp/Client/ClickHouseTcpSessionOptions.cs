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
    /// <para>
    /// Every operation of the session runs on the replica its connection reached. Sessions with the same tag reach the
    /// same replica while the number of replicas does not change. See <see cref="ClickHouseTcpClientOptions.ReplicaTag"/>
    /// for the rules a tag must follow.
    /// </para>
    /// <para>
    /// A tag other than the client's opens a new connection for the session. It counts against
    /// <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/> while the session is open. When the pool is full, the
    /// pool first closes its longest-idle connection.
    /// </para>
    /// </remarks>
    public string ReplicaTag { get; init; }
}
