using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Per-query overrides passed to a <see cref="ClickHouseTcpClient"/> operation. All members are optional; a
/// null options argument (or a null member) falls back to the client-level defaults.
/// <see cref="ClickHouseTcpInsertOptions"/> extends this with the insert-only knobs.
/// </summary>
/// <remarks>
/// Being a record, one instance can hold the shared settings and each operation can derive its own variant with a
/// <c>with</c> expression — <c>shared with { QueryId = id }</c>. A <c>with</c> expression on a variable of this type
/// clones the runtime type, so cloning an instance that is really a <see cref="ClickHouseTcpInsertOptions"/> keeps
/// the insert-only knobs. See <see cref="ClickHouseTcpClientOptions"/> for how <see cref="Settings"/> compares.
/// </remarks>
public record ClickHouseTcpQueryOptions
{
    /// <summary>
    /// Identifies the operation in server, client, and trace logs. Null or empty generates a GUID; supplied ids
    /// must be unique among concurrent operations.
    /// </summary>
    public string QueryId { get; init; }

    /// <summary>
    /// Settings for this query, as textual values. These override the client-level
    /// <see cref="ClickHouseTcpClientOptions.CustomSettings"/> for any key present in both. Null means none.
    /// </summary>
    public IReadOnlyDictionary<string, string> Settings { get; init; }

    /// <summary>
    /// The values bound to the query's <c>{name:Type}</c> placeholders. Null means none.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each parameter needs a type from its placeholder, for example <c>{id:Int32}</c>, or from
    /// <see cref="ClickHouseTcpParameter.ClickHouseType"/>. The operation throws when neither supplies one.
    /// </para>
    /// <para>
    /// Declare a timezone for <see cref="DateTimeOffset"/> and for <see cref="DateTime"/> values whose
    /// <see cref="DateTime.Kind"/> is not <see cref="DateTimeKind.Unspecified"/>, for example
    /// <c>{t:DateTime('UTC')}</c>. Without one, the client refuses the value to prevent the server's session
    /// timezone from changing the instant. Use <see cref="DateTimeKind.Unspecified"/> only for wall-clock time.
    /// </para>
    /// <para>
    /// On ClickHouse 25.8 through 26.6, names matching server settings, such as <c>limit</c> or <c>offset</c>,
    /// may be treated as settings and rejected. Rename them (for example, <c>row_limit</c>) when supporting
    /// those versions. Newer servers and this driver's HTTP transport are unaffected.
    /// </para>
    /// </remarks>
    public ClickHouseTcpParameterCollection Parameters { get; init; }

    /// <summary>
    /// Callbacks for the metadata the server interleaves into this operation's response — progress, the
    /// execution summary, server log lines, performance counters, WITH TOTALS and extremes. Null means none, and
    /// costs nothing.
    /// </summary>
    /// <remarks>
    /// The callbacks run synchronously on the thread draining the response, and one that throws terminates the
    /// connection. See <see cref="ClickHouseTcpQueryCallbacks"/> for the full contract.
    /// </remarks>
    public ClickHouseTcpQueryCallbacks Callbacks { get; init; }

    /// <summary>
    /// The replica tag for this operation, for ClickHouse Cloud replica-aware routing. Null or empty uses
    /// <see cref="ClickHouseTcpClientOptions.ReplicaTag"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The tag is sent when a connection opens, so the operation runs on a pooled connection that was opened with
    /// this tag, or on a new one. The pool keeps connections with different tags apart, under one
    /// <see cref="ClickHouseTcpClientOptions.MaxPoolSize"/>. When the pool is full, an idle connection with another
    /// tag is closed to make room.
    /// </para>
    /// <para>
    /// In a session the connection is already open. A session accepts a tag equal to its own and refuses a different
    /// one with <see cref="InvalidOperationException"/>. See <see cref="ClickHouseTcpClientOptions.ReplicaTag"/> for
    /// the rules a tag must follow.
    /// </para>
    /// </remarks>
    public string ReplicaTag { get; init; }
}
