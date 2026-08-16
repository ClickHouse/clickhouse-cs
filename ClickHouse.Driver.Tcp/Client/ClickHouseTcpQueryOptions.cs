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
    /// <summary>The query id, or null to let the server assign one.</summary>
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
    /// Each value is formatted as the type its placeholder declares, so the query text must carry the type —
    /// <c>SELECT * FROM t WHERE id = {id:Int32}</c>. A parameter can override that type; see
    /// <see cref="ClickHouseTcpParameter.ClickHouseType"/>. Parameters need a server whose protocol revision is
    /// 54459 or above; an older one rejects the query rather than running it without them.
    /// </para>
    /// <para>
    /// <b>Declare a timezone when the value names an instant.</b> A <see cref="DateTime"/> whose
    /// <see cref="DateTime.Kind"/> is <see cref="DateTimeKind.Utc"/> or <see cref="DateTimeKind.Local"/>, and
    /// any <see cref="DateTimeOffset"/>, names a point in time. The wire carries a wall-clock time and no
    /// timezone, so the server reads it in its session timezone: a type that declares none moves the instant
    /// whenever that session is not UTC, and reports no error. The client refuses such a parameter rather than
    /// send it. Write <c>{t:DateTime('UTC')}</c> or <c>{t:DateTime64(3, 'UTC')}</c>, or pass a
    /// <see cref="DateTime"/> of <see cref="DateTimeKind.Unspecified"/> when you do mean a wall-clock time for
    /// the server to read in its own timezone.
    /// </para>
    /// <para>
    /// <b>Avoid naming a parameter after a server setting.</b> The native protocol carries parameters in the
    /// settings list, so a server that reads the name as a setting applies it as that setting instead of
    /// binding it. The query then fails with a parse error about a quoted string, which names neither the
    /// parameter nor the cause. The common traps are <c>limit</c> and <c>offset</c>; <c>max_threads</c>,
    /// <c>readonly</c> and <c>log_comment</c> behave the same way. Rename the parameter (<c>row_limit</c>) and
    /// the query works.
    /// </para>
    /// <para>
    /// This is the server's behaviour, not this client's — <c>clickhouse-client --param_limit=</c> fails the
    /// same way — and it is version-dependent: it affects 25.8 through 26.6, while newer servers bind such a
    /// name correctly. It does not affect this driver's HTTP transport, which carries the name separately.
    /// Rename the parameter if you support any server in that range.
    /// </para>
    /// </remarks>
    public ClickHouseTcpParameterCollection Parameters { get; init; }
}
