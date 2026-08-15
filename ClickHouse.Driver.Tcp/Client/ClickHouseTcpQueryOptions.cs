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
}
