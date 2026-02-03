using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver;

/// <summary>
/// Options for query execution that can override client-level defaults.
/// </summary>
public class QueryOptions
{
    /// <summary>
    /// Gets or sets the query identifier for tracking and logging purposes.
    /// </summary>
    public string? QueryId { get; init; }

    /// <summary>
    /// Gets or sets the database to use for this query, overriding the client default.
    /// </summary>
    public string? Database { get; init; }

    /// <summary>
    /// Gets or sets the roles to use for this query, overriding the client default.
    /// </summary>
    public IReadOnlyList<string>? Roles { get; init; }

    /// <summary>
    /// Gets or sets custom ClickHouse settings for this query (e.g., max_threads, max_memory_usage).
    /// </summary>
    public IDictionary<string, object>? CustomSettings { get; init; }

    /// <summary>
    /// Gets or sets custom HTTP headers to send with each request.
    /// These headers are applied after the default headers, allowing you to override most headers.
    /// The following headers cannot be overridden and will be silently ignored:
    /// Connection, Authorization, User-Agent
    /// Default: null
    /// </summary>
    public IReadOnlyDictionary<string, string>? CustomHeaders { get; init; }

    /// <summary>
    /// Gets or sets whether to use sessions for the connection.
    /// Default: false
    /// </summary>
    public bool UseSession { get; init; } = ClickHouseDefaults.UseSession;

    /// <summary>
    /// Gets or sets the session ID to use (the value is only used if UseSession is true).
    /// If null and UseSession is true, a new GUID will be generated.
    /// Default: null
    /// </summary>
    public string SessionId { get; init; }

    /// <summary>
    /// Gets or sets the bearer token for JWT authentication.
    /// When set, Bearer authentication is used instead of Basic authentication
    /// (Username and Password are ignored for the Authorization header).
    /// The token should be provided as-is (already encoded if required by your auth provider).
    /// Default: null
    /// </summary>
    public string BearerToken { get; init; }
}
