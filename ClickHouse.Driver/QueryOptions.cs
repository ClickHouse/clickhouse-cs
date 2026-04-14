#nullable enable
using System;
using System.Collections.Generic;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;

namespace ClickHouse.Driver;

/// <summary>
/// Options for query execution that can override client-level defaults.
/// </summary>
public class QueryOptions
{
    /// <summary>
    /// Gets or sets the query identifier for tracking and logging purposes.
    /// If not provided, a GUID will be generated.
    /// For batch inserts, each batch gets a unique suffixed ID ({queryId}-1, {queryId}-2, etc.).
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
    /// If set to null, will not override client settings.
    /// Default: null
    /// </summary>
    public bool? UseSession { get; init; }

    /// <summary>
    /// Gets or sets the session ID to use (the value is only used if UseSession is true).
    /// Default: null
    /// </summary>
    public string? SessionId { get; init; }

    /// <summary>
    /// Gets or sets the bearer token for JWT authentication.
    /// When set, Bearer authentication is used instead of Basic authentication
    /// (Username and Password are ignored for the Authorization header).
    /// The token should be provided as-is (already encoded if required by your auth provider).
    /// Default: null
    /// </summary>
    public string? BearerToken { get; init; }

    /// <summary>
    /// Gets or sets a custom resolver for mapping .NET types to ClickHouse types for this query,
    /// overriding <see cref="ClickHouseClientSettings.ParameterTypeResolver"/>.
    /// Return null from the resolver to fall through to default behavior.
    /// Default: null (use client-level resolver)
    /// </summary>
    public IParameterTypeResolver? ParameterTypeResolver { get; init; }

    /// <summary>
    /// Gets or sets a custom formatter for serializing parameter values for this query,
    /// overriding <see cref="ClickHouseClientSettings.ParameterFormatter"/>.
    /// Return null from the formatter to fall through to default formatting.
    /// Default: null (use client-level formatter)
    /// </summary>
    public IParameterFormatter? ParameterFormatter { get; init; }

    /// <summary>
    /// Gets or sets a custom converter for same-type transformation of values returned by the data reader
    /// for this query, overriding <see cref="ClickHouseClientSettings.ReadValueConverter"/>.
    /// Default: null (use client-level converter)
    /// </summary>
    public IReadValueConverter? ReadValueConverter { get; init; }

    /// <summary>
    /// Gets or sets the maximum execution time for this query.
    /// When set, this value is passed to ClickHouse as the max_execution_time setting,
    /// which causes the server to cancel the query if it exceeds this duration.
    /// Default: null (no limit)
    /// </summary>
    public TimeSpan? MaxExecutionTime { get; init; }

    /// <summary>
    /// Gets or sets the HTTP <c>Accept-Encoding</c> header value sent with this query's request.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When non-null and non-empty, this value <b>overrides</b> the default Accept-Encoding
    /// (<c>gzip, deflate</c>) that the client adds when
    /// <see cref="ADO.ClickHouseClientSettings.UseCompression"/> is true. The server is asked
    /// to encode the response body with the negotiated algorithm. Multiple algorithms may be
    /// quality-weighted per the HTTP spec, e.g. <c>"zstd, gzip;q=0.5"</c>.
    /// </para>
    /// <para>
    /// Setting this property also forces <c>enable_http_compression=1</c> on the URL for the
    /// request — ClickHouse only honours <c>Accept-Encoding</c> when that setting is on.
    /// </para>
    /// <para>
    /// Note that the underlying <see cref="System.Net.Http.HttpClient"/> may transparently
    /// decompress some algorithms (gzip/deflate/brotli by default in .NET); the resulting body
    /// and <see cref="ADO.ClickHouseRawResult.ContentEncoding"/> reflect what the consumer
    /// actually receives after that step.
    /// </para>
    /// <para>
    /// <b>Warning:</b> selecting a codec the underlying <see cref="System.Net.Http.HttpClient"/>
    /// cannot decode (e.g. <c>zstd</c>, <c>lz4</c>) is only meaningful with
    /// <see cref="IClickHouseClient.ExecuteRawResultAsync"/>, where the caller decodes the body
    /// themselves after inspecting <see cref="ADO.ClickHouseRawResult.ContentEncoding"/>. The
    /// parsing reader APIs (<c>ExecuteReaderAsync</c>, <c>ExecuteScalarAsync</c>,
    /// <c>ExecuteNonQueryAsync</c>) will read the still-compressed bytes as if they were the
    /// ClickHouse wire format and return garbage or throw. To use such a codec end-to-end you
    /// also need an <see cref="System.Net.Http.HttpClient"/> configured without
    /// <c>AutomaticDecompression</c>; otherwise the framework strips
    /// <c>Content-Encoding</c> before the body reaches the driver.
    /// </para>
    /// </remarks>
    public string? AcceptEncoding { get; init; }

    /// <summary>
    /// Creates a new <see cref="QueryOptions"/> with the same settings but a different <see cref="QueryId"/>.
    /// </summary>
    internal QueryOptions WithQueryId(string queryId)
    {
        return new QueryOptions
        {
            QueryId = queryId,
            Database = Database,
            Roles = Roles,
            CustomSettings = CustomSettings,
            CustomHeaders = CustomHeaders,
            UseSession = UseSession,
            SessionId = SessionId,
            BearerToken = BearerToken,
            ParameterTypeResolver = ParameterTypeResolver,
            ParameterFormatter = ParameterFormatter,
            ReadValueConverter = ReadValueConverter,
            MaxExecutionTime = MaxExecutionTime,
            AcceptEncoding = AcceptEncoding,
        };
    }
}
