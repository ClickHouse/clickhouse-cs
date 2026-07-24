using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Web;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver;

internal class ClickHouseUriBuilder
{
    // Reused across ToString() calls on the same thread to avoid allocating a builder per request.
    // Detached while in use so a (theoretical) reentrant call cannot corrupt an in-progress build,
    // and dropped again if it grew unusually large so a one-off huge URI does not pin the buffer.
    [ThreadStatic]
    private static StringBuilder cachedBuilder;

    private const int MaxRetainedBuilderCapacity = 64 * 1024;

    // Allocated lazily: most queries carry no SQL parameters at all.
    private Dictionary<string, string> sqlQueryParameters;
    private string effectiveQueryId;

    public ClickHouseUriBuilder(Uri baseUri)
    {
        BaseUri = baseUri;
    }

    public Uri BaseUri { get; }

    public string Sql { get; set; }

    public bool UseCompression { get; set; }

    public string Database { get; set; }

    public string SessionId { get; set; }

    private string queryId;

    public string QueryId
    {
        get => queryId;
        set
        {
            queryId = value;
            effectiveQueryId = null; // Clear cache so GetEffectiveQueryId() re-evaluates
        }
    }

    public static string DefaultFormat => "RowBinaryWithNamesAndTypes";

    public IDictionary<string, object> ConnectionQueryStringParameters { get; set; }

    public IDictionary<string, object> CommandQueryStringParameters { get; set; }

    public IReadOnlyList<string> ConnectionRoles { get; set; }

    public IReadOnlyList<string> CommandRoles { get; set; }

    public JsonReadMode JsonReadMode { get; set; }

    public JsonWriteMode JsonWriteMode { get; set; }

    public TimeSpan? MaxExecutionTime { get; set; }

    /// <summary>
    /// Gets the effective query ID that will be used in the request.
    /// If QueryId is not set, generates and caches a new GUID.
    /// </summary>
    public string GetEffectiveQueryId()
    {
        return effectiveQueryId ??= string.IsNullOrEmpty(QueryId) ? Guid.NewGuid().ToString() : QueryId;
    }

    public bool AddSqlQueryParameter(string name, string value) =>
        (sqlQueryParameters ??= new Dictionary<string, string>()).TryAdd(name, value);

    public override string ToString()
    {
        var sb = cachedBuilder ?? new StringBuilder(256);
        cachedBuilder = null; // detach while in use
        sb.Clear();

        try
        {
            // Compose the absolute URI directly rather than allocating a UriBuilder on every call.
            // BaseUri is credential-free, so Scheme://Host:Port/AbsolutePath reproduces
            // UriBuilder.ToString() exactly, including the always-present (even default) port.
            sb.Append(BaseUri.Scheme)
              .Append("://")
              .Append(BaseUri.Host)
              .Append(':')
              .Append(BaseUri.Port.ToString(CultureInfo.InvariantCulture))
              .Append(BaseUri.AbsolutePath);

            var first = true;

            // Connection/command custom settings are the only source of key collisions. When none
            // are present every key we emit is unique by construction (distinct base-setting keys,
            // distinct JSON-mode keys, and `param_`-prefixed names unique within the collection),
            // so we can stream directly and skip the deduplication dictionary — the common case.
            if (ConnectionQueryStringParameters?.Count > 0 || CommandQueryStringParameters?.Count > 0)
            {
                foreach (var kvp in BuildDeduplicatedParameters())
                    AppendParameter(sb, ref first, kvp.Key, kvp.Value);
            }
            else
            {
                AppendBaseParameters(sb, ref first);

                if (sqlQueryParameters != null)
                {
                    foreach (var parameter in sqlQueryParameters)
                        AppendParameter(sb, ref first, "param_" + parameter.Key, parameter.Value);
                }

                if (MaxExecutionTime.HasValue)
                    AppendParameter(sb, ref first, "max_execution_time", MaxExecutionTime.Value.TotalSeconds.ToString(CultureInfo.InvariantCulture));
            }

            // Role parameters are intentionally repeatable (role=a&role=b) and never deduplicated;
            // command roles replace connection roles.
            var activeRoles = CommandRoles?.Count > 0 ? CommandRoles : ConnectionRoles;
            if (activeRoles?.Count > 0)
            {
                foreach (var role in activeRoles)
                    AppendParameter(sb, ref first, "role", role);
            }

            return sb.ToString();
        }
        finally
        {
            if (sb.Capacity <= MaxRetainedBuilderCapacity)
                cachedBuilder = sb;
        }
    }

    private static void AppendParameter(StringBuilder sb, ref bool first, string key, string value)
    {
        sb.Append(first ? '?' : '&');
        first = false;

        // Keys are known-safe or `param_`-prefixed identifiers; only values are URL-encoded,
        // matching the previous behavior exactly (including HttpUtility's lowercase %xx escapes).
        sb.Append(key).Append('=').Append(HttpUtility.UrlEncode(value));
    }

    private void AppendBaseParameters(StringBuilder sb, ref bool first)
    {
        AppendParameter(sb, ref first, "enable_http_compression", UseCompression ? "true" : "false");
        AppendParameter(sb, ref first, "default_format", DefaultFormat);

        if (!string.IsNullOrEmpty(Database))
            AppendParameter(sb, ref first, "database", Database);
        if (!string.IsNullOrEmpty(SessionId))
            AppendParameter(sb, ref first, "session_id", SessionId);
        if (!string.IsNullOrEmpty(Sql))
            AppendParameter(sb, ref first, "query", Sql);

        AppendParameter(sb, ref first, "query_id", GetEffectiveQueryId());

        // Inject JSON format settings based on mode. None skips the setting entirely
        // (for readonly connections or older servers).
        if (JsonReadMode == JsonReadMode.Binary)
            AppendParameter(sb, ref first, "output_format_binary_write_json_as_string", "0");
        else if (JsonReadMode == JsonReadMode.String)
            AppendParameter(sb, ref first, "output_format_binary_write_json_as_string", "1");

        if (JsonWriteMode == JsonWriteMode.Binary)
            AppendParameter(sb, ref first, "input_format_binary_read_json_as_string", "0");
        else if (JsonWriteMode == JsonWriteMode.String)
            AppendParameter(sb, ref first, "input_format_binary_read_json_as_string", "1");
    }

    // Slow path: custom settings can override any earlier key, so resolve the effective value per
    // key via a dictionary (last write wins) exactly as the original implementation did.
    private Dictionary<string, string> BuildDeduplicatedParameters()
    {
        var parameters = new Dictionary<string, string>();
        parameters.Set("enable_http_compression", UseCompression ? "true" : "false");
        parameters.Set("default_format", DefaultFormat);
        parameters.SetOrRemove("database", Database);
        parameters.SetOrRemove("session_id", SessionId);
        parameters.SetOrRemove("query", Sql);
        parameters.Set("query_id", GetEffectiveQueryId());

        // Inject JSON format settings before custom parameters to allow for overrides.
        if (JsonReadMode == JsonReadMode.Binary)
            parameters.Set("output_format_binary_write_json_as_string", "0");
        else if (JsonReadMode == JsonReadMode.String)
            parameters.Set("output_format_binary_write_json_as_string", "1");

        if (JsonWriteMode == JsonWriteMode.Binary)
            parameters.Set("input_format_binary_read_json_as_string", "0");
        else if (JsonWriteMode == JsonWriteMode.String)
            parameters.Set("input_format_binary_read_json_as_string", "1");

        if (sqlQueryParameters != null)
        {
            foreach (var parameter in sqlQueryParameters)
                parameters.Set("param_" + parameter.Key, parameter.Value);
        }

        if (ConnectionQueryStringParameters != null)
        {
            foreach (var parameter in ConnectionQueryStringParameters)
                parameters.Set(parameter.Key, Convert.ToString(parameter.Value, CultureInfo.InvariantCulture));
        }

        if (CommandQueryStringParameters != null)
        {
            foreach (var parameter in CommandQueryStringParameters)
                parameters.Set(parameter.Key, Convert.ToString(parameter.Value, CultureInfo.InvariantCulture));
        }

        if (MaxExecutionTime.HasValue)
            parameters.Set("max_execution_time", MaxExecutionTime.Value.TotalSeconds.ToString(CultureInfo.InvariantCulture));

        return parameters;
    }
}
