using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Web;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver;

internal class ClickHouseUriBuilder
{
    private readonly IDictionary<string, string> sqlQueryParameters = new Dictionary<string, string>();
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

    /// <summary>
    /// Gets the effective query ID that will be used in the request.
    /// If QueryId is not set, generates and caches a new GUID.
    /// </summary>
    public string GetEffectiveQueryId()
    {
        return effectiveQueryId ??= string.IsNullOrEmpty(QueryId) ? Guid.NewGuid().ToString() : QueryId;
    }

    public bool AddSqlQueryParameter(string name, string value) =>
        DictionaryExtensions.TryAdd(sqlQueryParameters, name, value);

    public override string ToString()
    {
        var parameters = new Dictionary<string, string>(); // NameValueCollection but a special one
        parameters.Set(
            "enable_http_compression",
            UseCompression.ToString(CultureInfo.InvariantCulture).ToLowerInvariant());
        parameters.Set("default_format", DefaultFormat);
        parameters.SetOrRemove("database", Database);
        parameters.SetOrRemove("session_id", SessionId);
        parameters.SetOrRemove("query", Sql);
        parameters.Set("query_id", GetEffectiveQueryId());

        foreach (var parameter in sqlQueryParameters)
            parameters.Set("param_" + parameter.Key, parameter.Value.ToString(CultureInfo.InvariantCulture));

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

        var queryString = string.Join("&", parameters.Select(kvp => $"{kvp.Key}={HttpUtility.UrlEncode(kvp.Value)}"));

        // Append role parameters - command roles replace connection roles
        var activeRoles = CommandRoles?.Count > 0 ? CommandRoles : ConnectionRoles;
        if (activeRoles?.Count > 0)
        {
            var roleParams = string.Join("&", activeRoles.Select(role => $"role={HttpUtility.UrlEncode(role)}"));
            queryString = string.IsNullOrEmpty(queryString) ? roleParams : $"{queryString}&{roleParams}";
        }

        var uriBuilder = new UriBuilder(BaseUri) { Query = queryString };
        return uriBuilder.ToString();
    }
}
