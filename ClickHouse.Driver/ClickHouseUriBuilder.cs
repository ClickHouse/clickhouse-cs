﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Web;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver;

internal class ClickHouseUriBuilder
{
    private readonly IDictionary<string, string> sqlQueryParameters = new Dictionary<string, string>();

    public ClickHouseUriBuilder(Uri baseUri)
    {
        BaseUri = baseUri;
    }

    public Uri BaseUri { get; }

    public string Sql { get; set; }

    public bool UseCompression { get; set; }

    public string Database { get; set; }

    public string SessionId { get; set; }

    public string QueryId { get; set; }

    public static string DefaultFormat => "RowBinaryWithNamesAndTypes";

    public IDictionary<string, object> ConnectionQueryStringParameters { get; set; }

    public IDictionary<string, object> CommandQueryStringParameters { get; set; }

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
        parameters.SetOrRemove("query_id", QueryId);

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

        var uriBuilder = new UriBuilder(BaseUri) { Query = queryString };
        return uriBuilder.ToString();
    }
}
