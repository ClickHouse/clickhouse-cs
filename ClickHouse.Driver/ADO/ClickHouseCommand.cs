﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Diagnostic;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.ADO;

public class ClickHouseCommand : DbCommand, IClickHouseCommand, IDisposable
{
    private readonly CancellationTokenSource cts = new CancellationTokenSource();
    private readonly ClickHouseParameterCollection commandParameters = new ClickHouseParameterCollection();
    private Dictionary<string, object> customSettings;
    private ClickHouseConnection connection;

    public ClickHouseCommand()
    {
    }

    public ClickHouseCommand(ClickHouseConnection connection)
    {
        this.connection = connection;
    }

    public override string CommandText { get; set; }

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType { get; set; }

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    /// Gets or sets QueryId associated with command
    /// After query execution, will be set by value provided by server
    /// Value will be same if provided or a UUID generated by server if not
    /// </summary>
    public string QueryId { get; set; }

    public QueryStats QueryStats { get; private set; }

    /// <summary>
    /// Gets collection of custom settings which will be passed as URL query string parameters.
    /// </summary>
    /// <remarks>Not thread-safe.</remarks>
    public IDictionary<string, object> CustomSettings => customSettings ??= new Dictionary<string, object>();

    protected override DbConnection DbConnection
    {
        get => connection;
        set => connection = (ClickHouseConnection)value;
    }

    protected override DbParameterCollection DbParameterCollection => commandParameters;

    protected override DbTransaction DbTransaction { get; set; }

    public override void Cancel() => cts.Cancel();

    public override int ExecuteNonQuery() => ExecuteNonQueryAsync(cts.Token).GetAwaiter().GetResult();

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        if (connection == null)
            throw new InvalidOperationException("Connection is not set");

        using var lcts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
        using var response = await PostSqlQueryAsync(CommandText, lcts.Token).ConfigureAwait(false);
#if NET5_0_OR_GREATER
        using var reader = new ExtendedBinaryReader(await response.Content.ReadAsStreamAsync(lcts.Token).ConfigureAwait(false));
#else
        using var reader = new ExtendedBinaryReader(await response.Content.ReadAsStreamAsync().ConfigureAwait(false));
#endif

        return reader.PeekChar() != -1 ? reader.Read7BitEncodedInt() : 0;
    }

    /// <summary>
    ///  Allows to return raw result from a query (with custom FORMAT)
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>ClickHouseRawResult object containing response stream</returns>
    public async Task<ClickHouseRawResult> ExecuteRawResultAsync(CancellationToken cancellationToken)
    {
        if (connection == null)
            throw new InvalidOperationException("Connection is not set");

        using var lcts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
        var response = await PostSqlQueryAsync(CommandText, lcts.Token).ConfigureAwait(false);
        return new ClickHouseRawResult(response);
    }

    public override object ExecuteScalar() => ExecuteScalarAsync(cts.Token).GetAwaiter().GetResult();

    public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        using var lcts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
        using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, lcts.Token).ConfigureAwait(false);
        return reader.Read() ? reader.GetValue(0) : null;
    }

    public override void Prepare() { /* ClickHouse has no notion of prepared statements */ }

    public new ClickHouseDbParameter CreateParameter() => new ClickHouseDbParameter();

    protected override DbParameter CreateDbParameter() => CreateParameter();

#pragma warning disable CA2215 // Dispose methods should call base class dispose
    protected override void Dispose(bool disposing)
#pragma warning restore CA2215 // Dispose methods should call base class dispose
    {
        if (disposing)
        {
            // Dispose token source but do not cancel
            cts.Dispose();
        }
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteDbDataReaderAsync(behavior, cts.Token).GetAwaiter().GetResult();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (connection == null)
            throw new InvalidOperationException("Connection is not set");

        using var lcts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
        var sqlBuilder = new StringBuilder(CommandText);
        switch (behavior)
        {
            case CommandBehavior.SingleRow:
                sqlBuilder.Append(" LIMIT 1");
                break;
            case CommandBehavior.SchemaOnly:
                sqlBuilder.Append(" LIMIT 0");
                break;
            default:
                break;
        }
        var result = await PostSqlQueryAsync(sqlBuilder.ToString(), lcts.Token).ConfigureAwait(false);
        return ClickHouseDataReader.FromHttpResponse(result, connection.TypeSettings);
    }

    private async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token)
    {
        if (connection == null)
            throw new InvalidOperationException("Connection not set");
        using var activity = connection.StartActivity("PostSqlQueryAsync");

        var uriBuilder = connection.CreateUriBuilder();
        await connection.EnsureOpenAsync().ConfigureAwait(false); // Preserve old behavior

        uriBuilder.QueryId = QueryId;
        uriBuilder.CommandQueryStringParameters = customSettings;

        using var postMessage = connection.UseFormDataParameters
            ? BuildHttpRequestMessageWithFormData(
                sqlQuery: sqlQuery,
                uriBuilder: uriBuilder)
            : BuildHttpRequestMessageWithQueryParams(
                sqlQuery: sqlQuery,
                uriBuilder: uriBuilder);

        activity.SetQuery(sqlQuery);

        var response = await connection.HttpClient
            .SendAsync(postMessage, HttpCompletionOption.ResponseHeadersRead, token)
            .ConfigureAwait(false);

        QueryId = ExtractQueryId(response);
        QueryStats = ExtractQueryStats(response);
        activity.SetQueryStats(QueryStats);
        return await ClickHouseConnection.HandleError(response, sqlQuery, activity).ConfigureAwait(false);
    }

    private HttpRequestMessage BuildHttpRequestMessageWithQueryParams(string sqlQuery, ClickHouseUriBuilder uriBuilder)
    {
        if (commandParameters != null)
        {
            sqlQuery = commandParameters.ReplacePlaceholders(sqlQuery);
            foreach (ClickHouseDbParameter parameter in commandParameters)
            {
                uriBuilder.AddSqlQueryParameter(
                    parameter.ParameterName,
                    HttpParameterFormatter.Format(parameter, connection.TypeSettings));
            }
        }

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        connection.AddDefaultHttpHeaders(postMessage.Headers);
        HttpContent content = new StringContent(sqlQuery);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/sql");
        if (connection.UseCompression)
        {
            content = new CompressedContent(content, DecompressionMethods.GZip);
        }

        postMessage.Content = content;

        return postMessage;
    }

    private HttpRequestMessage BuildHttpRequestMessageWithFormData(string sqlQuery, ClickHouseUriBuilder uriBuilder)
    {
        var content = new MultipartFormDataContent();

        if (commandParameters != null)
        {
            sqlQuery = commandParameters.ReplacePlaceholders(sqlQuery);

            foreach (ClickHouseDbParameter parameter in commandParameters)
            {
                content.Add(
                    content: new StringContent(HttpParameterFormatter.Format(parameter, connection.TypeSettings)),
                    name: $"param_{parameter.ParameterName}");
            }
        }

        content.Add(
            content: new StringContent(sqlQuery),
            name: "query");

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        connection.AddDefaultHttpHeaders(postMessage.Headers);

        postMessage.Content = content;

        return postMessage;
    }

    private static readonly JsonSerializerOptions SummarySerializerOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = new SnakeCaseNamingPolicy(),
        NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString,
    };

    private static QueryStats ExtractQueryStats(HttpResponseMessage response)
    {
        try
        {
            const string summaryHeader = "X-ClickHouse-Summary";
            if (response.Headers.Contains(summaryHeader))
            {
                var value = response.Headers.GetValues(summaryHeader).FirstOrDefault();
                var jsonDoc = JsonDocument.Parse(value);
                return JsonSerializer.Deserialize<QueryStats>(value, SummarySerializerOptions);
            }
        }
        catch
        {
        }
        return null;
    }

    private static string ExtractQueryId(HttpResponseMessage response)
    {
        const string queryIdHeader = "X-ClickHouse-Query-Id";
        if (response.Headers.Contains(queryIdHeader))
            return response.Headers.GetValues(queryIdHeader).FirstOrDefault();
        else
            return null;
    }
}
