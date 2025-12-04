using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
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
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.ADO;

public class ClickHouseCommand : DbCommand, IClickHouseCommand, IDisposable
{
    private readonly CancellationTokenSource cts = new CancellationTokenSource();
    private readonly ClickHouseParameterCollection commandParameters = new ClickHouseParameterCollection();
    private Dictionary<string, object> customSettings;
    private List<string> roles;
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
    /// Gets or sets QueryId associated with command.
    /// If not set before execution, a GUID will be automatically generated.
    /// </summary>
    public string QueryId { get; set; }

    public QueryStats QueryStats { get; private set; }

    /// <summary>
    /// Gets collection of custom settings which will be passed as URL query string parameters.
    /// </summary>
    /// <remarks>Not thread-safe.</remarks>
    public IDictionary<string, object> CustomSettings => customSettings ??= new Dictionary<string, object>();

    /// <summary>
    /// Gets or sets a bearer token for this command, overriding the connection-level token.
    /// When set, this token is used for Bearer authentication instead of the connection's
    /// BearerToken or Username/Password credentials.
    /// </summary>
    public string BearerToken { get; set; }

    /// <summary>
    /// Gets the roles to use for this command.
    /// When set, these roles replace any connection-level roles.
    /// </summary>
    /// <remarks>Not thread-safe.</remarks>
    public IList<string> Roles => roles ??= new List<string>();

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
        return await ClickHouseDataReader.FromHttpResponseAsync(result, connection.TypeSettings).ConfigureAwait(false);
    }

    private async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token)
    {
        if (connection == null)
            throw new InvalidOperationException("Connection not set");

        using var activity = connection.StartActivity("PostSqlQueryAsync");

        var uriBuilder = connection.CreateUriBuilder();
        uriBuilder.QueryId = QueryId;
        uriBuilder.CommandQueryStringParameters = customSettings;
        uriBuilder.CommandRoles = roles;

        var logger = connection.GetLogger(ClickHouseLogCategories.Command);
        var isDebugLoggingEnabled = logger?.IsEnabled(LogLevel.Debug) ?? false;
        Stopwatch stopwatch = null;
        if (isDebugLoggingEnabled)
        {
            stopwatch = Stopwatch.StartNew();
            logger.LogDebug("Executing SQL query. QueryId: {QueryId}", uriBuilder.GetEffectiveQueryId());
        }
        
        await connection.EnsureOpenAsync().ConfigureAwait(false); // Preserve old behavior

        using var postMessage = connection.UseFormDataParameters
            ? BuildHttpRequestMessageWithFormData(
                sqlQuery: sqlQuery,
                uriBuilder: uriBuilder)
            : BuildHttpRequestMessageWithQueryParams(
                sqlQuery: sqlQuery,
                uriBuilder: uriBuilder);

        activity.SetQuery(sqlQuery);

        HttpResponseMessage response = null;
        try
        {
            response = await connection
                .SendAsync(postMessage, HttpCompletionOption.ResponseHeadersRead, token)
                .ConfigureAwait(false);

            QueryId = ClickHouseConnection.ExtractQueryId(response);
            QueryStats = ExtractQueryStats(response);
            activity.SetQueryStats(QueryStats);

            var handled = await ClickHouseConnection.HandleError(response, sqlQuery, activity).ConfigureAwait(false);

            if (isDebugLoggingEnabled)
            {
                LogQuerySuccess(stopwatch, QueryId, logger);
            }

            return handled;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "Query (QueryId: {QueryId}) failed.", uriBuilder.GetEffectiveQueryId());
            activity?.SetException(ex);
            throw;
        }
    }

    private void LogQuerySuccess(Stopwatch stopwatch, string queryId, ILogger logger)
    {
        stopwatch.Stop();
        logger.LogDebug(
            "Query (QueryId: {QueryId}) succeeded in {ElapsedMilliseconds:F2} ms. Query Stats: {QueryStats}",
            queryId,
            stopwatch.Elapsed.TotalMilliseconds,
            QueryStats);
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

        connection.AddDefaultHttpHeaders(postMessage.Headers, bearerTokenOverride: BearerToken);
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

        connection.AddDefaultHttpHeaders(postMessage.Headers, bearerTokenOverride: BearerToken);

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
            if (response.Headers.TryGetValues(summaryHeader, out var values))
            {
                return JsonSerializer.Deserialize<QueryStats>(values.First(), SummarySerializerOptions);
            }
        }
        catch
        {
        }
        return null;
    }
}
