using System.Linq;
using System.Net.Http;
using System.Text.Json;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Json;

namespace ClickHouse.Driver;

internal class QueryResult
{
    /// <summary>
    /// Gets the HTTP response message.
    /// </summary>
    public HttpResponseMessage HttpResponseMessage { get; init; }

    /// <summary>
    /// Gets or sets QueryId associated with command.
    /// If not set before execution, a GUID will be automatically generated.
    /// </summary>
    public string QueryId { get; init; }

    /// <summary>
    /// Gets statistics from the last executed query (rows read, bytes read, elapsed time, etc.).
    /// Populated after query execution from the X-ClickHouse-Summary header.
    /// </summary>
    public QueryStats QueryStats { get; init; }

    /// <summary>
    /// Gets the server's timezone from the last executed query response.
    /// This is extracted from the X-ClickHouse-Timezone header.
    /// </summary>
    public string ServerTimezone { get; init; }

    public QueryResult(HttpResponseMessage httpResponseMessage)
    {
        HttpResponseMessage = httpResponseMessage;
        QueryId = ExtractQueryId(httpResponseMessage);
        QueryStats = ExtractQueryStats(httpResponseMessage);
        ServerTimezone = ExtractTimezone(httpResponseMessage);
    }

    internal static string ExtractQueryId(HttpResponseMessage response)
    {
        const string queryIdHeader = "X-ClickHouse-Query-Id";
        if (response.Headers.Contains(queryIdHeader))
            return response.Headers.GetValues(queryIdHeader).FirstOrDefault();
        else
            return null;
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

    private static string ExtractTimezone(HttpResponseMessage response)
    {
        const string timezoneHeader = "X-ClickHouse-Timezone";
        if (response.Headers.TryGetValues(timezoneHeader, out var values))
        {
            return values.FirstOrDefault();
        }
        return null;
    }
}
