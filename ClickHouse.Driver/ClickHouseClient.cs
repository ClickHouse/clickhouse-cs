using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Diagnostic;
using ClickHouse.Driver.Http;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver;

/// <summary>
/// A high-level client for interacting with ClickHouse.
/// This is the recommended API for new code. It is thread-safe and designed for singleton usage.
/// </summary>
/// <remarks>
/// <para>
/// Unlike <see cref="ADO.ClickHouseConnection"/>, which follows ADO.NET patterns,
/// <see cref="ClickHouseClient"/> provides a simpler, more direct API that better matches
/// ClickHouse's HTTP-based protocol.
/// </para>
/// <para>
/// For best performance, create a single <see cref="ClickHouseClient"/> instance and reuse it
/// throughout your application. The client manages HTTP connection pooling internally.
/// </para>
/// </remarks>
public sealed class ClickHouseClient : IClickHouseClient
{
    private readonly List<IDisposable> _disposables = new();
    private readonly ConcurrentDictionary<string, Lazy<ILogger>> _loggerCache = new();
    private readonly JsonTypeRegistry _jsonTypeRegistry = new();

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly string _httpClientName;
    private readonly Uri _serverUri;
    private readonly ILoggerFactory _loggerFactory;

    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClient"/> class with the specified connection string.
    /// </summary>
    /// <param name="connectionString">The ClickHouse connection string.</param>
    public ClickHouseClient(string connectionString)
        : this(new ClickHouseClientSettings(connectionString))
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClient"/> class with the specified connection string and an HttpClient instance.
    /// </summary>
    /// <param name="connectionString">The ClickHouse connection string.</param>
    /// <param name="httpClient">Instance of HttpClient</param>
    public ClickHouseClient(string connectionString, HttpClient httpClient)
        : this(new ClickHouseClientSettings(connectionString) { HttpClient = httpClient })
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClient"/> class with the specified connection string and an IHttpClientFactory.
    /// </summary>
    /// <param name="connectionString">The ClickHouse connection string.</param>
    /// <param name="httpClientFactory">An IHttpClientFactory</param>
    /// <param name="httpClientName">The name of the HTTP client you want to be created using the provided factory. If left empty, the default client will be created.</param>
    public ClickHouseClient(string connectionString, IHttpClientFactory httpClientFactory, string httpClientName = "")
        : this(new ClickHouseClientSettings(connectionString) 
            { HttpClientFactory = httpClientFactory, HttpClientName = httpClientName })
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseClient"/> class with the specified settings.
    /// </summary>
    /// <param name="settings">The client settings.</param>
    public ClickHouseClient(ClickHouseClientSettings settings)
    {
        Settings = settings ?? throw new ArgumentNullException(nameof(settings));
        Settings.Validate();

        _serverUri = new UriBuilder(Settings.Protocol, Settings.Host, Settings.Port, Settings.Path ?? string.Empty).Uri;
        _httpClientName = Settings.HttpClientName ?? string.Empty;
        _loggerFactory = Settings.LoggerFactory;

        if (Settings.EnableDebugMode && _loggerFactory != null)
        {
            TraceHelper.Activate(_loggerFactory);
        }

        _httpClientFactory = CreateHttpClientFactory(settings);
    }

    /// <summary>
    /// Gets the settings used by this client.
    /// </summary>
    public ClickHouseClientSettings Settings { get; }

    /// <summary>
    /// Gets the type settings for serialization.
    /// </summary>
    internal TypeSettings TypeSettings => new(Settings.UseCustomDecimals, Settings.ReadStringsAsByteArrays, _jsonTypeRegistry, Settings.JsonReadMode, Settings.JsonWriteMode);

    /// <summary>
    /// Gets the server URI.
    /// </summary>
    internal Uri ServerUri => _serverUri;

    /// <inheritdoc />
    public async Task<bool> PingAsync(QueryOptions queryOptions = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var pingUri = new Uri(_serverUri, "ping");
            using var request = new HttpRequestMessage(HttpMethod.Get, pingUri);
            AddDefaultHttpHeaders(request.Headers, queryOptions);

            using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex)
        {
            GetLogger(ClickHouseLogCategories.Connection)?.LogWarning(ex, "Ping to {Endpoint} failed.", _serverUri);
            return false;
        }
    }

    /// <inheritdoc />
    public void RegisterJsonSerializationType<T>()
        where T : class
        => _jsonTypeRegistry.RegisterType<T>();

    /// <inheritdoc />
    public void RegisterJsonSerializationType(Type type)
        => _jsonTypeRegistry.RegisterType(type);

    /// <inheritdoc/>
    public ClickHouseConnection CreateConnection()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public Task<int> ExecuteNonQueryAsync(
        string sql,
        IEnumerable<ClickHouseDbParameter> parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public Task<T> ExecuteScalarAsync<T>(
        string sql,
        IEnumerable<ClickHouseDbParameter> parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public Task<ClickHouseDataReader> ExecuteReaderAsync(
        string sql,
        IEnumerable<ClickHouseDbParameter> parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        // Will be implemented in Phase 2
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public Task<ClickHouseRawResult> ExecuteRawResultAsync(
        string sql,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        // Will be implemented in Phase 2
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public Task<long> InsertBinaryAsync(
        string table,
        IEnumerable<string> columns,
        IEnumerable<object[]> rows,
        InsertOptions options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public Task<HttpResponseMessage> InsertRawStreamAsync(
        string table,
        Stream stream,
        string format,
        IEnumerable<string> columns = null,
        bool useCompression = true,
        InsertOptions options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public async Task<HttpResponseMessage> PostStreamAsync(string sql, Stream data, bool isCompressed, CancellationToken token, QueryOptions queryOptions = null)
    {
        var content = new StreamContent(data);
        return await PostStreamAsync(sql, content, isCompressed, queryOptions, token).ConfigureAwait(false);
    }


    /// <inheritdoc />
    public async Task<HttpResponseMessage> PostStreamAsync(string sql, Func<Stream, CancellationToken, Task> callback, bool isCompressed, CancellationToken token, QueryOptions queryOptions = null)
    {
        var content = new StreamCallbackContent(callback, token);
        return await PostStreamAsync(sql, content, isCompressed, queryOptions, token).ConfigureAwait(false);
    }

    private async Task<HttpResponseMessage> PostStreamAsync(string sql, HttpContent content, bool isCompressed, QueryOptions queryOptions, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Releases all resources used by the client.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var d in _disposables)
        {
            d.Dispose();
        }

        GetLogger(ClickHouseLogCategories.Connection)?.LogDebug("ClickHouseClient disposed.");
    }

    /// <summary>
    /// Gets a logger for the specified category name.
    /// </summary>
    internal ILogger GetLogger(string categoryName)
    {
        if (_loggerFactory == null)
            return null;

        return _loggerCache.GetOrAdd(
            categoryName,
            key => new Lazy<ILogger>(() => _loggerFactory.CreateLogger(key))).Value;
    }

    /// <summary>
    /// Gets an HTTP client from the factory.
    /// </summary>
    internal HttpClient HttpClient => _httpClientFactory.CreateClient(_httpClientName);

    /// <summary>
    /// Sends an HTTP request.
    /// </summary>
    internal async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        HttpCompletionOption completionOption,
        CancellationToken cancellationToken)
    {
        return await HttpClient.SendAsync(request, completionOption, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a URI builder for the specified SQL query.
    /// </summary>
    internal ClickHouseUriBuilder CreateUriBuilder(string sql = null, QueryOptions queryOverride = null)
    {
        var customSettings = Settings.CustomSettings ?? new Dictionary<string, object>();
        var queryParams = new Dictionary<string, object>(customSettings);

        // TODO move settings merging into ClickHouseUriBuilder after refactor
        // Merge query-level custom settings
        if (queryOverride?.CustomSettings != null)
        {
            foreach (var kvp in queryOverride.CustomSettings)
            {
                queryParams[kvp.Key] = kvp.Value;
            }
        }

        string sessionId = Settings.UseSession ? Settings.SessionId : null;
        if (queryOverride != null && queryOverride.UseSession != null)
        {
            // Prioritize query-level setting
            sessionId = queryOverride.UseSession.Value ? queryOverride.SessionId : null;
        }

        return new ClickHouseUriBuilder(_serverUri)
        {
            Database = queryOverride?.Database ?? Settings.Database,
            SessionId = sessionId,
            UseCompression = Settings.UseCompression,
            ConnectionQueryStringParameters = queryParams,
            ConnectionRoles = queryOverride?.Roles ?? Settings.Roles,
            Sql = sql,
            JsonReadMode = Settings.JsonReadMode,
            JsonWriteMode = Settings.JsonWriteMode,
        };
    }

    /// <summary>
    /// Adds default HTTP headers to a request.
    /// </summary>
    internal void AddDefaultHttpHeaders(HttpRequestHeaders headers, QueryOptions queryOverride)
    {
        var userAgentInfo = UserAgentProvider.Info;

        // Priority: override > connection-level bearer token > basic auth
        var bearerToken = queryOverride?.BearerToken ?? Settings.BearerToken;
        if (!string.IsNullOrEmpty(bearerToken))
        {
            headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
        }
        else
        {
            headers.Authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"{Settings.Username}:{Settings.Password}")));
        }

        headers.UserAgent.Add(userAgentInfo.DriverProductInfo);
        headers.UserAgent.Add(userAgentInfo.SystemProductInfo);
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

        if (Settings.UseCompression)
        {
            headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
            headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
        }

        // Apply custom headers (blocked headers are silently ignored for security)
        ApplyCustomHeaders(headers, Settings.CustomHeaders);

        // Override
        ApplyCustomHeaders(headers, queryOverride?.CustomHeaders);
    }

    private static void ApplyCustomHeaders(HttpRequestHeaders requestHeaders, IReadOnlyDictionary<string, string> customHeaders)
    {
        if (customHeaders != null)
        {
            foreach (var kvp in customHeaders)
            {
                if (!IsBlockedHeader(kvp.Key))
                {
                    requestHeaders.TryAddWithoutValidation(kvp.Key, kvp.Value);
                }
            }
        }
    }

    /// <summary>
    /// Handles HTTP response errors.
    /// </summary>
    internal static async Task<HttpResponseMessage> HandleError(HttpResponseMessage response, string query, Activity activity)
    {
        if (response.IsSuccessStatusCode)
        {
            activity?.SetSuccess();
            return response;
        }

        var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var ex = ClickHouseServerException.FromServerResponse(error, query);
        activity?.SetException(ex);
        throw ex;
    }

    private IHttpClientFactory CreateHttpClientFactory(ClickHouseClientSettings settings)
    {
        if (settings.HttpClient != null)
        {
            GetLogger(ClickHouseLogCategories.Connection)?.LogInformation("Using provided HttpClient instance.");
            return new CannedHttpClientFactory(settings.HttpClient);
        }

        if (settings.HttpClientFactory != null)
        {
            GetLogger(ClickHouseLogCategories.Connection)?.LogInformation("Using IHttpClientFactory from settings.");
            return settings.HttpClientFactory;
        }

        // Default: create pooled factory
        GetLogger(ClickHouseLogCategories.Connection)?.LogInformation("Creating default pooled HttpClientFactory.");
        var factory = new DefaultPoolHttpClientFactory(settings.SkipServerCertificateValidation)
        {
            Timeout = settings.Timeout,
        };
        _disposables.Add(factory);
        return factory;
    }

    private static bool IsBlockedHeader(string headerName)
    {
        return string.Equals(headerName, "Connection", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(headerName, "Authorization", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(headerName, "User-Agent", StringComparison.OrdinalIgnoreCase);
    }
}
