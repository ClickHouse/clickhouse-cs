using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Copy.Serializer;
using ClickHouse.Driver.Diagnostic;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Http;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Logging;
using ClickHouse.Driver.Poco;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IO;

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
    private const int DefaultMemoryStreamBlockSize = 256 * 1024; // 256 KB
    private const int DefaultMaxSmallPoolFreeBytes = 128 * 1024 * 1024; // 128 MB
    private const int DefaultMaxLargePoolFreeBytes = 512 * 1024 * 1024; // 512 MB

    private readonly List<IDisposable> disposables = new();
    private readonly ConcurrentDictionary<string, Lazy<ILogger>> loggerCache = new();
    private readonly SchemaResolver schemaResolver;
    private readonly JsonTypeRegistry jsonTypeRegistry = new();
    private readonly PocoTypeRegistry pocoTypeRegistry = new();
    private readonly IHttpClientFactory httpClientFactory;
    private readonly string httpClientName;
    private readonly Uri serverUri;
    private readonly ILoggerFactory loggerFactory;
    private readonly UserAgentProvider userAgentProvider;

    private static readonly RecyclableMemoryStreamManager CommonMemoryStreamManager = new(new RecyclableMemoryStreamManager.Options
    {
        MaximumLargePoolFreeBytes = DefaultMaxLargePoolFreeBytes,
        MaximumSmallPoolFreeBytes = DefaultMaxSmallPoolFreeBytes,
        BlockSize = DefaultMemoryStreamBlockSize,
    });

    private readonly RecyclableMemoryStreamManager memoryStreamManager;

    /// <summary>
    /// Gets RecyclableMemoryStreamManager used to create recyclable streams.
    /// </summary>
    /// <remarks>
    /// No longer used: binary inserts now stream directly into the HTTP request body instead of
    /// buffering the payload through a <see cref="RecyclableMemoryStreamManager"/>, so setting this
    /// has no effect. Scheduled for removal in a future version.
    /// </remarks>
    [Obsolete("MemoryStreamManager is no longer used: binary inserts stream directly into the request body and no longer buffer through a RecyclableMemoryStream. This property has no effect and will be removed in a future version.")]
    public RecyclableMemoryStreamManager MemoryStreamManager
    {
        get { return memoryStreamManager ?? CommonMemoryStreamManager; }
        init { memoryStreamManager = value; }
    }

    private bool disposed;

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
        : this(new ClickHouseClientSettings(connectionString)
        {
            HttpClient = httpClient,
        })
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
        {
            HttpClientFactory = httpClientFactory,
            HttpClientName = httpClientName,
        })
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

        serverUri = new UriBuilder(Settings.Protocol, Settings.Host, Settings.Port, Settings.Path ?? string.Empty).Uri;
        httpClientName = Settings.HttpClientName ?? string.Empty;
        loggerFactory = Settings.LoggerFactory;

        if (Settings.EnableDebugMode && loggerFactory != null)
        {
            TraceHelper.Activate(loggerFactory);
        }

        httpClientFactory = CreateHttpClientFactory(settings);
        schemaResolver = new SchemaResolver(this);
        userAgentProvider = new UserAgentProvider(Settings.ApplicationInfo);
    }

    /// <summary>
    /// Gets the settings used by this client.
    /// </summary>
    public ClickHouseClientSettings Settings { get; }

    internal string RedactedConnectionString
    {
        get
        {
            var builder = ConnectionStringBuilder;
            builder.Password = "****";
            return builder.ToString();
        }
    }

    internal ClickHouseConnectionStringBuilder ConnectionStringBuilder => ClickHouseConnectionStringBuilder.FromSettings(Settings);

    /// <summary>
    /// Gets the type settings for serialization.
    /// </summary>
    internal TypeSettings TypeSettings => new(Settings.UseCustomDecimals, Settings.ReadStringsAsByteArrays, jsonTypeRegistry, Settings.JsonReadMode, Settings.JsonWriteMode, Settings.MapReadMode, Settings.AllowDuplicateJsonKeys);

    /// <summary>
    /// Gets the per-client POCO type registry shared by binary insert and read materialization.
    /// </summary>
    internal PocoTypeRegistry PocoRegistry => pocoTypeRegistry;

    /// <summary>
    /// Gets the server URI.
    /// </summary>
    internal Uri ServerUri => serverUri;

    /// <inheritdoc />
    public async Task<bool> PingAsync(QueryOptions queryOptions = null, CancellationToken cancellationToken = default)
    {
        try
        {
            // Append to the configured path instead of resolving "ping" as a relative reference:
            // relative resolution replaces the last segment, dropping a reverse-proxy prefix
            // configured via the Path setting (e.g. http://host:8123/ch -> http://host:8123/ping).
            var pingUri = new UriBuilder(serverUri) { Path = serverUri.AbsolutePath.TrimEnd('/') + "/ping" }.Uri;
            using var request = new HttpRequestMessage(HttpMethod.Get, pingUri);
            AddDefaultHttpHeaders(request.Headers, queryOptions);

            using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false);

            // No response-decompression seam here on purpose: /ping is judged purely by its status code
            // and its body is never read, so a transport-compressed body would never be misinterpreted.
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex)
        {
            GetLogger(ClickHouseLogCategories.Connection)?.LogWarning(ex, "Ping to {Endpoint} failed.", serverUri);
            return false;
        }
    }

    /// <inheritdoc />
    public void RegisterJsonSerializationType<T>()
        where T : class
        => jsonTypeRegistry.RegisterType<T>();

    /// <inheritdoc />
    public void RegisterJsonSerializationType(Type type)
        => jsonTypeRegistry.RegisterType(type);

    /// <inheritdoc />
    public void RegisterBinaryInsertType<T>()
        where T : class
        => pocoTypeRegistry.RegisterForInsert<T>(GetLogger(ClickHouseLogCategories.Client));

    /// <inheritdoc />
    public void RegisterPocoType<T>()
        where T : class
        => pocoTypeRegistry.RegisterForBoth<T>(GetLogger(ClickHouseLogCategories.Client));

    /// <inheritdoc/>
    public ClickHouseConnection CreateConnection()
    {
        return new ClickHouseConnection(this);
    }

    /// <inheritdoc />
    public async Task<int> ExecuteNonQueryAsync(
        string sql,
        ClickHouseParameterCollection parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        var result = await PostSqlQueryAsync(sql, parameters, options, cancellationToken: cancellationToken).ConfigureAwait(false);

        // The row count is fully consumed here, so this method owns the response: release it (and with it
        // the pooled connection) before returning. ExtendedBinaryReader does not propagate Dispose to the
        // response stream, so relying on the reader is not enough.
        using var response = result.HttpResponseMessage;
        var rawStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);

        // leaveOpen: the HTTP response owns the transport stream; we only own the decoder we add.
        var plaintext = ResponseDecompression.Wrap(rawStream, response, leaveOpen: true);
        var decompressor = ReferenceEquals(plaintext, rawStream) ? null : plaintext;
        try
        {
            using var reader = new ExtendedBinaryReader(plaintext);

            return reader.PeekChar() != -1 ? reader.Read7BitEncodedInt() : 0;
        }
        finally
        {
            decompressor?.Dispose();
        }
    }

    /// <inheritdoc />
    public async Task<object> ExecuteScalarAsync(
        string sql,
        ClickHouseParameterCollection parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        using var reader = await ExecuteReaderAsync(sql, parameters, options, cancellationToken).ConfigureAwait(false);
        return reader.Read() ? reader.GetValue(0) : null;
    }

    /// <inheritdoc />
    public async Task<ClickHouseDataReader> ExecuteReaderAsync(
        string sql,
        ClickHouseParameterCollection parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        var result = await PostSqlQueryAsync(sql, parameters, options, cancellationToken: cancellationToken).ConfigureAwait(false);
        var converter = options?.ReadValueConverter ?? Settings.ReadValueConverter;
        return await ClickHouseDataReader.FromHttpResponseAsync(result.HttpResponseMessage, TypeSettings, pocoTypeRegistry, Settings.ReadBufferSize, converter).ConfigureAwait(false);
    }

    /// <param name="rawBody">
    /// Marks a request whose response body is handed to the caller verbatim rather than parsed by the
    /// driver, which suppresses the default <c>Accept-Encoding</c>. See
    /// <see cref="AddDefaultHttpHeaders(HttpRequestHeaders, QueryOptions, bool)"/>.
    /// </param>
    internal async Task<QueryResult> PostSqlQueryAsync(
        string sql,
        ClickHouseParameterCollection parameters = null,
        QueryOptions options = null,
        bool rawBody = false,
        CancellationToken cancellationToken = default)
    {
        using var activity = this.StartActivity("PostSqlQueryAsync");

        var uriBuilder = CreateUriBuilder(queryOverride: options);

        var logger = GetLogger(ClickHouseLogCategories.Command);
        var isDebugLoggingEnabled = logger?.IsEnabled(LogLevel.Debug) ?? false;
        Stopwatch stopwatch = null;
        if (isDebugLoggingEnabled)
        {
            stopwatch = Stopwatch.StartNew();
            logger.LogDebug("Executing SQL query. QueryId: {QueryId}", uriBuilder.GetEffectiveQueryId());
        }

        using var postMessage = Settings.UseFormDataParameters
            ? BuildHttpRequestMessageWithFormData(
                sql,
                parameters,
                uriBuilder,
                options,
                rawBody)
            : BuildHttpRequestMessageWithQueryParams(
                sql,
                parameters,
                uriBuilder,
                options,
                rawBody);

        activity.SetQuery(sql);

        HttpResponseMessage response = null;
        try
        {
            response = await SendAsync(postMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);

            var handled = await HandleError(response, sql, activity).ConfigureAwait(false);
            var result = new QueryResult(handled);

            if (isDebugLoggingEnabled)
            {
                LogQuerySuccess(stopwatch, uriBuilder.GetEffectiveQueryId(), logger, result.QueryStats);
            }

            activity.SetQueryStats(result.QueryStats);

            return result;
        }
        catch (Exception ex)
        {
            // Nothing is handed back to the caller on this path, so the response (and its pooled
            // connection) would otherwise stay alive until finalization.
            response?.Dispose();
            logger?.LogError(ex, "Query (QueryId: {QueryId}) failed.", uriBuilder.GetEffectiveQueryId());
            activity?.SetException(ex);
            throw;
        }
    }

    private HttpRequestMessage BuildHttpRequestMessageWithQueryParams(string sqlQuery, ClickHouseParameterCollection parameters, ClickHouseUriBuilder uriBuilder, QueryOptions queryOptions, bool rawBody)
    {
        if (parameters != null)
        {
            var resolvedTypeNames = parameters.ResolveTypeNames(sqlQuery, queryOptions?.ParameterTypeResolver ?? Settings.ParameterTypeResolver);
            var customFormatter = queryOptions?.ParameterFormatter ?? Settings.ParameterFormatter;
            sqlQuery = parameters.ReplacePlaceholders(sqlQuery, resolvedTypeNames);
            foreach (ClickHouseDbParameter parameter in parameters)
            {
                resolvedTypeNames.TryGetValue(parameter.ParameterName, out var resolvedTypeName);
                uriBuilder.AddSqlQueryParameter(
                    parameter.ParameterName,
                    HttpParameterFormatter.Format(parameter, resolvedTypeName, TypeSettings, customFormatter));
            }
        }

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        AddDefaultHttpHeaders(postMessage.Headers, queryOptions, rawBody);
        HttpContent content = new StringContent(sqlQuery);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/sql");
        if (Settings.UseCompression)
        {
            content = new CompressedContent(content, DecompressionMethods.GZip);
        }

        postMessage.Content = content;

        return postMessage;
    }

    private HttpRequestMessage BuildHttpRequestMessageWithFormData(string sqlQuery, ClickHouseParameterCollection parameters, ClickHouseUriBuilder uriBuilder, QueryOptions queryOptions, bool rawBody)
    {
        var content = new MultipartFormDataContent();

        if (parameters != null)
        {
            var resolvedTypeNames = parameters.ResolveTypeNames(sqlQuery, queryOptions?.ParameterTypeResolver ?? Settings.ParameterTypeResolver);
            var customFormatter = queryOptions?.ParameterFormatter ?? Settings.ParameterFormatter;
            sqlQuery = parameters.ReplacePlaceholders(sqlQuery, resolvedTypeNames);

            foreach (ClickHouseDbParameter parameter in parameters)
            {
                resolvedTypeNames.TryGetValue(parameter.ParameterName, out var resolvedTypeName);
                content.Add(
                    content: new StringContent(HttpParameterFormatter.Format(parameter, resolvedTypeName, TypeSettings, customFormatter)),
                    name: $"param_{parameter.ParameterName}");
            }
        }

        content.Add(
            content: new StringContent(sqlQuery),
            name: "query");

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        AddDefaultHttpHeaders(postMessage.Headers, queryOptions, rawBody);

        postMessage.Content = content;

        return postMessage;
    }

    private static void LogQuerySuccess(Stopwatch stopwatch, string queryId, ILogger logger, QueryStats queryStats)
    {
        stopwatch.Stop();
        logger.LogDebug(
            "Query (QueryId: {QueryId}) succeeded in {ElapsedMilliseconds:F2} ms. Query Stats: {QueryStats}",
            queryId,
            stopwatch.Elapsed.TotalMilliseconds,
            queryStats);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<T> QueryAsync<T>(
        string sql,
        ClickHouseParameterCollection parameters = null,
        QueryOptions options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        var reader = await ExecuteReaderAsync(sql, parameters, options, cancellationToken).ConfigureAwait(false);
        try
        {
            // reader.Read()/TryMaterializeNextRow are sync because ClickHouseDataReader has no async overload —
            // the underlying HTTP stream is buffered, so per-row reads do not perform real I/O.
            if (reader.TryGetRowMaterializer<T>(out var materializers, out var constructor))
            {
                // Bypasses the reader's column slots and the boxing/unboxing MapTo<T> setter.
                while (reader.TryMaterializeNextRow(materializers, constructor, out var row))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return row;
                }
            }
            else
            {
                while (reader.Read())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return reader.MapTo<T>();
                }
            }
        }
        finally
        {
            reader.Dispose();
        }
    }

    /// <inheritdoc />
    public async Task<ClickHouseRawResult> ExecuteRawResultAsync(
        string sql,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        var response = await PostSqlQueryAsync(sql, null, options, rawBody: true, cancellationToken).ConfigureAwait(false);
        return new ClickHouseRawResult(response.HttpResponseMessage);
    }

    private async Task<int> SendBatchAsync(string destinationTable, Batch batch, BatchSerializer serializer, InsertOptions insertOptions, Action<long> onBatchSent, CancellationToken token)
    {
        var logger = GetLogger(ClickHouseLogCategories.Client);

        using (batch) // Dispose object regardless whether sending succeeds
        {
            token.ThrowIfCancellationRequested();
            var compressor = insertOptions.Compressor;

            // Async sending
            logger?.LogDebug("Sending batch of {Rows} rows to {Table}.", batch.Size, destinationTable);

            // Serialize the (optionally compressed) batch straight into the request stream instead of
            // first materializing the whole payload into a rented MemoryStream and seeking back to the
            // start. A serialization failure is captured and rethrown so callers still observe the
            // original ClickHouseBulkCopySerializationException rather than a transport-level wrapper.
            // With InsertQueryPlacement.Url the statement travels as the query URL parameter, where
            // proxies and access logs can read it, and the serializer leaves it out of the body.
            var queryPlacement = insertOptions.QueryPlacement;
            var urlQuery = queryPlacement == InsertQueryPlacement.Url ? batch.Query : null;

            ExceptionDispatchInfo serializationError = null;
            try
            {
                using var response = await PostStreamAsync(
                    urlQuery,
                    (stream, ct) =>
                    {
                        ct.ThrowIfCancellationRequested();
                        try
                        {
                            serializer.Serialize(batch, stream, compressor, queryPlacement);
                        }
                        catch (Exception ex)
                        {
                            serializationError = ExceptionDispatchInfo.Capture(ex);
                            throw;
                        }

                        return Task.CompletedTask;
                    },
                    compressor?.ContentEncoding,
                    insertOptions,
                    token).ConfigureAwait(false);
            }
            catch
            {
                RethrowSerializationError(serializationError);
                throw;
            }

            onBatchSent?.Invoke(batch.Size);

            logger?.LogDebug("Batch sent to {Table}. Rows in batch: {BatchRows}.", destinationTable, batch.Size);
            return batch.Size;
        }
    }

    // Now that serialization runs directly against the request-body stream, a transport-level write
    // failure (connection reset, cancellation) throws from inside serializer.Serialize and gets wrapped
    // into a ClickHouseBulkCopySerializationException with the failing row attached. Surfacing that would
    // both misreport a transport error as a serialization error and leak row contents into the exception
    // (and any logs built from it). So only rethrow the captured error when it's a genuine serialization
    // fault; for a transport/cancellation cause, let the original transport exception propagate instead.
    internal static void RethrowSerializationError(ExceptionDispatchInfo serializationError)
    {
        if (serializationError is null)
            return;

        if (serializationError.SourceException is ClickHouseBulkCopySerializationException wrapper &&
            IsTransportException(wrapper.InnerException))
        {
            ExceptionDispatchInfo.Capture(wrapper.InnerException).Throw();
        }

        serializationError.Throw();
    }

    // ObjectDisposedException is included because the client already treats it as a transport failure
    // mode when the underlying stream is torn down mid-operation (see DrainAndDisposeAsync); a request
    // stream disposed mid-write surfaces the same way.
    private static bool IsTransportException(Exception exception) =>
        exception is IOException or OperationCanceledException or ObjectDisposedException;

    /// <inheritdoc />
    public Task<long> InsertBinaryAsync(
        string table,
        IEnumerable<string> columns,
        IEnumerable<object[]> rows,
        InsertOptions options = default,
        CancellationToken cancellationToken = default)
    {
        return InsertBinaryAsync(table, columns, rows, options, onBatchSent: null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> InsertBinaryAsync<T>(
        string table,
        IEnumerable<T> rows,
        InsertOptions options = default,
        CancellationToken cancellationToken = default)
        where T : class
    {
        if (table is null)
            throw new InvalidOperationException($"{nameof(table)} is null");
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));

        var mapping = pocoTypeRegistry.GetInsertMapping<T>()
            ?? throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not registered for binary insert. " +
                $"Call RegisterBinaryInsertType<{typeof(T).Name}>() first.");

        var properties = mapping.Properties;

        options = ApplyPocoColumnAttributes(mapping, options);

        if (mapping.ColumnTypes == null && Array.Exists(properties, p => p.ExplicitClickHouseType != null))
        {
            GetLogger(ClickHouseLogCategories.Client)?.LogWarning(
                "Type '{TypeName}' has [ClickHouseColumn(Type)] on some properties but not all. " +
                "The schema probe will not be skipped. To skip it, add explicit types to all mapped properties.",
                typeof(T).Name);
        }

        return InsertBinaryPocoAsync(table, rows, properties, mapping.Getters, options, cancellationToken);
    }

    /// <summary>
    /// Applies column types from the POCO attribute mapping to the insert options,
    /// allowing the schema probe to be skipped when all properties declare explicit ClickHouse types.
    /// User-provided <see cref="InsertOptions.ColumnTypes"/> always takes precedence over attribute-derived types.
    /// </summary>
    private static InsertOptions ApplyPocoColumnAttributes(PocoInsertMapping mapping, InsertOptions options)
    {
        // User-provided ColumnTypes on InsertOptions always takes precedence
        if (options?.ColumnTypes is { Count: > 0 })
            return options;

        // Apply pre-built column types (non-null only when ALL properties have explicit types)
        if (mapping.ColumnTypes != null)
            return (options ?? new InsertOptions()).WithColumnTypes(mapping.ColumnTypes);

        // Otherwise we're gonna get the schema with a probe query
        return options;
    }

    /// <summary>
    /// Resolved insert metadata shared by both the <c>object[]</c> and POCO insert paths.
    /// Produced by <see cref="PrepareInsertAsync"/> after validation and schema resolution.
    /// </summary>
    private readonly struct InsertPlan
    {
        /// <summary>The finalized options (defaulted and validated).</summary>
        public InsertOptions Options { get; init; }

        /// <summary>The resolved ClickHouse column types, ordered to match the INSERT column list.</summary>
        public ClickHouseType[] ColumnTypes { get; init; }

        /// <summary>The full INSERT query including column list and FORMAT clause.</summary>
        public string Query { get; init; }

        /// <summary>Base query ID from which per-batch IDs are derived.</summary>
        public string BaseQueryId { get; init; }
    }

    /// <summary>
    /// Validates insert options, resolves table schema, and builds the INSERT query.
    /// Shared setup for both <see cref="InsertBinaryAsync(string, IEnumerable{string}, IEnumerable{object[]}, InsertOptions, CancellationToken)"/>
    /// and <see cref="InsertBinaryAsync{T}(string, IEnumerable{T}, InsertOptions, CancellationToken)"/>.
    /// </summary>
    private async Task<InsertPlan> PrepareInsertAsync(
        string table, IEnumerable<string> columns, InsertOptions options)
    {
        options ??= new InsertOptions();

        if (options.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "BatchSize must be greater than zero");
        if (options.MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxDegreeOfParallelism must be greater than zero");
        // A cast or a configuration binding can put a value outside the enum into QueryPlacement. Both
        // send paths derive the URL parameter and the body framing from it separately, so a value that
        // is neither Body nor Url would leave the statement out of both. Reject it here, where every
        // insert passes, rather than sending a request the server cannot read.
        if (options.QueryPlacement is not InsertQueryPlacement.Body and not InsertQueryPlacement.Url)
            throw new ArgumentOutOfRangeException(nameof(options), options.QueryPlacement, "QueryPlacement must be InsertQueryPlacement.Body or InsertQueryPlacement.Url");

        var useSession = options.UseSession ?? Settings.UseSession;
        if (useSession && options.MaxDegreeOfParallelism > 1)
        {
            throw new InvalidOperationException(
                $"InsertBinaryAsync is configured with MaxDegreeOfParallelism={options.MaxDegreeOfParallelism} while sessions are enabled. " +
                "ClickHouse only allows one concurrent query per session. " +
                "Set MaxDegreeOfParallelism to 1, or disable sessions for this insert by setting InsertOptions.UseSession to false.");
        }

        var logger = GetLogger(ClickHouseLogCategories.Client);
        logger?.LogDebug("Loading metadata for table {Table}.", table);

        var (columnNames, columnTypes) = await schemaResolver.ResolveAsync(table, columns, options).ConfigureAwait(false);
        if (columnNames == null || columnTypes == null)
            throw new InvalidOperationException("Column names not initialized. Initialization failed.");

        if (logger?.IsEnabled(LogLevel.Debug) ?? false)
        {
            logger.LogDebug("Metadata loaded for table {Table}. Columns: {Columns}.", table, string.Join(", ", columnNames ?? Array.Empty<string>()));
        }

        var query = $"INSERT INTO {table} ({string.Join(", ", columnNames)}) FORMAT {options.Format.ToString()}";
        var baseQueryId = options.QueryId ?? Guid.NewGuid().ToString();

        return new InsertPlan
        {
            Options = options,
            ColumnTypes = columnTypes,
            Query = query,
            BaseQueryId = baseQueryId,
        };
    }

    private async Task<long> InsertBinaryPocoAsync<T>(
        string table,
        IEnumerable<T> rows,
        PocoPropertyInfo[] properties,
        Func<T, object>[] getters,
        InsertOptions options,
        CancellationToken cancellationToken)
        where T : class
    {
        var plan = await PrepareInsertAsync(table, properties.Select(x => x.ColumnName), options).ConfigureAwait(false);
        var serializer = PocoBatchSerializer.GetByRowBinaryFormat(plan.Options.Format);

        // The RowBinary fast path writes each column through a compiled, box-free delegate. The
        // RowBinaryWithDefaults path must inspect a boxed DBDefault sentinel per value, so it stays on
        // the boxed getters and gets no writer delegates.
        var writers = plan.Options.Format == RowBinaryFormat.RowBinary
            ? pocoTypeRegistry.GetOrBuildWriters<T>(properties, getters, plan.ColumnTypes)
            : null;

        int queryIdCounter = 0;

        var logger = GetLogger(ClickHouseLogCategories.Client);
        var isDebugLoggingEnabled = logger?.IsEnabled(LogLevel.Debug) ?? false;
        Stopwatch stopwatch = null;
        if (isDebugLoggingEnabled)
        {
            stopwatch = Stopwatch.StartNew();
            logger.LogDebug("Starting bulk copy into {Table} with batch size {BatchSize} and degree {Degree}.", table, plan.Options.BatchSize, plan.Options.MaxDegreeOfParallelism);
        }

        long totalRowsWritten = 0;
        var batches = IntoPocoBatches(rows, plan);

        await Parallel.ForEachAsync(
            batches,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = plan.Options.MaxDegreeOfParallelism,
                CancellationToken = cancellationToken,
            },
            async (batch, ct) =>
            {
                var batchOptions = plan.Options.WithQueryId($"{plan.BaseQueryId}-{Interlocked.Increment(ref queryIdCounter)}"); // Avoid duplicate query ids across batches
                var count = await SendPocoBatchAsync(table, batch, getters, writers, serializer, batchOptions, ct).ConfigureAwait(false);
                Interlocked.Add(ref totalRowsWritten, count);
            }).ConfigureAwait(false);

        if (isDebugLoggingEnabled)
        {
            stopwatch.Stop();
            logger.LogDebug("Bulk copy into {Table} completed in {ElapsedMilliseconds:F2} ms. Total rows: {Rows}.", table, stopwatch.Elapsed.TotalMilliseconds, totalRowsWritten);
        }

        return totalRowsWritten;
    }

    private async Task<int> SendPocoBatchAsync<T>(string destinationTable, PocoBatch<T> batch, Func<T, object>[] getters, Action<T, ExtendedBinaryWriter>[] writers, PocoBatchSerializer serializer, InsertOptions insertOptions, CancellationToken token)
    {
        var logger = GetLogger(ClickHouseLogCategories.Client);

        using (batch)
        {
            token.ThrowIfCancellationRequested();
            var compressor = insertOptions.Compressor;

            logger?.LogDebug("Sending batch of {Rows} rows to {Table}.", batch.Size, destinationTable);

            // Stream the (optionally compressed) batch straight into the request stream (see
            // SendBatchAsync for the serialization-error capture and query-placement rationale).
            var queryPlacement = insertOptions.QueryPlacement;
            var urlQuery = queryPlacement == InsertQueryPlacement.Url ? batch.Query : null;

            ExceptionDispatchInfo serializationError = null;
            try
            {
                using var response = await PostStreamAsync(
                    urlQuery,
                    (stream, ct) =>
                    {
                        ct.ThrowIfCancellationRequested();
                        try
                        {
                            serializer.Serialize(batch, getters, writers, stream, compressor, queryPlacement);
                        }
                        catch (Exception ex)
                        {
                            serializationError = ExceptionDispatchInfo.Capture(ex);
                            throw;
                        }

                        return Task.CompletedTask;
                    },
                    compressor?.ContentEncoding,
                    insertOptions,
                    token).ConfigureAwait(false);
            }
            catch
            {
                RethrowSerializationError(serializationError);
                throw;
            }

            logger?.LogDebug("Batch sent to {Table}. Rows in batch: {BatchRows}.", destinationTable, batch.Size);
            return batch.Size;
        }
    }

    private static IEnumerable<PocoBatch<T>> IntoPocoBatches<T>(IEnumerable<T> rows, InsertPlan plan)
    {
        foreach (var (batch, size) in rows.BatchRented(plan.Options.BatchSize))
        {
            yield return new PocoBatch<T> { Rows = batch, Size = size, Query = plan.Query, Types = plan.ColumnTypes };
        }
    }

    /// <summary>
    /// Internal version which takes a callback method, to allow us to maintain backwards
    /// compat with the BatchSent event in BulkCopy.
    /// </summary>
    internal async Task<long> InsertBinaryAsync(
        string table,
        IEnumerable<string> columns,
        IEnumerable<object[]> rows,
        InsertOptions options,
        Action<long> onBatchSent,
        CancellationToken cancellationToken)
    {
        if (table is null)
            throw new InvalidOperationException($"{nameof(table)} is null");
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));

        var plan = await PrepareInsertAsync(table, columns, options).ConfigureAwait(false);
        var serializer = BatchSerializer.GetByRowBinaryFormat(plan.Options.Format);
        int queryIdCounter = 0;

        var logger = GetLogger(ClickHouseLogCategories.Client);
        var isDebugLoggingEnabled = logger?.IsEnabled(LogLevel.Debug) ?? false;
        Stopwatch stopwatch = null;
        if (isDebugLoggingEnabled)
        {
            stopwatch = Stopwatch.StartNew();
            logger.LogDebug("Starting bulk copy into {Table} with batch size {BatchSize} and degree {Degree}.", table, plan.Options.BatchSize, plan.Options.MaxDegreeOfParallelism);
        }

        long totalRowsWritten = 0;
        var batches = IntoBatches(rows, plan.Query, plan.ColumnTypes, plan.Options.BatchSize);

        await Parallel.ForEachAsync(
            batches,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = plan.Options.MaxDegreeOfParallelism,
                CancellationToken = cancellationToken,
            },
            async (batch, ct) =>
            {
                var batchOptions = plan.Options.WithQueryId($"{plan.BaseQueryId}-{Interlocked.Increment(ref queryIdCounter)}");
                var count = await SendBatchAsync(table, batch, serializer, batchOptions, onBatchSent, ct).ConfigureAwait(false);
                Interlocked.Add(ref totalRowsWritten, count);
            }).ConfigureAwait(false);

        if (isDebugLoggingEnabled)
        {
            stopwatch.Stop();
            logger.LogDebug("Bulk copy into {Table} completed in {ElapsedMilliseconds:F2} ms. Total rows: {Rows}.", table, stopwatch.Elapsed.TotalMilliseconds, totalRowsWritten);
        }

        return totalRowsWritten;
    }

    private static IEnumerable<Batch> IntoBatches(IEnumerable<object[]> rows, string query, ClickHouseType[] types, int batchSize)
    {
        foreach (var (batch, size) in rows.BatchRented(batchSize))
        {
            yield return new Batch { Rows = batch, Size = size, Query = query, Types = types };
        }
    }

    /// <inheritdoc />
    public async Task<HttpResponseMessage> InsertRawStreamAsync(
        string table,
        Stream stream,
        string format,
        IEnumerable<string> columns = null,
        bool useCompression = true,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name cannot be null or empty", nameof(table));
        if (stream == null)
            throw new ArgumentNullException(nameof(stream));
        if (string.IsNullOrEmpty(format))
            throw new ArgumentException("Format cannot be null or empty", nameof(format));

        var columnList = columns != null ? $"({string.Join(", ", columns)})" : string.Empty;
        var query = $"INSERT INTO {table} {columnList} FORMAT {format}";

        // The request message that carries this content disposes it - and, through StreamContent,
        // the supplied stream - once the request completes, on success and on failure alike.
        HttpContent content = new StreamContent(stream);
        if (useCompression)
        {
            // CompressedContent handles compression and adds Content-Encoding header
            content = new CompressedContent(content, System.Net.DecompressionMethods.GZip);
        }

        // Pass contentEncoding=null since CompressedContent already adds the Content-Encoding header
        return await PostStreamAsync(query, content, contentEncoding: null, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<HttpResponseMessage> PostStreamAsync(string sql, Stream data, bool isCompressed, CancellationToken token, QueryOptions queryOptions = null)
    {
        var content = new StreamContent(data);
        return await PostStreamAsync(sql, content, isCompressed ? "gzip" : null, queryOptions, token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<HttpResponseMessage> PostStreamAsync(string sql, Func<Stream, CancellationToken, Task> callback, bool isCompressed, CancellationToken token, QueryOptions queryOptions = null)
        => PostStreamAsync(sql, callback, isCompressed ? "gzip" : null, queryOptions, token);

    private Task<HttpResponseMessage> PostStreamAsync(string sql, Func<Stream, CancellationToken, Task> callback, string contentEncoding, QueryOptions queryOptions, CancellationToken token)
    {
        var content = new StreamCallbackContent(callback, token);
        return PostStreamAsync(sql, content, contentEncoding, queryOptions, token);
    }

    private async Task<HttpResponseMessage> PostStreamAsync(string sql, HttpContent content, string contentEncoding, QueryOptions queryOptions, CancellationToken token)
    {
        using var activity = this.StartActivity("PostStreamAsync");
        activity.SetQuery(sql);

        var builder = CreateUriBuilder(sql, queryOptions);

        using var postMessage = new HttpRequestMessage(HttpMethod.Post, builder.ToString());
        // rawBody: this method returns the HttpResponseMessage itself — PostStreamAsync and
        // InsertRawStreamAsync are public — so its body belongs to the caller, exactly like a raw result,
        // so it advertises no codec rather than one the caller never asked for and nothing would decode.
        AddDefaultHttpHeaders(postMessage.Headers, queryOptions, rawBody: true);

        postMessage.Content = content;
        postMessage.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        if (!string.IsNullOrEmpty(contentEncoding))
        {
            postMessage.Content.Headers.Add("Content-Encoding", contentEncoding);
        }

        GetLogger(ClickHouseLogCategories.Transport)?.LogDebug("Sending streamed request to {Endpoint} (Content-Encoding: {ContentEncoding}).", serverUri, string.IsNullOrEmpty(contentEncoding) ? "none" : contentEncoding);

        HttpResponseMessage response = null;
        try
        {
            response = await SendAsync(postMessage, HttpCompletionOption.ResponseContentRead, token).ConfigureAwait(false);
            GetLogger(ClickHouseLogCategories.Transport)?.LogDebug("Streamed request to {Endpoint} received response {StatusCode}.", serverUri, response.StatusCode);

            return await HandleError(response, sql, activity).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Ownership never reaches the caller on this path, so release the response here.
            response?.Dispose();
            GetLogger(ClickHouseLogCategories.Transport)?.LogError(ex, "Streamed request to {Endpoint} failed.", serverUri);
            throw;
        }
    }

    /// <summary>
    /// Releases all resources used by the client.
    /// </summary>
    public void Dispose()
    {
        if (disposed)
            return;

        disposed = true;

        foreach (var d in disposables)
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
        if (loggerFactory == null)
            return null;

        return loggerCache.GetOrAdd(
            categoryName,
            key => new Lazy<ILogger>(() => loggerFactory.CreateLogger(key))).Value;
    }

    /// <summary>
    /// Gets an HTTP client from the factory.
    /// </summary>
    internal HttpClient HttpClient => httpClientFactory.CreateClient(httpClientName);

    /// <summary>
    /// Creates a URI builder for the specified SQL query.
    /// </summary>
    internal ClickHouseUriBuilder CreateUriBuilder(string sql = null, QueryOptions queryOverride = null)
    {
        string sessionId = Settings.UseSession ? Settings.SessionId : null;
        if (queryOverride?.UseSession != null)
        {
            // Prioritize query-level setting
            sessionId = queryOverride.UseSession.Value ? queryOverride.SessionId : null;
        }

        // An Accept-Encoding chosen by the caller is meaningless unless the server is also told to honour
        // it via enable_http_compression. Force it on when the caller asks for compression.
        var useCompression = Settings.UseCompression || CarriesACodec(ExplicitAcceptEncoding(queryOverride));

        return new ClickHouseUriBuilder(serverUri)
        {
            Database = queryOverride?.Database ?? Settings.Database,
            SessionId = sessionId,
            UseCompression = useCompression,
            ConnectionQueryStringParameters = Settings.CustomSettings,
            CommandQueryStringParameters = queryOverride?.CustomSettings,
            ConnectionRoles = Settings.Roles,
            CommandRoles = queryOverride?.Roles,
            Sql = sql,
            JsonReadMode = Settings.JsonReadMode,
            JsonWriteMode = Settings.JsonWriteMode,
            QueryId = queryOverride?.QueryId,
            MaxExecutionTime = queryOverride?.MaxExecutionTime,
        };
    }

    /// <summary>
    /// Adds default HTTP headers to a request. <paramref name="rawBody"/> marks a request whose body is
    /// handed to the caller verbatim (<see cref="ExecuteRawResultAsync"/>, <see cref="PostStreamAsync"/>),
    /// which advertises no codec at all rather than the driver's own default: the driver must not change
    /// the shape of bytes it does not itself consume, and nothing else decodes them either now that the
    /// handler's <c>AutomaticDecompression</c> is off. An <c>AcceptEncoding</c> the caller configured
    /// explicitly still applies, to either kind of request — asking for a codec on a verbatim body is how
    /// a caller exports compressed bytes on purpose.
    /// </summary>
    internal void AddDefaultHttpHeaders(HttpRequestHeaders headers, QueryOptions queryOverride = null, bool rawBody = false)
    {
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

        headers.UserAgent.Add(userAgentProvider.DriverProductInfo);
        headers.UserAgent.Add(userAgentProvider.MetadataProductInfo);
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/csv"));
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

        // Client-level Accept-Encoding, then the driver's own default. Both go on *before* custom headers,
        // so that a CustomHeaders["Accept-Encoding"] at either level still wins over them — only the
        // per-query property outranks a custom header, which is the precedence that applied before
        // client-level configuration existed. ClickHouse resolves this header by its own fixed preference
        // order and ignores q-values, so it is a capability announcement, not a demand.
        if (CarriesACodec(Settings.AcceptEncoding))
        {
            ApplyAcceptEncodingOverride(headers, Settings.AcceptEncoding);
        }
        else if (!rawBody && Settings.UseCompression && !CarriesACodec(queryOverride?.AcceptEncoding))
        {
            // The codecs the driver can decode. A body handed over verbatim advertises nothing at all
            // instead (the !rawBody guard above): the driver does not decode it, and no longer relies on
            // the framework to, so any codec offered here would reach the caller still compressed.
            foreach (var token in ResponseDecompression.DefaultAcceptEncoding.Split(','))
            {
                headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(token.Trim()));
            }
        }

        // Apply custom headers (blocked headers are silently ignored for security)
        ApplyCustomHeaders(headers, Settings.CustomHeaders);

        // Override
        ApplyCustomHeaders(headers, queryOverride?.CustomHeaders);

        // A per-query Accept-Encoding replaces whatever was attached above — the client-level value, the
        // default codecs, and any value injected via CustomHeaders — and applies to the raw path too:
        // there the caller has asked for the encoding themselves, so honouring it is not a silent change.
        ApplyAcceptEncodingOverride(headers, queryOverride?.AcceptEncoding);
    }

    /// <summary>
    /// The <c>Accept-Encoding</c> the caller chose, if any: the per-query value first, then the
    /// client-level setting — the same precedence the other per-query hooks (ReadValueConverter,
    /// ParameterFormatter, ParameterTypeResolver) use. <see langword="null"/> means "not chosen", which
    /// is what lets the driver fall back to <see cref="ResponseDecompression.DefaultAcceptEncoding"/>.
    /// Naming a codec explicitly also implies asking for compression, so this drives
    /// <c>enable_http_compression</c> even when <see cref="ClickHouseClientSettings.UseCompression"/> is
    /// false.
    /// </summary>
    private string ExplicitAcceptEncoding(QueryOptions queryOverride)
    {
        var perQuery = queryOverride?.AcceptEncoding;
        return CarriesACodec(perQuery) ? perQuery : Settings.AcceptEncoding;
    }

    /// <summary>
    /// Whether an <c>Accept-Encoding</c> value names at least one codec. A value that is null, empty,
    /// whitespace or only separators counts as "not set", so it falls back to the driver's default rather
    /// than silently turning compression off — which is what clearing the header on such a value would do,
    /// since the server sends <c>identity</c> when no codec is offered.
    /// </summary>
    private static bool CarriesACodec(string acceptEncoding)
    {
        if (string.IsNullOrWhiteSpace(acceptEncoding))
            return false;

        foreach (var entry in acceptEncoding.Split(','))
        {
            if (entry.Trim().Length > 0)
                return true;
        }

        return false;
    }

    private static void ApplyAcceptEncodingOverride(HttpRequestHeaders headers, string acceptEncoding)
    {
        // Guarded on tokens rather than emptiness: a value of "  " or "," would otherwise clear the header
        // and put nothing back, which reads as "no compression" instead of "nothing configured".
        if (!CarriesACodec(acceptEncoding))
            return;

        headers.AcceptEncoding.Clear();
        foreach (var entry in acceptEncoding.Split(','))
        {
            var trimmed = entry.Trim();
            if (trimmed.Length > 0)
                headers.AcceptEncoding.TryParseAdd(trimmed);
        }
    }

    private static void ApplyCustomHeaders(HttpRequestHeaders requestHeaders, IReadOnlyDictionary<string, string> customHeaders)
    {
        if (customHeaders != null)
        {
            foreach (var kvp in customHeaders)
            {
                if (!IsBlockedHeader(kvp.Key))
                {
                    requestHeaders.Remove(kvp.Key);
                    requestHeaders.TryAddWithoutValidation(kvp.Key, kvp.Value);
                }
            }
        }
    }

    /// <summary>
    /// Handles HTTP response errors.
    /// </summary>
    private static async Task<HttpResponseMessage> HandleError(HttpResponseMessage response, string query, Activity activity)
    {
        if (response.IsSuccessStatusCode)
        {
            activity?.SetSuccess();
            return response;
        }

        var error = await ReadErrorBodyAsync(response).ConfigureAwait(false);
        var ex = string.IsNullOrWhiteSpace(error)
            ? CreateEmptyBodyException(response, query)
            : ClickHouseServerException.FromServerResponse(error, query);
        activity?.SetException(ex);
        throw ex;
    }

    /// <summary>
    /// Builds an exception for a non-success HTTP response whose body is empty or whitespace-only.
    /// This happens when an upstream component (a ClickHouse Cloud edge, a load balancer, a proxy)
    /// fails the request before it reaches a ClickHouse node, so there is no server error body to
    /// parse. Surface the HTTP status line — and the <c>X-ClickHouse-Exception-Code</c> header when
    /// present — so the caller still gets actionable diagnostics instead of a blank message.
    /// </summary>
    private static ClickHouseServerException CreateEmptyBodyException(HttpResponseMessage response, string query)
    {
        var exceptionCode = TryGetExceptionCodeHeader(response);

        var message = new StringBuilder("ClickHouse server returned HTTP ")
            .Append(((int)response.StatusCode).ToString(CultureInfo.InvariantCulture));
        if (!string.IsNullOrWhiteSpace(response.ReasonPhrase))
        {
            message.Append(" (").Append(response.ReasonPhrase).Append(')');
        }

        message.Append(" with an empty response body.");
        if (exceptionCode.HasValue)
        {
            message.Append(" X-ClickHouse-Exception-Code: ")
                .Append(exceptionCode.Value.ToString(CultureInfo.InvariantCulture))
                .Append('.');
        }

        return new ClickHouseServerException(message.ToString(), query, exceptionCode ?? -1);
    }

    /// <summary>
    /// Reads the numeric <c>X-ClickHouse-Exception-Code</c> response header, when the server set one.
    /// Returns <see langword="null"/> if the header is absent or not a valid integer.
    /// </summary>
    private static int? TryGetExceptionCodeHeader(HttpResponseMessage response)
    {
        if (response.Headers.TryGetValues("X-ClickHouse-Exception-Code", out var values))
        {
            foreach (var value in values)
            {
                if (int.TryParse(value?.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var code))
                {
                    return code;
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Reads the error body of a non-success HTTP response as a string. When the response is
    /// transport-compressed (because the caller asked for <c>Accept-Encoding</c>), the server
    /// compresses error bodies the same way it would compress data — so we route the body through the
    /// shared <see cref="ResponseDecompression"/> resolver, and fall back to a placeholder for codecs we
    /// can't decode (snappy, …) rather than handing back garbled binary bytes as a string. An error body must never turn a server error into an obscure
    /// decompression crash, so this uses the non-throwing form of the resolver.
    /// </summary>
    private static async Task<string> ReadErrorBodyAsync(HttpResponseMessage response)
    {
        var encoding = ResponseDecompression.GetContentEncoding(response);
        if (string.IsNullOrEmpty(encoding))
        {
            return await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        }

        var rawStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

        if (!ResponseDecompression.TryWrap(rawStream, encoding, leaveOpen: false, out var decompressed))
        {
            // Unknown codec — surfacing the raw compressed bytes as a string is worse than
            // a placeholder. Best-effort drain the body first so HttpClient can reuse the
            // connection when the transport stream supports it.
            await DrainAndDisposeAsync(rawStream).ConfigureAwait(false);
            return
                $"<server returned HTTP {(int)response.StatusCode} {response.ReasonPhrase} with unsupported Content-Encoding: {encoding}. " +
                "The error body is compressed with a codec this client cannot decode (xz, snappy, …); " +
                "please re-run the request without compression or inspect 'system.query_log' on the ClickHouse server to read the original error message.>";
        }

        try
        {
            using (decompressed)
            using (var reader = new StreamReader(decompressed, GetErrorBodyEncoding(response), detectEncodingFromByteOrderMarks: true))
            {
                return await reader.ReadToEndAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Decoding the error body failed — a truncated or corrupt compressed payload, or a custom
            // compressor whose Decompress throws. The server's status line is the information that
            // actually matters, so it must survive: never let a decompression failure replace the
            // ClickHouseServerException with an obscure codec error.
            return
                $"<server returned HTTP {(int)response.StatusCode} {response.ReasonPhrase}, but its error body " +
                $"(Content-Encoding: {encoding}) could not be decoded: {ex.GetType().Name}: {ex.Message}. " +
                "Inspect 'system.query_log' on the ClickHouse server to read the original error message.>";
        }
    }

    private static Encoding GetErrorBodyEncoding(HttpResponseMessage response)
    {
        var charset = response.Content.Headers.ContentType?.CharSet;
        if (string.IsNullOrWhiteSpace(charset))
        {
            return Encoding.UTF8;
        }

        try
        {
            return Encoding.GetEncoding(charset.Trim('"'));
        }
        catch (ArgumentException)
        {
            return Encoding.UTF8;
        }
    }

    private static async Task DrainAndDisposeAsync(Stream stream)
    {
        try
        {
            await stream.CopyToAsync(Stream.Null).ConfigureAwait(false);
        }
        catch (IOException)
        {
            // The placeholder is still more actionable than replacing the server error with
            // a transport-read failure. The connection will not be reusable in this case.
        }
        catch (ObjectDisposedException)
        {
        }
        finally
        {
            stream.Dispose();
        }
    }

    private IHttpClientFactory CreateHttpClientFactory(ClickHouseClientSettings settings)
    {
        IHttpClientFactory factory;
        if (settings.HttpClient != null)
        {
            GetLogger(ClickHouseLogCategories.Connection)?.LogInformation("Using provided HttpClient instance.");
            factory = new CannedHttpClientFactory(settings.HttpClient);
        }
        else if (settings.HttpClientFactory != null)
        {
            GetLogger(ClickHouseLogCategories.Connection)?.LogInformation("Using IHttpClientFactory from settings.");
            factory = settings.HttpClientFactory;
        }
        else
        {
            // Default: create pooled factory
            GetLogger(ClickHouseLogCategories.Connection)?.LogInformation("Creating default pooled HttpClientFactory.");
            var defaultFactory = new DefaultPoolHttpClientFactory(settings.SkipServerCertificateValidation)
            {
                Timeout = settings.Timeout,
            };
            disposables.Add(defaultFactory);
            factory = defaultFactory;
        }

        LoggingHelpers.LogHttpClientConfiguration(GetLogger(ClickHouseLogCategories.Client), factory);

        return factory;
    }

    /// <summary>
    /// Sends an HTTP request
    /// </summary>
    internal async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, HttpCompletionOption completionOption, CancellationToken cancellationToken)
    {
        return await HttpClient.SendAsync(request, completionOption, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsBlockedHeader(string headerName)
    {
        return string.Equals(headerName, "Connection", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(headerName, "Authorization", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(headerName, "User-Agent", StringComparison.OrdinalIgnoreCase);
    }
}
