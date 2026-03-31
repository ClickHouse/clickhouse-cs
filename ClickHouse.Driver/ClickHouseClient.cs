using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Copy.Serializer;
using ClickHouse.Driver.Diagnostic;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Http;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Logging;
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
    private readonly BinaryInsertTypeRegistry binaryInsertTypeRegistry = new();
    private readonly IHttpClientFactory httpClientFactory;
    private readonly string httpClientName;
    private readonly Uri serverUri;
    private readonly ILoggerFactory loggerFactory;

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
    internal TypeSettings TypeSettings => new(Settings.UseCustomDecimals, Settings.ReadStringsAsByteArrays, jsonTypeRegistry, Settings.JsonReadMode, Settings.JsonWriteMode);

    /// <summary>
    /// Gets the server URI.
    /// </summary>
    internal Uri ServerUri => serverUri;

    /// <inheritdoc />
    public async Task<bool> PingAsync(QueryOptions queryOptions = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var pingUri = new Uri(serverUri, "ping");
            using var request = new HttpRequestMessage(HttpMethod.Get, pingUri);
            AddDefaultHttpHeaders(request.Headers, queryOptions);

            using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false);
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
        => binaryInsertTypeRegistry.RegisterType<T>();

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
        var response = await PostSqlQueryAsync(sql, parameters, options, cancellationToken).ConfigureAwait(false);
        using var reader = new ExtendedBinaryReader(await response.HttpResponseMessage.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false));

        return reader.PeekChar() != -1 ? reader.Read7BitEncodedInt() : 0;
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
        var result = await PostSqlQueryAsync(sql, parameters, options, cancellationToken).ConfigureAwait(false);
        return await ClickHouseDataReader.FromHttpResponseAsync(result.HttpResponseMessage, TypeSettings).ConfigureAwait(false);
    }

    internal async Task<QueryResult> PostSqlQueryAsync(
        string sql,
        ClickHouseParameterCollection parameters = null,
        QueryOptions options = null,
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
                options)
            : BuildHttpRequestMessageWithQueryParams(
                sql,
                parameters,
                uriBuilder,
                options);

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
            logger?.LogError(ex, "Query (QueryId: {QueryId}) failed.", uriBuilder.GetEffectiveQueryId());
            activity?.SetException(ex);
            throw;
        }
    }

    private HttpRequestMessage BuildHttpRequestMessageWithQueryParams(string sqlQuery, ClickHouseParameterCollection parameters, ClickHouseUriBuilder uriBuilder, QueryOptions queryOptions)
    {
        if (parameters != null)
        {
            var resolvedTypeNames = parameters.ResolveTypeNames(sqlQuery, queryOptions?.ParameterTypeResolver ?? Settings.ParameterTypeResolver);
            sqlQuery = parameters.ReplacePlaceholders(sqlQuery, resolvedTypeNames);
            foreach (ClickHouseDbParameter parameter in parameters)
            {
                resolvedTypeNames.TryGetValue(parameter.ParameterName, out var resolvedTypeName);
                uriBuilder.AddSqlQueryParameter(
                    parameter.ParameterName,
                    HttpParameterFormatter.Format(parameter, resolvedTypeName, TypeSettings));
            }
        }

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        AddDefaultHttpHeaders(postMessage.Headers, queryOptions);
        HttpContent content = new StringContent(sqlQuery);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/sql");
        if (Settings.UseCompression)
        {
            content = new CompressedContent(content, DecompressionMethods.GZip);
        }

        postMessage.Content = content;

        return postMessage;
    }

    private HttpRequestMessage BuildHttpRequestMessageWithFormData(string sqlQuery, ClickHouseParameterCollection parameters, ClickHouseUriBuilder uriBuilder, QueryOptions queryOptions)
    {
        var content = new MultipartFormDataContent();

        if (parameters != null)
        {
            var resolvedTypeNames = parameters.ResolveTypeNames(sqlQuery, queryOptions?.ParameterTypeResolver ?? Settings.ParameterTypeResolver);
            sqlQuery = parameters.ReplacePlaceholders(sqlQuery, resolvedTypeNames);

            foreach (ClickHouseDbParameter parameter in parameters)
            {
                resolvedTypeNames.TryGetValue(parameter.ParameterName, out var resolvedTypeName);
                content.Add(
                    content: new StringContent(HttpParameterFormatter.Format(parameter, resolvedTypeName, TypeSettings)),
                    name: $"param_{parameter.ParameterName}");
            }
        }

        content.Add(
            content: new StringContent(sqlQuery),
            name: "query");

        var uri = uriBuilder.ToString();

        var postMessage = new HttpRequestMessage(HttpMethod.Post, uri);

        AddDefaultHttpHeaders(postMessage.Headers, queryOptions);

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
    public async Task<ClickHouseRawResult> ExecuteRawResultAsync(
        string sql,
        QueryOptions options = null,
        CancellationToken cancellationToken = default)
    {
        var response = await PostSqlQueryAsync(sql, null, options, cancellationToken).ConfigureAwait(false);
        return new ClickHouseRawResult(response.HttpResponseMessage);
    }

    private async Task<int> SendBatchAsync(string destinationTable, Batch batch, BatchSerializer serializer, InsertOptions insertOptions, Action<long> onBatchSent, CancellationToken token)
    {
        var logger = GetLogger(ClickHouseLogCategories.Client);

        using (batch) // Dispose object regardless whether sending succeeds
        {
            using var stream = MemoryStreamManager.GetStream(nameof(SendBatchAsync), 128 * 1024);
            // Async serialization
            await Task.Run(() => serializer.Serialize(batch, stream), token).ConfigureAwait(false);

            // Seek to beginning as after writing it's at end
            stream.Seek(0, SeekOrigin.Begin);

            // Async sending
            logger?.LogDebug("Sending batch of {Rows} rows to {Table}.", batch.Size, destinationTable);
            await PostStreamAsync(null, stream, true, token, insertOptions).ConfigureAwait(false);

            onBatchSent?.Invoke(batch.Size);

            logger?.LogDebug("Batch sent to {Table}. Rows in batch: {BatchRows}.", destinationTable, batch.Size);
            return batch.Size;
        }
    }

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

        var mapping = binaryInsertTypeRegistry.GetMapping<T>()
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
    private static InsertOptions ApplyPocoColumnAttributes(PocoTypeMapping mapping, InsertOptions options)
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
        BinaryInsertPropertyInfo[] properties,
        Func<T, object>[] getters,
        InsertOptions options,
        CancellationToken cancellationToken)
        where T : class
    {
        var plan = await PrepareInsertAsync(table, properties.Select(x => x.ColumnName), options).ConfigureAwait(false);
        var serializer = PocoBatchSerializer.GetByRowBinaryFormat(plan.Options.Format);
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
        var batches = IntoPocoBatches(rows, plan.Query, plan.ColumnTypes, properties, getters, plan.Options.BatchSize);

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
                var count = await SendPocoBatchAsync(table, batch, serializer, batchOptions, ct).ConfigureAwait(false);
                Interlocked.Add(ref totalRowsWritten, count);
            }).ConfigureAwait(false);

        if (isDebugLoggingEnabled)
        {
            stopwatch.Stop();
            logger.LogDebug("Bulk copy into {Table} completed in {ElapsedMilliseconds:F2} ms. Total rows: {Rows}.", table, stopwatch.Elapsed.TotalMilliseconds, totalRowsWritten);
        }

        return totalRowsWritten;
    }

    private async Task<int> SendPocoBatchAsync<T>(string destinationTable, PocoBatch<T> batch, PocoBatchSerializer serializer, InsertOptions insertOptions, CancellationToken token)
    {
        var logger = GetLogger(ClickHouseLogCategories.Client);

        using (batch)
        {
            using var stream = MemoryStreamManager.GetStream(nameof(SendPocoBatchAsync), 128 * 1024);
            await Task.Run(() => serializer.Serialize(batch, stream), token).ConfigureAwait(false);

            stream.Seek(0, SeekOrigin.Begin);

            logger?.LogDebug("Sending batch of {Rows} rows to {Table}.", batch.Size, destinationTable);
            await PostStreamAsync(null, stream, true, token, insertOptions).ConfigureAwait(false);

            logger?.LogDebug("Batch sent to {Table}. Rows in batch: {BatchRows}.", destinationTable, batch.Size);
            return batch.Size;
        }
    }

    private static IEnumerable<PocoBatch<T>> IntoPocoBatches<T>(IEnumerable<T> rows, string query, ClickHouseType[] types, BinaryInsertPropertyInfo[] properties, Func<T, object>[] getters, int batchSize)
    {
        foreach (var (batch, size) in rows.BatchRented(batchSize))
        {
            yield return new PocoBatch<T> { Rows = batch, Size = size, Query = query, Types = types, Properties = properties, Getters = getters };
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

        HttpContent content = new StreamContent(stream);
        if (useCompression)
        {
            // CompressedContent handles compression and adds Content-Encoding header
            content = new CompressedContent(content, System.Net.DecompressionMethods.GZip);
        }

        // Pass isCompressed=false since CompressedContent already adds the Content-Encoding header
        try
        {
            return await PostStreamAsync(query, content, isCompressed: false, options, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            content.Dispose();
            throw;
        }
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
        using var activity = this.StartActivity("PostStreamAsync");
        activity.SetQuery(sql);

        var builder = CreateUriBuilder(sql, queryOptions);

        using var postMessage = new HttpRequestMessage(HttpMethod.Post, builder.ToString());
        AddDefaultHttpHeaders(postMessage.Headers, queryOptions);

        postMessage.Content = content;
        postMessage.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        if (isCompressed)
        {
            postMessage.Content.Headers.Add("Content-Encoding", "gzip");
        }

        GetLogger(ClickHouseLogCategories.Transport)?.LogDebug("Sending streamed request to {Endpoint} (Compressed: {Compressed}).", serverUri, isCompressed);

        try
        {
            var response = await SendAsync(postMessage, HttpCompletionOption.ResponseContentRead, token).ConfigureAwait(false);
            GetLogger(ClickHouseLogCategories.Transport)?.LogDebug("Streamed request to {Endpoint} received response {StatusCode}.", serverUri, response.StatusCode);

            return await HandleError(response, sql, activity).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
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

        return new ClickHouseUriBuilder(serverUri)
        {
            Database = queryOverride?.Database ?? Settings.Database,
            SessionId = sessionId,
            UseCompression = Settings.UseCompression,
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
    /// Adds default HTTP headers to a request.
    /// </summary>
    internal void AddDefaultHttpHeaders(HttpRequestHeaders headers, QueryOptions queryOverride = null)
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

        var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        var ex = ClickHouseServerException.FromServerResponse(error, query);
        activity?.SetException(ex);
        throw ex;
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
