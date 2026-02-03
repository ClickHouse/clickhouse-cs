using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Json;

namespace ClickHouse.Driver;

/// <summary>
/// Defines the contract for a ClickHouse client.
/// </summary>
public interface IClickHouseClient : IDisposable
{
    /// <summary>
    /// Gets the settings used by this client.
    /// </summary>
    ClickHouseClientSettings Settings { get; }

    /// <summary>
    /// Executes a SQL statement and returns the number of rows affected.
    /// </summary>
    /// <param name="sql">The SQL statement to execute.</param>
    /// <param name="parameters">Optional parameters for the query.</param>
    /// <param name="options">Optional query options to override client defaults.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows affected.</returns>
    Task<int> ExecuteNonQueryAsync(
        string sql,
        IEnumerable<ClickHouseDbParameter> parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a SQL query and returns the first column of the first row.
    /// </summary>
    /// <typeparam name="T">The expected return type.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="parameters">Optional parameters for the query.</param>
    /// <param name="options">Optional query options to override client defaults.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The first column of the first row, or default if no results.</returns>
    Task<T> ExecuteScalarAsync<T>(
        string sql,
        IEnumerable<ClickHouseDbParameter> parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a SQL query and returns a data reader for iterating results.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="parameters">Optional parameters for the query.</param>
    /// <param name="options">Optional query options to override client defaults.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A data reader for the query results.</returns>
    Task<ClickHouseDataReader> ExecuteReaderAsync(
        string sql,
        IEnumerable<ClickHouseDbParameter> parameters = null,
        QueryOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a SQL query and returns a raw result for custom format handling.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="options">Optional query options to override client defaults.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A raw result containing the response stream.</returns>
    Task<ClickHouseRawResult> ExecuteRawResultAsync(
        string sql,
        QueryOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts rows into a table using the binary protocol.
    /// </summary>
    /// <param name="table">The destination table name.</param>
    /// <param name="columns">The column names to insert into.</param>
    /// <param name="rows">The rows to insert, where each row is an array of column values.</param>
    /// <param name="options">Optional insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows inserted.</returns>
    Task<long> InsertBinaryAsync(
        string table,
        IEnumerable<string> columns,
        IEnumerable<object[]> rows,
        InsertOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts data from a stream into a table using the specified format.
    /// </summary>
    /// <param name="table">The destination table name.</param>
    /// <param name="stream">The stream containing the data to insert.</param>
    /// <param name="format">The ClickHouse format of the data (e.g., "CSV", "JSONEachRow", "Parquet").</param>
    /// <param name="columns">Optional column names. If null, all columns are assumed in table order.</param>
    /// <param name="options">Optional insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task InsertRawStreamAsync(
        string table,
        Stream stream,
        string format,
        IEnumerable<string> columns = null,
        InsertOptions options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Post a raw stream to the server.
    /// </summary>
    /// <param name="sql">SQL query to add to URL, may be empty</param>
    /// <param name="data">Raw stream to be sent. May contain SQL query at the beginning. May be gzip-compressed</param>
    /// <param name="isCompressed">Indicates whether "Content-Encoding: gzip" header should be added</param>
    /// <param name="token">Cancellation token</param>
    /// <param name="queryOptions">Query options that override connection-level options</param>
    /// <returns>Task-wrapped HttpResponseMessage object</returns>
    Task<HttpResponseMessage> PostStreamAsync(string sql, Stream data, bool isCompressed, CancellationToken token, QueryOptions queryOptions = null);

    /// <summary>
    /// Post a raw stream to the server using a stream-generating callback.
    /// </summary>
    /// <param name="sql">SQL query to add to URL, may be empty</param>
    /// <param name="callback">Callback invoked to write to the stream. May contain SQL query at the beginning. May be gzip-compressed</param>
    /// <param name="isCompressed">Iindicates whether "Content-Encoding: gzip" header should be added</param>
    /// <param name="token">Cancellation token</param>
    /// <param name="queryOptions">Query options that override connection-level options</param>
    /// <returns>Task-wrapped HttpResponseMessage object</returns>
    Task<HttpResponseMessage> PostStreamAsync(string sql, Func<Stream, CancellationToken, Task> callback, bool isCompressed, CancellationToken token, QueryOptions queryOptions = null);

    /// <summary>
    /// Pings the ClickHouse server to check if it is available.
    /// </summary>
    /// <param name="queryOptions">Query options that override connection-level options</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the server responds successfully, false otherwise.</returns>
    Task<bool> PingAsync(QueryOptions queryOptions = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers a POCO type for JSON column serialization.
    /// Types must be registered before they can be used in operations with JSON or Dynamic columns.
    /// </summary>
    /// <typeparam name="T">The POCO type to register.</typeparam>
    /// <exception cref="ClickHouseJsonSerializationException">
    /// Thrown if any property type cannot be mapped to a ClickHouse type.
    /// </exception>
    void RegisterJsonSerializationType<T>() where T : class;

    /// <summary>
    /// Registers a POCO type for JSON column serialization.
    /// Types must be registered before they can be used in operations with JSON or Dynamic columns.
    /// </summary>
    /// <param name="type">The POCO type to register.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="type"/> is null.</exception>
    /// <exception cref="ClickHouseJsonSerializationException">
    /// Thrown if any property type cannot be mapped to a ClickHouse type.
    /// </exception>
    void RegisterJsonSerializationType(Type type);

    /// <summary>
    /// Creates a new <see cref="ClickHouseConnection"/> that uses this client's HTTP connection pool.
    /// </summary>
    /// <returns>A new connection instance.</returns>
    ClickHouseConnection CreateConnection();
}
