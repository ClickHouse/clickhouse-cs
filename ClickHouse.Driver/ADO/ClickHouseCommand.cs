using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Represents a SQL command to execute against a ClickHouse database.
/// </summary>
public class ClickHouseCommand : DbCommand, IClickHouseCommand, IDisposable
{
    private readonly CancellationTokenSource cts = new CancellationTokenSource();
    private readonly ClickHouseParameterCollection commandParameters = new ClickHouseParameterCollection();
    private Dictionary<string, object> customSettings;
    private List<string> roles;
    private ClickHouseConnection connection;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseCommand"/> class.
    /// </summary>
    public ClickHouseCommand()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseCommand"/> class, with the specified connection.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    public ClickHouseCommand(ClickHouseConnection connection)
    {
        this.connection = connection;
    }

    /// <summary>
    /// Gets or sets the SQL query to execute.
    /// </summary>
    public override string CommandText { get; set; }

    /// <summary>
    /// Gets or sets the command timeout in seconds. Not currently used by ClickHouse.
    /// </summary>
    public override int CommandTimeout { get; set; }

    /// <inheritdoc/>
    public override CommandType CommandType { get; set; }

    /// <inheritdoc/>
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc/>
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    /// Gets or sets QueryId associated with command.
    /// If not set before execution, a GUID will be automatically generated.
    /// </summary>
    public string QueryId { get; set; }

    /// <summary>
    /// Gets statistics from the last executed query (rows read, bytes read, elapsed time, etc.).
    /// Populated after query execution from the X-ClickHouse-Summary header.
    /// </summary>
    public QueryStats QueryStats { get; private set; }

    /// <summary>
    /// Gets the server's timezone from the last executed query response.
    /// This is extracted from the X-ClickHouse-Timezone header.
    /// </summary>
    public string ServerTimezone { get; private set; }

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

    /// <inheritdoc/>
    public override void Cancel() => cts.Cancel();

    /// <inheritdoc/>
    public override int ExecuteNonQuery() => ExecuteNonQueryAsync(cts.Token).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        if (connection == null)
            throw new InvalidOperationException("Connection is not set");

        using var lcts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
        using var response = await PostSqlQueryAsync(CommandText, lcts.Token).ConfigureAwait(false);
        using var reader = new ExtendedBinaryReader(await response.Content.ReadAsStreamAsync(lcts.Token).ConfigureAwait(false));

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

    /// <inheritdoc/>
    public override object ExecuteScalar() => ExecuteScalarAsync(cts.Token).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        using var lcts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
        using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, lcts.Token).ConfigureAwait(false);
        return reader.Read() ? reader.GetValue(0) : null;
    }

    /// <summary>
    /// No-op. ClickHouse does not support prepared statements.
    /// </summary>
    public override void Prepare() { /* ClickHouse has no notion of prepared statements */ }

    /// <summary>
    /// Creates a new <see cref="ClickHouseDbParameter"/> for this command.
    /// </summary>
    /// <returns>A new parameter instance.</returns>
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
        return await ClickHouseDataReader.FromHttpResponseAsync(result, connection.ClickHouseClient.TypeSettings).ConfigureAwait(false);
    }

    private async Task<HttpResponseMessage> PostSqlQueryAsync(string sqlQuery, CancellationToken token)
    {
        var options = BuildQueryOptions();
        QueryResult result = await connection.ClickHouseClient.PostSqlQueryAsync(sqlQuery, commandParameters, options, token).ConfigureAwait(false);
        QueryId = result.QueryId;
        QueryStats = result.QueryStats;
        ServerTimezone = result.ServerTimezone;
        return result.HttpResponseMessage;
    }

    private QueryOptions BuildQueryOptions()
    {
        return new QueryOptions
        {
            QueryId = QueryId,
            BearerToken = BearerToken,
            Database = connection?.Database,
            Roles = roles?.Count > 0 ? roles : null,
            CustomSettings = customSettings?.Count > 0 ? customSettings : null,
        };
    }
}
