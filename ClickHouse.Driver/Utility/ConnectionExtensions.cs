using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Adapters;

namespace ClickHouse.Driver.Utility;

/// <summary>
/// Convenience extension methods for executing queries directly on a <see cref="DbConnection"/>.
/// </summary>
public static class ConnectionExtensions
{
    /// <summary>
    /// Executes a non-query SQL statement asynchronously.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="sql">The SQL statement to execute.</param>
    /// <returns>The number of rows affected. Note that this includes rows affected in materialized views. The number is inaccurate in the case of async inserts. The number is not available for DELETE/TRUNCATE queries.</returns>
    public static Task<int> ExecuteStatementAsync(this DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Executes a query asynchronously and returns the first column of the first row.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="sql">The SQL query to execute.</param>
    /// <returns>The first column of the first row, or null if no results.</returns>
    public static Task<object> ExecuteScalarAsync(this DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalarAsync();
    }

    /// <summary>
    /// Executes a query asynchronously and returns a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="sql">The SQL query to execute.</param>
    /// <returns>A data reader for iterating over the results.</returns>
    public static Task<DbDataReader> ExecuteReaderAsync(this DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteReaderAsync();
    }

    /// <summary>
    /// Executes a query and returns the results as a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="sql">The SQL query to execute.</param>
    /// <returns>A DataTable containing all rows from the query.</returns>
    public static DataTable ExecuteDataTable(this DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        using var adapter = new ClickHouseDataAdapter();
        command.CommandText = sql;
        adapter.SelectCommand = command;
        var dataTable = new DataTable();
        adapter.Fill(dataTable);
        return dataTable;
    }
}
