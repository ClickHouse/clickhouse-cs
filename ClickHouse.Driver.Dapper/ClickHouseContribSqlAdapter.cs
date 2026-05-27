using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Dapper.Contrib.Extensions;

namespace ClickHouse.Driver.Dapper;

/// <summary>
/// Dapper.Contrib <see cref="ISqlAdapter"/> that emits ClickHouse-flavoured SQL.
/// </summary>
/// <remarks>
/// <para>
/// ClickHouse has no auto-increment / identity columns, so <see cref="Insert"/> and
/// <see cref="InsertAsync"/> always return <c>0</c>. Callers must supply primary key values
/// themselves.
/// </para>
/// <para>
/// Dapper.Contrib's <c>Update&lt;T&gt;</c> and <c>Delete&lt;T&gt;</c> generate fixed
/// <c>update … set … where …</c> and <c>delete from … where …</c> statements. ClickHouse's standard
/// mutation path is <c>ALTER TABLE … UPDATE/DELETE</c>; the in-place SQL is not supported on the
/// stable server feature set. Prefer issuing those mutations directly via
/// <c>connection.ExecuteAsync</c> with <c>ALTER TABLE</c> syntax.
/// </para>
/// </remarks>
public sealed class ClickHouseContribSqlAdapter : ISqlAdapter
{
    public int Insert(
        IDbConnection connection,
        IDbTransaction transaction,
        int? commandTimeout,
        string tableName,
        string columnList,
        string parameterList,
        IEnumerable<PropertyInfo> keyProperties,
        object entityToInsert)
    {
        var sql = BuildInsertSql(tableName, columnList, parameterList);
        connection.Execute(sql, entityToInsert, transaction, commandTimeout);
        return 0;
    }

    public async Task<int> InsertAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        int? commandTimeout,
        string tableName,
        string columnList,
        string parameterList,
        IEnumerable<PropertyInfo> keyProperties,
        object entityToInsert)
    {
        var sql = BuildInsertSql(tableName, columnList, parameterList);
        await connection.ExecuteAsync(sql, entityToInsert, transaction, commandTimeout).ConfigureAwait(false);
        return 0;
    }

    public void AppendColumnName(StringBuilder sb, string columnName)
    {
        sb.AppendFormat(CultureInfo.InvariantCulture, "`{0}`", columnName);
    }

    public void AppendColumnNameEqualsValue(StringBuilder sb, string columnName)
    {
        sb.AppendFormat(CultureInfo.InvariantCulture, "`{0}` = @{1}", columnName, columnName);
    }

    private static string BuildInsertSql(string tableName, string columnList, string parameterList)
    {
        return string.Format(CultureInfo.InvariantCulture, "INSERT INTO {0} ({1}) VALUES ({2})", tableName, columnList, parameterList);
    }
}
