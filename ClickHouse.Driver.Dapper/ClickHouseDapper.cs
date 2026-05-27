using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading;
using ClickHouse.Driver.Dapper.TypeHandlers;
using Dapper;
using Dapper.Contrib.Extensions;

namespace ClickHouse.Driver.Dapper;

/// <summary>
/// Entry point for ClickHouse + Dapper integration. Call <see cref="Register"/> once at startup
/// to install type handlers and the Dapper.Contrib SQL adapter.
/// </summary>
public static class ClickHouseDapper
{
    private static int registered;

    /// <summary>
    /// Registers ClickHouse-specific Dapper type handlers and the Dapper.Contrib SQL adapter.
    /// Idempotent and thread-safe — calling it more than once is a no-op.
    /// </summary>
    public static void Register()
    {
        if (Interlocked.Exchange(ref registered, 1) == 1)
            return;

        SqlMapper.AddTypeHandler(new ClickHouseDecimalHandler());
        SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
        SqlMapper.AddTypeHandler(new ITupleHandler());
        SqlMapper.AddTypeHandler(new IPAddressHandler());
        SqlMapper.AddTypeHandler(new BigIntegerHandler());

        // DateTime maps to DateTime2 so Dapper sends DateTime literals without the SQL Server "datetime"
        // sub-millisecond truncation. ClickHouse's DateTime / DateTime64 accept the resulting ISO format.
        SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime2);

        RegisterContribAdapter();
    }

    private static void RegisterContribAdapter()
    {
        // Dapper.Contrib's AdapterDictionary is private static readonly. There is no public
        // extensibility point, so we reach in via reflection and insert under the connection's
        // lowercase type name (the same key Contrib's internal GetFormatter uses).
        // If the field is missing or has an unexpected shape, fail loudly — silently no-op'ing
        // here would leave Contrib using SqlServerAdapter against ClickHouse with no signal.
        const string AdapterFieldName = "AdapterDictionary";
        const string ConnectionKey = "clickhouseconnection";

        var field = typeof(SqlMapperExtensions).GetField(
            AdapterFieldName,
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                $"Could not locate {nameof(SqlMapperExtensions)}.{AdapterFieldName} via reflection. " +
                "The installed Dapper.Contrib version may be incompatible with ClickHouse.Driver.Dapper.");

        if (field.GetValue(null) is not IDictionary<string, ISqlAdapter> adapters)
        {
            throw new InvalidOperationException(
                $"{nameof(SqlMapperExtensions)}.{AdapterFieldName} has an unexpected type. " +
                "The installed Dapper.Contrib version may be incompatible with ClickHouse.Driver.Dapper.");
        }

        adapters[ConnectionKey] = new ClickHouseContribSqlAdapter();
    }
}
