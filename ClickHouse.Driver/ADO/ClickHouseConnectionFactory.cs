﻿using System.Data.Common;
using ClickHouse.Driver.ADO.Adapters;
using ClickHouse.Driver.ADO.Parameters;

namespace ClickHouse.Driver.ADO;

public class ClickHouseConnectionFactory : DbProviderFactory
{
    public static ClickHouseConnectionFactory Instance => new();

    public override DbConnection CreateConnection() => new ClickHouseConnection();

    public override DbDataAdapter CreateDataAdapter() => new ClickHouseDataAdapter();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new ClickHouseConnectionStringBuilder();

    public override DbParameter CreateParameter() => new ClickHouseDbParameter();

    public override DbCommand CreateCommand() => new ClickHouseCommand();

#if NET7_0_OR_GREATER
    public override DbDataSource CreateDataSource(string connectionString) => new ClickHouseDataSource(connectionString);
#endif
}
