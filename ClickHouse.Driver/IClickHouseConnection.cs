using System.Data;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver;

public interface IClickHouseConnection : IDbConnection
{
    new ClickHouseCommand CreateCommand(string commandText = null);
}
