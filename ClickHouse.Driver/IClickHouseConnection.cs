using System.Data;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver;

public interface IClickHouseConnection : IDbConnection
{
    new ClickHouseCommand CreateCommand(string commandText = null);

    Task<bool> PingAsync(CancellationToken cancellationToken = default);
}
