using System.Data;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver;

public interface IClickHouseConnection : IDbConnection
{
#pragma warning disable CS0109 // Member does not hide an inherited member; new keyword is not required
    new ClickHouseCommand CreateCommand(string commandText = null);
#pragma warning restore CS0109 // Member does not hide an inherited member; new keyword is not required

    Task<bool> PingAsync(CancellationToken cancellationToken = default);
}
