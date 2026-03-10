#if NET7_0_OR_GREATER
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver;

public interface IClickHouseDataSource
{
    string ConnectionString { get; }

    IClickHouseConnection CreateConnection();

    IClickHouseConnection OpenConnection();

    Task<IClickHouseConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);

    IClickHouseClient GetClient();
}
#endif
