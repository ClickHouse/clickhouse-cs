using System;
using Microsoft.Extensions.DependencyInjection;

namespace ClickHouse.Driver;

public static class ClickHouseServiceCollectionExtensions
{
    public static IServiceCollection AddClickHouseClient(
        this IServiceCollection services,
        string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        services.AddSingleton<IClickHouseClient>(sp =>
            new ClickHouseClient(connectionString));

        return services;
    }
}
