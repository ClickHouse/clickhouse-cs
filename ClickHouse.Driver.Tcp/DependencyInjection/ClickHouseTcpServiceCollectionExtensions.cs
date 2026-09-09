using System;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers one singleton TCP data source and forwards its client interfaces to the same shared pool. The
/// service provider owns and disposes that pool; consumers must not dispose the injected client.
/// </summary>
[Experimental("CHTCP0001")]
public static class ClickHouseTcpServiceCollectionExtensions
{
    /// <summary>Registers a singleton TCP data source configured from a connection string.</summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="connectionString">A ClickHouse native-protocol connection string (keys such as <c>Host</c>, <c>Port</c>, <c>Username</c>, <c>set_&lt;name&gt;</c>).</param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey" /> of the registrations, or null for unkeyed ones.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services" /> or <paramref name="connectionString" /> is null.</exception>
    /// <exception cref="ArgumentException">A resulting option value is invalid.</exception>
    public static IServiceCollection AddClickHouseTcpDataSource(
        this IServiceCollection services,
        string connectionString,
        object serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        return AddClickHouseTcpDataSource(
            services,
            _ => ClickHouseTcpClientOptions.FromConnectionString(connectionString),
            serviceKey);
    }

    /// <summary>Registers a singleton TCP data source configured from options.</summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="options">The client configuration (endpoint, credentials, timeouts, client-level settings).</param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey" /> of the registrations, or null for unkeyed ones.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    /// <remarks>A null logger factory is filled from the container without modifying <paramref name="options"/>.</remarks>
    /// <exception cref="ArgumentNullException"><paramref name="services" /> or <paramref name="options" /> is null.</exception>
    public static IServiceCollection AddClickHouseTcpDataSource(
        this IServiceCollection services,
        ClickHouseTcpClientOptions options,
        object serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        return AddClickHouseTcpDataSource(services, _ => options, serviceKey);
    }

    /// <summary>Registers a singleton TCP data source configured by an options factory.</summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="optionsFactory">A factory that builds the client configuration.</param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey" /> of the registrations, or null for unkeyed ones.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    /// <remarks>The factory runs once; a null logger factory is filled from the container.</remarks>
    /// <exception cref="ArgumentNullException"><paramref name="services" /> or <paramref name="optionsFactory" /> is null.</exception>
    public static IServiceCollection AddClickHouseTcpDataSource(
        this IServiceCollection services,
        Func<IServiceProvider, ClickHouseTcpClientOptions> optionsFactory,
        object serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(optionsFactory);

        return AddClickHouseTcpDataSource(
            services,
            (sp, _) => new ClickHouseTcpDataSource(WithLoggerFactory(optionsFactory(sp), sp)),
            serviceKey);
    }

    /// <summary>Registers a singleton TCP data source built by a factory.</summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="dataSourceFactory">A factory for the <see cref="ClickHouseTcpDataSource" />, taking the service provider and the service key.</param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey" /> of the registrations, or null for unkeyed ones.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    /// <remarks>The factory owns all configuration. Existing registrations for the same service and key win.</remarks>
    /// <exception cref="ArgumentNullException"><paramref name="services" /> or <paramref name="dataSourceFactory" /> is null.</exception>
    public static IServiceCollection AddClickHouseTcpDataSource(
        this IServiceCollection services,
        Func<IServiceProvider, object, ClickHouseTcpDataSource> dataSourceFactory,
        object serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSourceFactory);

        services.TryAdd(new ServiceDescriptor(typeof(ClickHouseTcpDataSource), serviceKey, dataSourceFactory, ServiceLifetime.Singleton));

        // Forwarded to the concrete singleton, so injecting either the class or the interface gets the one
        // instance that owns the pool, and the provider disposes it once.
        services.TryAdd(new ServiceDescriptor(typeof(IClickHouseTcpDataSource), serviceKey, static (sp, key) => GetService<ClickHouseTcpDataSource>(sp, key), ServiceLifetime.Singleton));
        services.TryAdd(new ServiceDescriptor(typeof(IClickHouseTcpClient), serviceKey, static (sp, key) => GetService<ClickHouseTcpDataSource>(sp, key).GetClient(), ServiceLifetime.Singleton));
        services.TryAdd(new ServiceDescriptor(typeof(IClickHouseTcpOperations), serviceKey, static (sp, key) => GetService<IClickHouseTcpClient>(sp, key), ServiceLifetime.Singleton));
        return services;

        static T GetService<T>(IServiceProvider serviceProvider, object serviceKey)
            => serviceKey == null ? serviceProvider.GetRequiredService<T>() : serviceProvider.GetRequiredKeyedService<T>(serviceKey);
    }

    private static ClickHouseTcpClientOptions WithLoggerFactory(ClickHouseTcpClientOptions options, IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(options);

        return options.LoggerFactory == null
            ? options with { LoggerFactory = serviceProvider.GetService<ILoggerFactory>() }
            : options;
    }
}
