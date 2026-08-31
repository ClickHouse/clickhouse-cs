using System;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for setting up the ClickHouse native-protocol (TCP) client in an
/// <see cref="IServiceCollection" />.
/// </summary>
/// <remarks>
/// <para>
/// Every overload registers one <see cref="ClickHouseTcpDataSource" /> as a singleton and resolves
/// <see cref="IClickHouseTcpDataSource" />, <see cref="IClickHouseTcpClient" /> and
/// <see cref="IClickHouseTcpOperations" /> from it, so the whole application shares one connection pool.
/// Singleton is the only lifetime offered: the data source owns a pool that has to outlive every consumer, and
/// the client is that pool rather than a per-consumer resource.
/// </para>
/// <para>
/// <b>Do not dispose the injected client.</b> Disposing it closes the shared pool, so every other consumer's
/// operations fail from then on. Disposing the provider closes the pool once, at shutdown, which is what you
/// want; either disposal path works, though prefer <c>await provider.DisposeAsync()</c> where the call site can
/// await it, as a generic host does.
/// </para>
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </remarks>
[Experimental("CHTCP0001")]
public static class ClickHouseTcpServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="ClickHouseTcpDataSource" /> and the <see cref="IClickHouseTcpClient" /> it owns in
    /// the <see cref="IServiceCollection" />, configured from a connection string.
    /// </summary>
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

    /// <summary>
    /// Registers a <see cref="ClickHouseTcpDataSource" /> and the <see cref="IClickHouseTcpClient" /> it owns in
    /// the <see cref="IServiceCollection" />, configured from options.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="options">The client configuration (endpoint, credentials, timeouts, client-level settings).</param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey" /> of the registrations, or null for unkeyed ones.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    /// <remarks>
    /// A null <see cref="ClickHouseTcpClientOptions.LoggerFactory" /> is filled in from the container's
    /// <see cref="ILoggerFactory" /> when it registers one; the options given here are left unchanged.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="services" /> or <paramref name="options" /> is null.</exception>
    public static IServiceCollection AddClickHouseTcpDataSource(
        this IServiceCollection services,
        ClickHouseTcpClientOptions options,
        object serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        return AddClickHouseTcpDataSource(services, _ => options, serviceKey);
    }

    /// <summary>
    /// Registers a <see cref="ClickHouseTcpDataSource" /> and the <see cref="IClickHouseTcpClient" /> it owns in
    /// the <see cref="IServiceCollection" />, configured by an options factory with access to the service provider.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="optionsFactory">A factory that builds the client configuration.</param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey" /> of the registrations, or null for unkeyed ones.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    /// <remarks>
    /// The factory runs once, when the data source is first resolved. A null
    /// <see cref="ClickHouseTcpClientOptions.LoggerFactory" /> on its result is filled in from the container's
    /// <see cref="ILoggerFactory" /> when it registers one.
    /// </remarks>
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

    /// <summary>
    /// Registers a <see cref="ClickHouseTcpDataSource" /> built by a factory, and the
    /// <see cref="IClickHouseTcpClient" /> it owns, in the <see cref="IServiceCollection" />.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="dataSourceFactory">A factory for the <see cref="ClickHouseTcpDataSource" />, taking the service provider and the service key.</param>
    /// <param name="serviceKey">The <see cref="ServiceDescriptor.ServiceKey" /> of the registrations, or null for unkeyed ones.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    /// <remarks>
    /// The factory owns the whole configuration, including <see cref="ClickHouseTcpClientOptions.LoggerFactory" />;
    /// nothing is filled in from the container. Each service is added with
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAdd(IServiceCollection, ServiceDescriptor)" />, so an
    /// earlier registration of the same service (and key) wins.
    /// </remarks>
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
