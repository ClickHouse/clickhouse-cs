#if NET7_0_OR_GREATER
using System;
using System.Data.Common;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.ADO;

public sealed class ClickHouseDataSource : DbDataSource, IClickHouseDataSource
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly string httpClientName;
    private readonly HttpClient httpClient;
    private readonly bool disposeHttpClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseDataSource"/> class using provided HttpClient.
    /// Note that HttpClient must have AutomaticDecompression enabled if compression is not disabled in connection string
    /// </summary>
    /// <param name="connectionString">Connection string</param>
    /// <param name="httpClient">instance of HttpClient</param>
    /// <param name="disposeHttpClient">dispose of the passed-in instance of HttpClient</param>
    public ClickHouseDataSource(string connectionString, HttpClient httpClient = null, bool disposeHttpClient = true)
    {
        ConnectionString = connectionString;
        this.httpClient = httpClient;
        this.disposeHttpClient = disposeHttpClient;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseDataSource"/> class using an HttpClient generated by the provided <paramref name="httpClientFactory"/>.
    /// </summary>
    /// <param name="connectionString">The ClickHouse connection string.</param>
    /// <param name="httpClientFactory">The factory to be used for creating the clients.</param>
    /// <param name="httpClientName">
    /// The name of the HTTP client you want to be created using the provided factory.
    /// If left empty, the default client will be created.
    /// </param>
    /// <remarks>
    /// <list type="bullet">
    /// <item>
    /// If compression is not disabled in the <paramref name="connectionString"/>, the <paramref name="httpClientFactory"/>
    /// must be configured to enable <see cref="HttpClientHandler.AutomaticDecompression"/> for its generated clients.
    /// <example>
    /// For example, you can do this while registering the HTTP client:
    /// <code>
    /// services.AddHttpClient("ClickHouseClient").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
    /// {
    ///     AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    /// });
    /// </code>
    /// </example>
    /// </item>
    /// <item>
    /// The <paramref name="httpClientFactory"/> must set the timeout for its clients if needed.
    /// <example>
    /// For example, you can do this while registering the HTTP client:
    /// <code>
    /// services.AddHttpClient("ClickHouseClient", c => c.Timeout = TimeSpan.FromMinutes(5));
    /// </code>
    /// </example>
    /// </item>
    /// </list>
    /// </remarks>
    public ClickHouseDataSource(string connectionString, IHttpClientFactory httpClientFactory, string httpClientName = "")
    {
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(httpClientName);
        ConnectionString = connectionString;
        this.httpClientFactory = httpClientFactory;
        this.httpClientName = httpClientName;
    }

    public override string ConnectionString
    {
        get;
    }

    public ILogger Logger
    {
        get;
        set;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing && disposeHttpClient)
        {
            httpClient?.Dispose();
        }
    }

    protected override DbConnection CreateDbConnection()
    {
        var cn = httpClientFactory != null
            ? new ClickHouseConnection(ConnectionString, httpClientFactory, httpClientName)
            : new ClickHouseConnection(ConnectionString, httpClient);
        if (cn.Logger == null && Logger != null)
        {
            cn.Logger = Logger;
        }

        return cn;
    }

    public new ClickHouseConnection CreateConnection() => (ClickHouseConnection)CreateDbConnection();

    IClickHouseConnection IClickHouseDataSource.CreateConnection() => CreateConnection();

    public new ClickHouseConnection OpenConnection() => (ClickHouseConnection)OpenDbConnection();

    IClickHouseConnection IClickHouseDataSource.OpenConnection() => OpenConnection();

    public new async Task<ClickHouseConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var cn = await OpenDbConnectionAsync(cancellationToken).ConfigureAwait(false);
        return (ClickHouseConnection)cn;
    }

    async Task<IClickHouseConnection> IClickHouseDataSource.OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var cn = await OpenDbConnectionAsync(cancellationToken).ConfigureAwait(false);
        return (IClickHouseConnection)cn;
    }
}
#endif
