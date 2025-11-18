using System;
using System.Net;
using System.Net.Http;
using System.Net.Security;

namespace ClickHouse.Driver.Http;

internal static class HttpHandlerProvider
{
    public static HttpMessageHandler CreateHandler(bool skipServerCertificateValidation, int? maxConnectionsPerServer = null)
    {
#if NETCOREAPP2_1_OR_GREATER
        var socketsHandler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5),
        };

        if (maxConnectionsPerServer.HasValue)
        {
            socketsHandler.MaxConnectionsPerServer = maxConnectionsPerServer.Value;
        }

        if (skipServerCertificateValidation)
        {
            socketsHandler.SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, _, _, _) => true,
            };
        }
        return socketsHandler;
#else
        var defaultHandler = new DefaultHttpClientHandler(skipServerCertificateValidation);
        if (maxConnectionsPerServer.HasValue)
        {
            defaultHandler.MaxConnectionsPerServer = maxConnectionsPerServer.Value;
        }
        return defaultHandler;
#endif
    }
}
