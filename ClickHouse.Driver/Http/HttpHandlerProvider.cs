using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Http;
using System.Net.Security;

namespace ClickHouse.Driver.Http;

[SuppressMessage("Security", "CA5359:Do Not Disable Certificate Validation",  Justification = "We deliberately want to skip certificate validation in some cases.")]
internal static class HttpHandlerProvider
{
    /// <summary>
    /// Builds the handler the driver uses when the caller has not supplied an <see cref="HttpClient"/>.
    /// </summary>
    /// <remarks>
    /// <c>AutomaticDecompression</c> is deliberately <see cref="DecompressionMethods.None"/>. The framework
    /// does not merely decode matching responses: at send time it also <i>adds</i> every algorithm in the
    /// mask that is missing from the request's <c>Accept-Encoding</c>. With a mask of
    /// <c>GZip | Deflate</c> an explicit <c>identity</c> went out as <c>identity, gzip, deflate</c> and
    /// <c>deflate</c> as <c>deflate, gzip</c>; ClickHouse resolved those by its own fixed codec preference
    /// and answered <c>gzip</c>, which the handler then decoded and stripped — so the driver could neither
    /// honour an exact codec choice nor even observe that the negotiation had been overridden. Leaving the
    /// mask off makes the advertised set exactly what was asked for, and the driver decodes the answer
    /// itself via <see cref="ResponseDecompression"/> — which it has to do regardless, for the codecs the
    /// framework cannot decode at all.
    /// </remarks>
    public static HttpMessageHandler CreateHandler(bool skipServerCertificateValidation, int? maxConnectionsPerServer = null)
    {
#if NETCOREAPP2_1_OR_GREATER
        var socketsHandler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
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
