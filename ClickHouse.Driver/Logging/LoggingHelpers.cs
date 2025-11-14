using System;
using System.Net.Http;
using ClickHouse.Driver.Http;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Logging;

internal static class LoggingHelpers
{
    internal static readonly EventId HttpClientConfigEventId = new(1, "HttpClientConfig");
    internal static readonly EventId HttpClientHandlerConfigEventId = new(2, "HttpClientHandlerConfig");

    private static readonly Action<ILogger, string, Exception> LogHttpClientConfig =
        LoggerMessage.Define<string>(
            LogLevel.Trace,
            HttpClientConfigEventId,
            "HttpClient config\n  Factory: {FactoryType}");

    private static readonly Action<ILogger, string, string, string, string, string, Exception> LogHttpClientHandlerConfig =
        LoggerMessage.Define<string, string, string, string, string>(
            LogLevel.Trace,
            HttpClientHandlerConfigEventId,
            "HttpMessageHandler config\n" +
            "  Type: {HandlerType}\n" +
            "  Connection Pool: {ConnectionPoolSettings}\n" +
            "  Timeouts: {TimeoutSettings}\n" +
            "  AutomaticDecompression: {AutomaticDecompression}\n" +
            "  SslOptions: {SslSettings}");

    internal static void HttpClientFactoryConfigured(
        this ILogger logger,
        string factoryType) =>
        LogHttpClientConfig(logger, factoryType, null);

    internal static void HttpClientHandlerConfigured(
        this ILogger logger,
        string handlerType,
        string connectionPoolSettings,
        string timeoutSettings,
        string automaticDecompression,
        string sslSettings) =>
        LogHttpClientHandlerConfig(logger, handlerType, connectionPoolSettings, timeoutSettings, automaticDecompression, sslSettings, null);

    internal static void LogHttpClientConfiguration(ILogger logger, IHttpClientFactory clientFactory)
    {
        if (logger == null || !logger.IsEnabled(LogLevel.Trace))
        {
            return;
        }

        try
        {
            var client = clientFactory.CreateClient();
            var factoryType = clientFactory?.GetType().Name ?? "null";

            logger.HttpClientFactoryConfigured(factoryType);

            var handler = client.GetHandler();

            // Log based on the actual handler type returned
            if (handler is HttpClientHandler httpClientHandler)
            {
                var connectionPoolSettings = $"MaxConnectionsPerServer: {httpClientHandler.MaxConnectionsPerServer}";

                var sslSettings = httpClientHandler.ServerCertificateCustomValidationCallback != null
                    ? "RemoteCertificateValidationCallback: Set"
                    : "RemoteCertificateValidationCallback: Null";

                logger.HttpClientHandlerConfigured(
                    "HttpClientHandler",
                    connectionPoolSettings,
                    client.Timeout.ToString(), // HttpClientHandler doesn't expose timeout settings beyond HttpClient.Timeout
                    httpClientHandler.AutomaticDecompression.ToString(),
                    sslSettings);
            }
#if NET5_0_OR_GREATER
            else if (handler is SocketsHttpHandler socketsHandler)
            {
                var connectionPoolSettings = $"MaxConnectionsPerServer: {socketsHandler.MaxConnectionsPerServer}, " +
                    $"PooledConnectionLifetime: {socketsHandler.PooledConnectionLifetime}, " +
                    $"PooledConnectionIdleTimeout: {socketsHandler.PooledConnectionIdleTimeout}";

                var timeoutSettings = $"ConnectTimeout: {socketsHandler.ConnectTimeout}, " +
                    $"Expect100ContinueTimeout: {socketsHandler.Expect100ContinueTimeout}, " +
                    $"KeepAlivePingTimeout: {socketsHandler.KeepAlivePingTimeout}";

                var sslProtocols = socketsHandler.SslOptions?.EnabledSslProtocols.ToString() ?? "Default";
                var certValidation = socketsHandler.SslOptions?.RemoteCertificateValidationCallback != null ? "Set" : "Null";
                var sslSettings = $"SslProtocols: {sslProtocols}, RemoteCertificateValidationCallback: {certValidation}";

                logger.HttpClientHandlerConfigured(
                    "SocketsHttpHandler",
                    connectionPoolSettings,
                    timeoutSettings,
                    socketsHandler.AutomaticDecompression.ToString(),
                    sslSettings);
            }
#endif
            else
            {
                logger.LogDebug("Unknown handler type {HandlerType}", handler.GetType().Name);
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Failed to get http client handler.");
        }
    }
}
