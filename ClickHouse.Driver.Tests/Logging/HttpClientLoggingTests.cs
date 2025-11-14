using System;
using System.Collections.Generic;
using System.Net.Http;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Logging;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Logging;

public class HttpClientLoggingTests
{
    [Test]
    public void ConnectionOpen_DefaultConnection_LogsHttpClientAndHandlerConfigAtTraceLevel()
    {
        var factory = new CapturingLoggerFactory();
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            // This will fail to connect but should log the config
            connection.Open();
        }
        catch
        {
            // Ignore connection errors - we're just testing logging
        }

        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Connection));
        var logger = factory.Loggers[ClickHouseLogCategories.Connection];

        // Should have logged HttpClient config
        var httpClientConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientConfigEventId);
        Assert.That(httpClientConfigLog, Is.Not.Null);
        Assert.That(httpClientConfigLog.LogLevel, Is.EqualTo(LogLevel.Trace));
        Assert.That(httpClientConfigLog.Message, Does.Contain("HttpClient config"));
        Assert.That(httpClientConfigLog.Message, Does.Contain("Factory:"));

        // Should have logged HttpMessageHandler config
        var handlerConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientHandlerConfigEventId);
        Assert.That(handlerConfigLog, Is.Not.Null);
        Assert.That(handlerConfigLog.LogLevel, Is.EqualTo(LogLevel.Trace));
        Assert.That(handlerConfigLog.Message, Does.Contain("HttpMessageHandler config"));
    }

    [Test]
    public void ConnectionOpen_DefaultConnection_LogsSocketsHttpHandlerType()
    {
        var factory = new CapturingLoggerFactory();
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            connection.Open();
        }
        catch
        {
            // Ignore connection errors
        }

        var logger = factory.Loggers[ClickHouseLogCategories.Connection];
        var handlerConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientHandlerConfigEventId);

        Assert.That(handlerConfigLog, Is.Not.Null);

        // .NET 5+ should log SocketsHttpHandler
        Assert.That(handlerConfigLog.Message, Does.Contain("Type: SocketsHttpHandler"));
    }

    [Test]
    public void ConnectionOpen_CustomHttpClientWithHandlerSettings_LogsCustomConfiguration()
    {
        var factory = new CapturingLoggerFactory();
        var handler = new HttpClientHandler
        {
            MaxConnectionsPerServer = 42,
            AllowAutoRedirect = false
        };
        using var httpClient = new HttpClient(handler, disposeHandler: true);

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            HttpClient =  httpClient,
            LoggerFactory = factory,
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            connection.Open();
        }
        catch
        {
            // Ignore connection errors
        }

        var logger = factory.Loggers[ClickHouseLogCategories.Connection];
        var handlerConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientHandlerConfigEventId);

        Assert.That(handlerConfigLog, Is.Not.Null);
        Assert.That(handlerConfigLog.Message, Does.Contain("MaxConnectionsPerServer: 42"));
    }

    [Test]
    public void ConnectionOpen_UseSessionEnabled_LogsHttpClientAndHandlerConfig()
    {
        var factory = new CapturingLoggerFactory();
        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
            UseSession = true
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            connection.Open();
        }
        catch
        {
            // Ignore connection errors
        }

        Assert.That(factory.Loggers, Does.ContainKey(ClickHouseLogCategories.Connection));
        var logger = factory.Loggers[ClickHouseLogCategories.Connection];

        // Should have logged HttpClient config even with UseSession=true
        var httpClientConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientConfigEventId);
        Assert.That(httpClientConfigLog, Is.Not.Null);
        Assert.That(httpClientConfigLog.LogLevel, Is.EqualTo(LogLevel.Trace));

        // Should have logged handler config
        var handlerConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientHandlerConfigEventId);
        Assert.That(handlerConfigLog, Is.Not.Null);
    }

    [Test]
    public void ConnectionOpen_CustomHttpClientFactory_LogsFactoryTypeName()
    {
        var factory = new CapturingLoggerFactory();
        var customFactory = new CustomHttpClientFactory();

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            HttpClientFactory = customFactory,
            LoggerFactory = factory,
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            connection.Open();
        }
        catch
        {
            // Ignore connection errors
        }

        var logger = factory.Loggers[ClickHouseLogCategories.Connection];
        var httpClientConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientConfigEventId);

        Assert.That(httpClientConfigLog, Is.Not.Null);
        Assert.That(httpClientConfigLog.Message, Does.Contain("Factory: CustomHttpClientFactory"));
    }

    [Test]
    public void ConnectionOpen_TraceLevelNotEnabled_DoesNotLogHttpClientConfig()
    {
        // Create a logger that doesn't log Trace level
        var factory = new CapturingLoggerFactory();
        factory.MinimumLevel = LogLevel.Debug;

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            LoggerFactory = factory,
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            connection.Open();
        }
        catch
        {
            // Ignore connection errors
        }

        // Should not have logged anything since Trace is not enabled
        var logger = factory.Loggers[ClickHouseLogCategories.Connection];
        Assert.That(logger.Logs.Count(l => l.EventId == LoggingHelpers.HttpClientConfigEventId), Is.EqualTo(0));
    }

    #if NET5_0_OR_GREATER
    [Test]
    public void ConnectionOpen_SocketsHttpHandler_LogsSettings()
    {
        var factory = new CapturingLoggerFactory();

        // Create custom handler with specific settings we can verify
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = 50,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            PooledConnectionIdleTimeout = TimeSpan.FromSeconds(90),
            ConnectTimeout = TimeSpan.FromSeconds(15),
            Expect100ContinueTimeout = TimeSpan.FromSeconds(1),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
            SslOptions = new SslClientAuthenticationOptions
            {
                EnabledSslProtocols = SslProtocols.Tls12 |  SslProtocols.Tls13,
            }
        };
        using var httpClient = new HttpClient(handler, disposeHandler: true);

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            HttpClient = httpClient,
            LoggerFactory = factory,
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            connection.Open();
        }
        catch
        {
            // Ignore connection errors
        }

        var logger = factory.Loggers[ClickHouseLogCategories.Connection];
        var handlerConfigLog = logger.Logs.Find(l => l.EventId == LoggingHelpers.HttpClientHandlerConfigEventId);

        Assert.That(handlerConfigLog, Is.Not.Null);

        // Check that connection pool settings are logged with the values we set
        Assert.That(handlerConfigLog.Message, Does.Contain("MaxConnectionsPerServer: 50"));
        Assert.That(handlerConfigLog.Message, Does.Contain("PooledConnectionLifetime: 00:05:00"));
        Assert.That(handlerConfigLog.Message, Does.Contain("PooledConnectionIdleTimeout: 00:01:30"));

        // Check that timeout settings are logged with the values we set
        Assert.That(handlerConfigLog.Message, Does.Contain("ConnectTimeout: 00:00:15"));
        Assert.That(handlerConfigLog.Message, Does.Contain("Expect100ContinueTimeout: 00:00:01"));
        Assert.That(handlerConfigLog.Message, Does.Contain("KeepAlivePingTimeout: 00:00:30"));

        // Check that SSL settings are logged
        Assert.That(handlerConfigLog.Message, Does.Contain("SslProtocols: Tls12, Tls13"));
        Assert.That(handlerConfigLog.Message, Does.Contain("RemoteCertificateValidationCallback: Null"));
    }
#endif

    [Test]
    public void ConnectionOpen_UnknownHandlerType_LogsUnknownHandlerTypeAtDebugLevel()
    {
        var factory = new CapturingLoggerFactory();

        // Create a custom handler that is neither HttpClientHandler nor SocketsHttpHandler
        var customHandler = new CustomMessageHandler();
        using var httpClient = new HttpClient(customHandler, disposeHandler: true);

        var settings = new ClickHouseClientSettings(TestUtilities.GetConnectionStringBuilder())
        {
            HttpClient = httpClient,
            LoggerFactory = factory,
        };
        var connection = new ClickHouseConnection(settings);

        try
        {
            connection.Open();
        }
        catch
        {
            // Ignore connection errors
        }

        var logger = factory.Loggers[ClickHouseLogCategories.Connection];
        var unknownHandlerLog = logger.Logs.Find(l =>
            l.LogLevel == LogLevel.Debug &&
            l.Message.Contains("Unknown handler type") &&
            l.Message.Contains("CustomMessageHandler"));

        Assert.That(unknownHandlerLog, Is.Not.Null, "Should log unknown handler type at Debug level");
    }

    private class CustomMessageHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        }
    }

    private class CustomHttpClientFactory : IHttpClientFactory
    {
        public HttpClient CreateClient(string name = null)
        {
            return new HttpClient();
        }
    }
}
