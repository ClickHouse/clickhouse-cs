using System.Net.Http;
using ClickHouse.Driver.Http;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Http;

public class HttpClientExtensionsTests
{
    [Test]
    public void GetHandler_ReturnsHandlerForDefaultHttpClient()
    {
        using var httpClient = new HttpClient();

        var handler = httpClient.GetHandler();

        Assert.That(handler, Is.Not.Null);
        // Default HttpClient always creates SocketsHttpHandler on .NET 5+
#if NET5_0_OR_GREATER
        Assert.That(handler, Is.InstanceOf<SocketsHttpHandler>());
#else
        Assert.That(handler, Is.InstanceOf<HttpClientHandler>());
#endif
    }

#if NET5_0_OR_GREATER
    [Test]
    public void GetHandler_ReturnsProvidedSocketsHttpHandler()
    {
        var handler = new SocketsHttpHandler();
        using var httpClient = new HttpClient(handler, disposeHandler: false);

        var retrievedHandler = httpClient.GetHandler();

        Assert.That(retrievedHandler, Is.SameAs(handler));
        Assert.That(retrievedHandler, Is.InstanceOf<SocketsHttpHandler>());
    }


    [Test]
    public void GetHandler_RetrievesHttpClientHandlerConfiguration()
    {
        var handler = new HttpClientHandler
        {
            MaxConnectionsPerServer = 42,
            AllowAutoRedirect = false,
            UseCookies = false
        };
        using var httpClient = new HttpClient(handler, disposeHandler: false);

        var retrievedHandler = httpClient.GetHandler() as SocketsHttpHandler;

        Assert.That(retrievedHandler, Is.Not.Null);
        Assert.That(retrievedHandler.MaxConnectionsPerServer, Is.EqualTo(42));
        Assert.That(retrievedHandler.AllowAutoRedirect, Is.False);
        Assert.That(retrievedHandler.UseCookies, Is.False);
    }


    [Test]
    public void GetHandler_RetrievesSocketsHttpHandlerConfiguration()
    {
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = 100,
            AllowAutoRedirect = true,
            UseCookies = false
        };
        using var httpClient = new HttpClient(handler, disposeHandler: false);

        var retrievedHandler = httpClient.GetHandler() as SocketsHttpHandler;

        Assert.That(retrievedHandler, Is.Not.Null);
        Assert.That(retrievedHandler.MaxConnectionsPerServer, Is.EqualTo(100));
        Assert.That(retrievedHandler.AllowAutoRedirect, Is.True);
        Assert.That(retrievedHandler.UseCookies, Is.False);
    }
#endif
}
