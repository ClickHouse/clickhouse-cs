using System;
using System.Diagnostics;
using System.Net.Http;
using System.Reflection;

#if NET8_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace ClickHouse.Driver.Http;

internal static class HttpClientExtensions
{
    internal static HttpMessageHandler GetHandler(this HttpClient source)
    {
        var handler = GetHandlerField(source);
        if (handler == null)
        {
            return null;
        }

#if NET5_0_OR_GREATER
        // On .NET 5+, if handler is SocketsHttpHandler, check if it has an underlying handler
        // (This happens when you pass HttpClientHandler to HttpClient - it wraps it)
        if (handler is HttpClientHandler httpClientHandler)
        {
            var underlyingHandler = GetUnderlyingHandlerField(httpClientHandler);
            if (underlyingHandler != null)
            {
                Debug.Assert(
                    underlyingHandler is SocketsHttpHandler,
                    $"Expected SocketsHttpHandler as underlying handler but got {underlyingHandler.GetType().Name}");
                return underlyingHandler;
            }
        }
#endif

        return handler;
    }

    private static HttpMessageHandler GetHandlerField(HttpClient client)
    {
#if NET8_0_OR_GREATER
        return UnsafeAccessor_Handler(client);
#else
        var field = typeof(HttpMessageInvoker).GetField("_handler", BindingFlags.NonPublic | BindingFlags.Instance);
        return field?.GetValue(client) as HttpMessageHandler;
#endif
    }

#if NET5_0_OR_GREATER
    private static HttpMessageHandler GetUnderlyingHandlerField(HttpClientHandler handler)
    {
#if NET8_0_OR_GREATER
        return UnsafeAccessor_UnderlyingHandler(handler);
#else
        var field = typeof(HttpClientHandler).GetField("_underlyingHandler", BindingFlags.NonPublic | BindingFlags.Instance);
        return field?.GetValue(handler) as HttpMessageHandler;
#endif
    }
#endif

#if NET8_0_OR_GREATER
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_handler")]
    private static extern ref HttpMessageHandler UnsafeAccessor_Handler(HttpMessageInvoker obj);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_underlyingHandler")]
    private static extern ref SocketsHttpHandler UnsafeAccessor_UnderlyingHandler(HttpClientHandler obj);
#endif
}
