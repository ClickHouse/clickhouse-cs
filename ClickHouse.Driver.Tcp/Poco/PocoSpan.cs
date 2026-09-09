using System;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Span access for the compiled POCO scatters.
/// </summary>
internal static class PocoSpan
{
    /// <summary>
    /// Reads a span element by value, which expression trees can represent unlike the span's ref-returning indexer.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="span">The span to read from.</param>
    /// <param name="index">The zero-based element index.</param>
    /// <returns>The element at that index.</returns>
    public static T At<T>(ReadOnlySpan<T> span, int index) => span[index];
}
