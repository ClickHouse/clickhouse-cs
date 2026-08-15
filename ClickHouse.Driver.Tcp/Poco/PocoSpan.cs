using System;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Span access for the compiled POCO scatters.
/// </summary>
internal static class PocoSpan
{
    /// <summary>
    /// Reads one element of a span. An expression tree cannot consume <see cref="ReadOnlySpan{T}"/>'s indexer
    /// directly, because it returns <c>ref readonly T</c> and a tree has no way to express a managed reference
    /// (<c>Expression of type 'System.Int32&amp;' cannot be used for return type 'System.Int32'</c>). Routing the
    /// index through this method returns the element by value instead; the JIT inlines it away, so the compiled
    /// scatter still reads straight out of the column's borrowed storage.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="span">The span to read from.</param>
    /// <param name="index">The zero-based element index.</param>
    /// <returns>The element at that index.</returns>
    public static T At<T>(ReadOnlySpan<T> span, int index) => span[index];
}
