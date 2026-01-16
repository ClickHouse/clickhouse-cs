using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace ClickHouse.Driver.ADO;

/// <summary>
/// Represents the raw HTTP response from a ClickHouse query executed with a custom FORMAT clause.
/// </summary>
/// <remarks>
/// This class provides direct access to the response content without any parsing,
/// allowing you to handle custom output formats (e.g., FORMAT JSON, FORMAT CSV) yourself.
/// </remarks>
public class ClickHouseRawResult : IDisposable
{
    private readonly HttpResponseMessage response;

    internal ClickHouseRawResult(HttpResponseMessage response)
    {
        this.response = response;
    }

    /// <summary>
    /// Reads the response content as a stream.
    /// </summary>
    /// <returns>A task that resolves to the response content stream.</returns>
    public Task<Stream> ReadAsStreamAsync() => response.Content.ReadAsStreamAsync();

    /// <summary>
    /// Reads the response content as a byte array.
    /// </summary>
    /// <returns>A task that resolves to the response content as bytes.</returns>
    public Task<byte[]> ReadAsByteArrayAsync() => response.Content.ReadAsByteArrayAsync();

    /// <summary>
    /// Reads the response content as a string.
    /// </summary>
    /// <returns>A task that resolves to the response content as a string.</returns>
    public Task<string> ReadAsStringAsync() => response.Content.ReadAsStringAsync();

    /// <summary>
    /// Copies the response content to the specified stream.
    /// </summary>
    /// <param name="stream">The destination stream to copy the content to.</param>
    /// <returns>A task that completes when the copy operation is finished.</returns>
    public Task CopyToAsync(Stream stream) => response.Content.CopyToAsync(stream);

    public void Dispose()
    {
        response?.Dispose();
        GC.SuppressFinalize(this);
    }
}
