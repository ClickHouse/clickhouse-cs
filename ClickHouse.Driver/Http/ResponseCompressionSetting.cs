using System;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Http;

/// <summary>
/// Maps the connection-string <c>ResponseCompression</c> keyword onto a built-in
/// <see cref="IClickHouseCompressor"/> and back. Connection-string parity matters because ORM users
/// (Dapper, EF Core, linq2db) configure the driver through a connection string and never touch
/// <see cref="ADO.ClickHouseClientSettings"/> directly.
/// </summary>
internal static class ResponseCompressionSetting
{
    /// <summary>
    /// Parses a <c>ResponseCompression</c> keyword value. <see langword="null"/>, empty and
    /// <c>none</c>/<c>identity</c>/<c>off</c>/<c>false</c> all mean "no response decompression".
    /// Comparison is case-insensitive, culture-invariant and whitespace-tolerant.
    /// </summary>
    /// <exception cref="ArgumentException">The value names a codec the driver has no built-in compressor for.</exception>
    public static IClickHouseCompressor Parse(string value)
    {
        var token = value?.Trim();
        if (string.IsNullOrEmpty(token))
            return null;

        if (Is(token, "none") || Is(token, "identity") || Is(token, "off") || Is(token, "false"))
            return null;

        if (Is(token, "lz4"))
            return Lz4Compressor.Default;

        if (Is(token, "gzip"))
            return GZipCompressor.Default;

        if (Is(token, "br") || Is(token, "brotli"))
            return BrotliCompressor.Default;

        throw new ArgumentException(
            $"Unsupported ResponseCompression value '{token}'. Supported values are 'lz4', 'gzip', 'br' and 'none'. " +
            "For any other codec, set ClickHouseClientSettings.ResponseCompressor to a custom IClickHouseCompressor instead.",
            nameof(value));
    }

    /// <summary>
    /// Renders a compressor back into a connection-string keyword value. Returns <see langword="null"/>
    /// for no compressor and for custom implementations, which cannot be expressed in a connection string.
    /// </summary>
    public static string Format(IClickHouseCompressor compressor) => compressor switch
    {
        Lz4Compressor => "lz4",
        GZipCompressor => "gzip",
        BrotliCompressor => "br",
        _ => null,
    };

    private static bool Is(string value, string expected)
        => string.Equals(value, expected, StringComparison.OrdinalIgnoreCase);
}
