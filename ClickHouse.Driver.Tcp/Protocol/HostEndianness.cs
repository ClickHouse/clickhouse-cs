using System;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Enforces the little-endian host required by codecs that reinterpret native-protocol bytes as CLR values.
/// </summary>
internal static class HostEndianness
{
    /// <summary>Throws if this host is big-endian.</summary>
    /// <exception cref="PlatformNotSupportedException">The host is big-endian.</exception>
    public static void RequireLittleEndian()
    {
        if (!BitConverter.IsLittleEndian)
        {
            throw new PlatformNotSupportedException(
                "ClickHouse.Driver.Tcp requires a little-endian host: the native protocol is little-endian and " +
                "the column codecs map wire bytes onto CLR values directly. Use the HTTP driver " +
                "(ClickHouse.Driver) on this platform.");
        }
    }
}
