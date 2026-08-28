using System;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Asserts the little-endian host the column codecs assume.
/// </summary>
/// <remarks>
/// The codecs reinterpret wire bytes as CLR values with <c>MemoryMarshal.Cast</c> and write them back with
/// <c>MemoryMarshal.AsBytes</c>. The native protocol is little-endian, so those casts are correct only on a
/// little-endian host. Every runtime .NET currently supports is little-endian, so this is a guard rather than
/// the alternative to a byte-swapping path.
/// </remarks>
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
