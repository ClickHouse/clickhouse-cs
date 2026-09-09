using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Compression;

/// <summary>
/// Which packet bodies travel in compression frames when a query requests compression.
/// <para>
/// Not every block-bearing packet is framed, which is the trap here: <c>Log</c> and <c>ProfileEvents</c>
/// carry blocks and are decoded by the same block reader, yet the server sends them uncompressed at our
/// protocol target (they become compressible only at revision 54481). Framing them would read a block name
/// as a frame checksum and desynchronize the connection the first time a server has <c>send_logs_level</c>
/// set.
/// </para>
/// <para>
/// The list matches the server's own notion of a compressible message, and the Go client's
/// <c>ServerCode.Compressible</c>, which admits the same three.
/// </para>
/// </summary>
internal static class FramedPackets
{
    /// <summary>Whether this server packet's block body arrives in frames while compression is active.</summary>
    /// <param name="packet">The packet type just read from the envelope.</param>
    /// <returns>True when the body is framed; false when it is written straight to the raw stream.</returns>
    public static bool CarriesFramedBody(ServerPacketType packet)
        => packet is ServerPacketType.Data or ServerPacketType.Totals or ServerPacketType.Extremes;
}
