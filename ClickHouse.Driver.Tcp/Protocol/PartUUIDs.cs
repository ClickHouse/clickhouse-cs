using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// The PartUUIDs packet: a VarUInt count followed by that many 16-byte part UUIDs. The server sends it during
/// query execution when part-level query deduplication is active. The client has no result surface for these
/// values yet, so the body is read purely to keep the stream aligned and the UUIDs are discarded.
/// </summary>
internal static class PartUUIDs
{
    /// <summary>
    /// Reads and discards the PartUUIDs body, leaving the reader positioned at the next packet boundary.
    /// </summary>
    /// <param name="reader">The reader positioned at the start of the packet body.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    public static async ValueTask ConsumeAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        ulong count = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        for (ulong i = 0; i < count; i++)
        {
            // Each UUID is a fixed 16-byte value; read and drop it via the zero-allocation fixed-width path.
            await reader.ReadUInt128Async(cancellationToken).ConfigureAwait(false);
        }
    }
}
