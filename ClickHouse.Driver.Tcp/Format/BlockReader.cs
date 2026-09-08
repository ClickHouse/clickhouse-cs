using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// Reads a full block from the wire: the leading block name, the block info, the column/row counts, and each
/// column (header plus body via its codec). This one reader serves every block-bearing packet — Data, Totals,
/// Extremes, Log, and ProfileEvents — so they share an identical entry point.
/// </summary>
internal static class BlockReader
{
    /// <summary>Reads one block using the given codec registry, at the negotiated protocol version.</summary>
    /// <param name="reader">The reader positioned at the block name.</param>
    /// <param name="negotiated">The negotiated protocol, for version-gated header fields.</param>
    /// <param name="registry">The registry that resolves each column's type string to a codec.</param>
    /// <param name="context">The resolution context (e.g. the server timezone) passed to each column's codec factory.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded block.</returns>
    /// <exception cref="ClickHouseProtocolException">A column uses unsupported custom serialization, or a count is implausible.</exception>
    public static async ValueTask<Block> ReadBlockAsync(
        ClickHouseBinaryReader reader,
        NegotiatedProtocol negotiated,
        ColumnCodecRegistry registry,
        ResolveContext context,
        CancellationToken cancellationToken)
    {
        string name = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
        BlockInfo info = await ReadBlockInfoAsync(reader, cancellationToken).ConfigureAwait(false);

        int columnCount = ToColumnCount(await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false));
        int rowCount = ToCount(await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false), "rows");

        var columns = new IColumn[columnCount];
        try
        {
            for (int i = 0; i < columnCount; i++)
            {
                string columnName = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);
                string columnType = await reader.ReadStringAsync(cancellationToken).ConfigureAwait(false);

                if (negotiated.Supports(ProtocolFeature.CustomSerialization))
                {
                    bool hasCustomSerialization = await reader.ReadBoolAsync(cancellationToken).ConfigureAwait(false);
                    if (hasCustomSerialization)
                    {
                        throw new ClickHouseProtocolException(
                            $"Column '{columnName}' ({columnType}) uses custom serialization, which this client does not support.");
                    }
                }

                IColumnCodec codec = registry.Resolve(columnType, in context);

                // A zero-row block (a schema header, or an end-of-input marker) carries no state prefix and no body.
                // TODO: this holds for every type with default serialization, but dictionary-bearing types
                // (e.g. LowCardinality) write a serialization-state prefix even when empty. When such types are
                // added, the reader and writer must emit their prefix regardless of row count or the stream desyncs.
                if (rowCount != 0)
                {
                    await codec.ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
                }

                columns[i] = await codec.ReadColumnAsync(reader, columnName, columnType, rowCount, cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            // Return any pooled buffers from columns read before the failure; the block is never handed out.
            foreach (IColumn column in columns)
            {
                column?.Dispose();
            }

            throw;
        }

        return new Block(name, info, rowCount, columns);
    }

    /// <summary>
    /// Reads the field-id-tagged block info, stopping at the terminator. Each field is a VarUInt id followed
    /// by its value: id 1 is <c>is_overflows</c> (a byte), id 2 is <c>bucket_number</c> (Int32), id 0
    /// terminates. Any other id is a protocol violation.
    /// </summary>
    /// <param name="reader">The reader positioned at the start of the block info.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded block info.</returns>
    /// <exception cref="ClickHouseProtocolException">An unknown field id was encountered.</exception>
    internal static async ValueTask<BlockInfo> ReadBlockInfoAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        bool isOverflows = false;
        int bucketNumber = -1;

        while (true)
        {
            ulong fieldId = await reader.ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
            switch (fieldId)
            {
                case BlockInfo.TerminatorFieldId:
                    return new BlockInfo(isOverflows, bucketNumber);
                case BlockInfo.IsOverflowsFieldId:
                    isOverflows = await reader.ReadBoolAsync(cancellationToken).ConfigureAwait(false);
                    break;
                case BlockInfo.BucketNumberFieldId:
                    bucketNumber = await reader.ReadInt32Async(cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new ClickHouseProtocolException($"Unknown BlockInfo field id {fieldId} (corrupt stream or unsupported protocol feature).");
            }
        }
    }

    // A defensive ceiling on the column count read from the wire.
    private const int MaxColumnsPerBlock = 1_000_000;

    /// <summary>Narrows the column count to int and rejects a value beyond the defensive ceiling.</summary>
    /// <param name="value">The raw count.</param>
    /// <returns>The count as an int.</returns>
    /// <exception cref="ClickHouseProtocolException">The count exceeds <see cref="MaxColumnsPerBlock"/>.</exception>
    private static int ToColumnCount(ulong value)
    {
        if (value > MaxColumnsPerBlock)
        {
            throw new ClickHouseProtocolException(
                $"Block declares {value} columns, exceeding the supported maximum of {MaxColumnsPerBlock} (corrupt stream).");
        }

        return (int)value;
    }

    /// <summary>Narrows a VarUInt count to int, rejecting an implausibly large value.</summary>
    /// <param name="value">The raw count.</param>
    /// <param name="what">The count's name, for error messages.</param>
    /// <returns>The count as an int.</returns>
    /// <exception cref="ClickHouseProtocolException">The count exceeds <see cref="int.MaxValue"/>.</exception>
    private static int ToCount(ulong value, string what)
    {
        if (value > int.MaxValue)
        {
            throw new ClickHouseProtocolException($"Block declares {value} {what}, exceeding the supported maximum (corrupt stream).");
        }

        return (int)value;
    }
}
