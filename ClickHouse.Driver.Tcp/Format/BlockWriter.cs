using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// Writes outgoing blocks. This slice needs only the empty block — the client sends one, with no columns and no
/// rows, as the end-of-input "go" marker after a Query so the server begins executing. Writing populated blocks
/// (for INSERT) is added later.
/// </summary>
internal static class BlockWriter
{
    /// <summary>
    /// Writes the empty end-of-input block: an empty name, the standard block info, and zero column/row counts.
    /// Does not flush.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    public static void WriteEmptyBlock(ClickHouseBinaryWriter writer)
    {
        writer.WriteString(string.Empty);
        WriteBlockInfo(writer, BlockInfo.Default);
        writer.WriteVarUInt(0); // num_columns
        writer.WriteVarUInt(0); // num_rows
    }

    /// <summary>Writes the field-id-tagged block info: <c>is_overflows</c>, <c>bucket_number</c>, then the terminator.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="info">The block info to write.</param>
    internal static void WriteBlockInfo(ClickHouseBinaryWriter writer, BlockInfo info)
    {
        writer.WriteVarUInt(BlockInfo.IsOverflowsFieldId);
        writer.WriteBool(info.IsOverflows);
        writer.WriteVarUInt(BlockInfo.BucketNumberFieldId);
        writer.WriteInt32(info.BucketNumber);
        writer.WriteVarUInt(BlockInfo.TerminatorFieldId);
    }
}
