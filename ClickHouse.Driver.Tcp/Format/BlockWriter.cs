using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// Writes outgoing blocks: the empty end-of-input marker sent after a Query, and the populated data blocks an
/// INSERT streams to the server. The wire layout mirrors <see cref="BlockReader"/>.
/// </summary>
internal static class BlockWriter
{
    /// <summary>
    /// The default per-block encoded-size target (50 MiB): the insert path sizes blocks to stay within it, and
    /// the encoder flushes between columns once the buffer passes it as a backstop.
    /// </summary>
    public const int DefaultFlushThresholdBytes = 50 * 1024 * 1024;

    /// <summary>
    /// Writes the empty end-of-input block: an empty name, the standard block info, and zero column/row counts.
    /// Does not flush.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    public static void WriteEmptyBlock(ClickHouseBinaryWriter writer)
    {
        writer.WriteString(string.Empty);
        WriteEmptyBlockBody(writer);
    }

    /// <summary>
    /// Writes the empty end-of-input block without its name, which the caller has already written.
    /// <para>
    /// The split exists because the name belongs to the packet envelope rather than to the block: with
    /// compression on the name is written raw and the body is framed, so the caller writes the name to the
    /// raw writer and passes a plaintext writer here.
    /// </para>
    /// </summary>
    /// <param name="writer">The writer to encode the body into, plaintext if the body is framed.</param>
    public static void WriteEmptyBlockBody(ClickHouseBinaryWriter writer)
    {
        WriteBlockInfo(writer, BlockInfo.Default);
        writer.WriteVarUInt(0); // num_columns
        writer.WriteVarUInt(0); // num_rows
    }

    /// <summary>
    /// Writes a populated data block from whole columns, resolving each column's codec from its own type string.
    /// Every column must hold exactly <paramref name="rowCount"/> rows.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="negotiated">The negotiated protocol, gating the <c>has_custom_serialization</c> byte.</param>
    /// <param name="columns">The columns to write, in header order; each supplies its own name, type, and values.</param>
    /// <param name="rowCount">The number of rows every column holds.</param>
    /// <param name="registry">The registry that resolves each column's type string to its codec.</param>
    /// <param name="flushThresholdBytes">The buffered-byte cap that triggers a between-column flush.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    public static ValueTask WriteDataBlockAsync(
        ClickHouseBinaryWriter writer,
        NegotiatedProtocol negotiated,
        IReadOnlyList<IColumn> columns,
        int rowCount,
        ColumnCodecRegistry registry,
        int flushThresholdBytes,
        CancellationToken cancellationToken)
    {
        // A column whose length disagrees with the declared row count would corrupt the block. Catch it first.
        foreach (IColumn column in columns)
        {
            if (column.RowCount != rowCount)
            {
                throw new ArgumentException(
                    $"Column '{column.Name}' holds {column.RowCount} row(s) but the block declares {rowCount}; every column must match the block's row count.",
                    nameof(columns));
            }
        }

        // A self-described block has no server sample/context to borrow. Explicit type timezones still apply;
        // timezone-less DateTime types fall back to UTC for an Unspecified wall clock.
        ResolveContext context = ResolveContext.ForWrite;
        var planned = new InsertColumn[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            IColumn column = columns[i];
            planned[i] = new InsertColumn(column.Name, column.TypeName, registry.Resolve(column.TypeName, in context), column);
        }

        return WriteDataBlockAsync(writer, negotiated, planned, start: 0, rowCount, flushThresholdBytes, cancellationToken);
    }

    /// <summary>
    /// Writes a populated data block covering rows <c>[start, start + rowCount)</c> of each column, read straight
    /// from its borrowed span. Each column's header and codec come from the descriptor (the target schema's
    /// authoritative name and resolved type), not the value column.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="negotiated">The negotiated protocol, gating the <c>has_custom_serialization</c> byte.</param>
    /// <param name="columns">The columns to write, in header order; each pairs a target header and codec with values.</param>
    /// <param name="start">The zero-based first row of the range each column contributes.</param>
    /// <param name="rowCount">The number of rows the block holds.</param>
    /// <param name="flushThresholdBytes">The buffered-byte cap that triggers a between-column flush.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    public static async ValueTask WriteDataBlockAsync(
        ClickHouseBinaryWriter writer,
        NegotiatedProtocol negotiated,
        IReadOnlyList<InsertColumn> columns,
        int start,
        int rowCount,
        int flushThresholdBytes,
        CancellationToken cancellationToken)
    {
        EnsureRangeWithinColumns(columns, start, rowCount);

        writer.WriteString(string.Empty); // table_name: empty for the INSERT row stream
        await WriteDataBlockBodyCoreAsync(writer, negotiated, columns, start, rowCount, flushThresholdBytes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a populated block's body without its name, which the caller has already written. See
    /// <see cref="WriteEmptyBlockBody"/> for why the name is written separately.
    /// </summary>
    /// <param name="writer">The writer to encode the body into, plaintext if the body is framed.</param>
    /// <param name="negotiated">The negotiated protocol, gating the <c>has_custom_serialization</c> byte.</param>
    /// <param name="columns">The columns to write, in header order.</param>
    /// <param name="start">The zero-based first row of the range each column contributes.</param>
    /// <param name="rowCount">The number of rows the block holds.</param>
    /// <param name="flushThresholdBytes">The buffered-byte cap that triggers a between-column flush.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    public static async ValueTask WriteDataBlockBodyAsync(
        ClickHouseBinaryWriter writer,
        NegotiatedProtocol negotiated,
        IReadOnlyList<InsertColumn> columns,
        int start,
        int rowCount,
        int flushThresholdBytes,
        CancellationToken cancellationToken)
    {
        EnsureRangeWithinColumns(columns, start, rowCount);
        await WriteDataBlockBodyCoreAsync(writer, negotiated, columns, start, rowCount, flushThresholdBytes, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes the block info, the counts and every column. The row range is already validated.</summary>
    private static async ValueTask WriteDataBlockBodyCoreAsync(
        ClickHouseBinaryWriter writer,
        NegotiatedProtocol negotiated,
        IReadOnlyList<InsertColumn> columns,
        int start,
        int rowCount,
        int flushThresholdBytes,
        CancellationToken cancellationToken)
    {
        WriteBlockInfo(writer, BlockInfo.Default);
        writer.WriteVarUInt((ulong)columns.Count); // num_columns
        writer.WriteVarUInt((ulong)rowCount);       // num_rows

        foreach (InsertColumn column in columns)
        {
            writer.WriteString(column.Name);
            writer.WriteString(column.TypeName);
            if (negotiated.Supports(ProtocolFeature.CustomSerialization))
            {
                writer.WriteBool(false); // has_custom_serialization: default serialization only
            }

            // A zero-row block has no state prefix or body; skip both rather than rely on every codec treating
            // an empty range as a no-op (the Nothing codec, for one, refuses to write at all).
            if (rowCount != 0)
            {
                // Compute any per-operation scratch once, share it across the prefix and body phases (a
                // data-dependent prefix and the element-flattening composites need this), and free it after.
                IColumnWriteState state = column.Codec.BeginWrite(column.Values, start, rowCount);
                try
                {
                    column.Codec.WriteStatePrefix(writer, column.Values, start, rowCount, state);
                    column.Codec.WriteColumn(writer, column.Values, start, rowCount, state);
                }
                finally
                {
                    state?.Dispose();
                }
            }

            // Backstop: flush between columns once the buffer passes the threshold, so a wide block doesn't
            // balloon before the message-boundary flush.
            if (writer.BufferedBytes >= flushThresholdBytes)
            {
                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Checks that the requested row range lies within every column, before anything is written: a range past
    /// the values would run the body off the end of a column.
    /// </summary>
    /// <param name="columns">The columns the block will draw from.</param>
    /// <param name="start">The zero-based first row of the range.</param>
    /// <param name="rowCount">The number of rows requested.</param>
    /// <exception cref="ArgumentException">A column cannot supply the requested range.</exception>
    private static void EnsureRangeWithinColumns(IReadOnlyList<InsertColumn> columns, int start, int rowCount)
    {
        foreach (InsertColumn column in columns)
        {
            if (start < 0 || rowCount < 0 || start + (long)rowCount > column.Values.RowCount)
            {
                throw new ArgumentException(
                    $"Column '{column.Name}' cannot supply rows [{start}, {start + (long)rowCount}) of its {column.Values.RowCount} row(s).",
                    nameof(columns));
            }
        }
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
