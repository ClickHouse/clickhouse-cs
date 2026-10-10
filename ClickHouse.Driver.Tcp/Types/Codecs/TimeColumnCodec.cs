using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Time</c> column: a little-endian <c>Int32</c> second count (a signed
/// time-of-day/duration, not tied to a date), surfaced as the raw <see cref="int"/> second count. The
/// representable range is [-999:59:59, 999:59:59]. The converter layer also writes the type from a
/// <see cref="TimeSpan"/> or a <see cref="TimeOnly"/>.
/// <para>
/// Reading as a <see cref="TimeOnly"/> is a narrowing: a column value may be negative or past 24 hours, which no
/// time of day is, and such a row is refused rather than reduced modulo a day.
/// </para>
/// </summary>
internal sealed class TimeColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly TimeColumnCodec Instance = new();

    private TimeColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "Time";

    /// <inheritdoc/>
    public Type ElementType => typeof(int);

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => TimeColumn.ReadAsync(reader, columnName, columnType, rowCount, cancellationToken);

    /// <inheritdoc/>
    // The column that a query of the type reads.
    public bool CanWrite(IColumn column) => column is TimeColumn;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        // The decoded column stores the wire values.
        var stored = (TimeColumn)column;
        writer.WriteBytes(MemoryMarshal.AsBytes(stored.Values.Slice(start, length)));
    }
}
