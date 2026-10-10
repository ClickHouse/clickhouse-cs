using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Time64(scale)</c> column: a little-endian <c>Int64</c> tick count at
/// 10^-<c>scale</c> seconds (a signed time-of-day/duration), surfaced as the raw <see cref="long"/> count that
/// retains the exact wire value at any scale (including scales 8 and 9, which are finer than a .NET tick). The
/// converter layer also writes the type from a <see cref="TimeSpan"/> or a <see cref="TimeOnly"/>, truncated toward
/// zero to the column scale.
/// <para>
/// Reading as a <see cref="TimeOnly"/> is a narrowing: a column value may be negative or past 24 hours, which no
/// time of day is, and such a row is refused rather than reduced modulo a day.
/// </para>
/// </summary>
internal sealed class Time64ColumnCodec : IColumnCodec
{
    private readonly int scale;

    private Time64ColumnCodec(string typeName, int scale)
    {
        TypeName = typeName;
        this.scale = scale;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(long);

    /// <summary>The number of decimal digits after the second (0 to 9).</summary>
    internal int Scale => scale;

    /// <summary>Builds a <c>Time64</c> codec from its scale argument.</summary>
    /// <param name="node">The parsed <c>Time64</c> type node.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The scale argument is missing, malformed, or out of the range 0..9.</exception>
    public static Time64ColumnCodec Create(TypeNode node)
    {
        if (node.Arguments.Count == 0 || !int.TryParse(node.Arguments[0].Name.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out int scale))
        {
            throw new FormatException($"Time64 type '{node}' must specify a numeric scale, e.g. Time64(3).");
        }

        if (scale is < 0 or > 9)
        {
            throw new FormatException($"Time64 scale {scale} is out of the supported range 0..9.");
        }

        return new Time64ColumnCodec(node.ToString(), scale);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => Time64Column.ReadAsync(reader, columnName, columnType, scale, rowCount, cancellationToken);

    /// <inheritdoc/>
    // The column that a query of the type reads, at the same scale: a stored count is a count at the scale of the column
    // that holds it.
    public bool CanWrite(IColumn column) => column is Time64Column stored && stored.Scale == scale;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        // The decoded column stores the wire values.
        var stored = (Time64Column)column;
        writer.WriteBytes(MemoryMarshal.AsBytes(stored.Values.Slice(start, length)));
    }
}
