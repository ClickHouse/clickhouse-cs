using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Encodes ClickHouse <c>DateTime64</c> as signed counts at its declared scale. Reads preserve the raw
/// <see cref="long"/> value; the explicit or session timezone controls <see cref="DateTimeOffset"/> projections and
/// how unspecified <see cref="DateTime"/> values are interpreted.
/// </summary>
internal sealed class DateTime64ColumnCodec : IColumnCodec
{
    private const int MaxScale = 9; // Nanoseconds — the finest scale ClickHouse DateTime64 supports.

    private readonly int scale;
    private readonly ResolvedTimeZone timeZone;

    private DateTime64ColumnCodec(string typeName, int scale, ResolvedTimeZone timeZone)
    {
        TypeName = typeName;
        this.scale = scale;
        this.timeZone = timeZone;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(long);

    /// <summary>The number of decimal digits after the second (0 to 9).</summary>
    internal int Scale => scale;

    /// <summary>The timezone of the column: from the type string, else from the session, else UTC.</summary>
    internal ResolvedTimeZone TimeZone => timeZone;

    /// <summary>Builds a <c>DateTime64</c> codec from its scale and optional timezone arguments.</summary>
    /// <param name="node">The parsed <c>DateTime64</c> type node.</param>
    /// <param name="serverTimezone">The session timezone, used when the type string carries none.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The scale argument is missing, malformed, or out of the range 0..9.</exception>
    public static DateTime64ColumnCodec Create(TypeNode node, string serverTimezone)
    {
        if (node.Arguments.Count == 0 || !int.TryParse(node.Arguments[0].Name.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out int scale))
        {
            throw new FormatException($"DateTime64 type '{node}' must specify a numeric scale, e.g. DateTime64(3).");
        }

        if (scale is < 0 or > MaxScale)
        {
            throw new FormatException($"DateTime64 scale {scale} is out of the supported range 0..{MaxScale}.");
        }

        string explicitTz = node.Arguments.Count > 1 ? DateTimeZones.UnquoteTimezone(node.Arguments[1]) : null;
        ResolvedTimeZone tz = DateTimeZones.Resolve(explicitTz, serverTimezone);
        return new DateTime64ColumnCodec(node.ToString(), scale, tz);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => DateTime64Column.ReadAsync(reader, columnName, columnType, scale, timeZone, rowCount, cancellationToken);

    /// <inheritdoc/>
    // The column that a query of the type reads.
    public bool CanWrite(IColumn column) => column is DateTime64Column;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        // The decoded column stores the wire values.
        var stored = (DateTime64Column)column;
        writer.WriteBytes(MemoryMarshal.AsBytes(stored.Values.Slice(start, length)));
    }
}
