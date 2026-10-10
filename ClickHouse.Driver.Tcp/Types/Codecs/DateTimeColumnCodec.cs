using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// Encodes ClickHouse <c>DateTime</c> as Unix seconds. The explicit or session timezone controls
/// <see cref="DateTimeOffset"/> projections and how unspecified <see cref="DateTime"/> values are interpreted.
/// Timezone errors are deferred until a calendar value is requested.
/// </summary>
internal sealed class DateTimeColumnCodec : IColumnCodec
{
    private readonly ResolvedTimeZone timeZone;

    private DateTimeColumnCodec(string typeName, ResolvedTimeZone timeZone)
    {
        TypeName = typeName;
        this.timeZone = timeZone;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(uint);

    /// <summary>The timezone of the column: from the type string, else from the session, else UTC.</summary>
    internal ResolvedTimeZone TimeZone => timeZone;

    /// <summary>Builds a <c>DateTime</c> codec, resolving its timezone from the type string or the session.</summary>
    /// <param name="node">The parsed <c>DateTime</c> type node (its optional argument is the timezone).</param>
    /// <param name="serverTimezone">The session timezone, used when the type string carries none.</param>
    /// <returns>The codec.</returns>
    public static DateTimeColumnCodec Create(TypeNode node, string serverTimezone)
    {
        string explicitTz = node.Arguments.Count > 0 ? DateTimeZones.UnquoteTimezone(node.Arguments[0]) : null;
        ResolvedTimeZone tz = DateTimeZones.Resolve(explicitTz, serverTimezone);
        return new DateTimeColumnCodec(node.ToString(), tz);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => DateTimeColumn.ReadAsync(reader, columnName, columnType, timeZone, rowCount, cancellationToken);

    /// <inheritdoc/>
    // The column that a query of the type reads.
    public bool CanWrite(IColumn column) => column is DateTimeColumn;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        // The decoded column stores the wire values.
        var stored = (DateTimeColumn)column;
        writer.WriteBytes(MemoryMarshal.AsBytes(stored.Values.Slice(start, length)));
    }

    // Reduces a DateTime to the UTC instant to encode. Utc and Local already denote an instant; a Local value
    // resolves against the host machine's timezone, under the BCL's daylight-saving rules and not the ones below.
    // An Unspecified value has no offset, so its wall-clock is read in the column's timezone.
    // Resolve the column timezone only for an Unspecified wall clock.
    internal static DateTime ToUtc(DateTime value, ResolvedTimeZone resolved)
    {
        if (value.Kind != DateTimeKind.Unspecified)
        {
            return value.ToUniversalTime();
        }

        TimeZoneInfo timeZone = resolved.Value;

        // A skipped wall-clock names no instant, so it is rejected instead of guessed. Deriving the pre-gap offset
        // from TimeZoneInfo is not reliable: GetUtcOffset answers with the zone's base offset, which differs from
        // the pre-gap one wherever the historical standard offset did (America/Juneau in the 1970s, and 27 other
        // zones as late as 2023).
        if (timeZone.IsInvalidTime(value))
        {
            throw new ArgumentException(
                $"{value:yyyy-MM-dd HH:mm:ss} does not exist in '{timeZone.Id}': a daylight-saving change skips it. " +
                "Pass a DateTimeOffset, or a DateTime with Kind=Utc, to name the instant you mean.",
                nameof(value));
        }

        // An hour that occurs twice takes the earlier occurrence: the larger offset, since it subtracts to an
        // earlier instant. A fall-back always lowers the offset, so this holds in every zone.
        if (timeZone.IsAmbiguousTime(value))
        {
            TimeSpan[] offsets = timeZone.GetAmbiguousTimeOffsets(value);
            TimeSpan earliest = offsets[0];
            for (int i = 1; i < offsets.Length; i++)
            {
                if (offsets[i] > earliest)
                {
                    earliest = offsets[i];
                }
            }

            return DateTime.SpecifyKind(value - earliest, DateTimeKind.Utc);
        }

        return TimeZoneInfo.ConvertTimeToUtc(value, timeZone);
    }
}
