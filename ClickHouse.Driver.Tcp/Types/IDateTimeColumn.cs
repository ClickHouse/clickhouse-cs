using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Converts decoded <c>DateTime</c> and <c>DateTime64</c> counts to timezone-aware instants. The underlying
/// <see cref="IColumn{T}"/> view retains the exact integer counts.
/// </summary>
public interface IDateTimeColumn : IColumn
{
    /// <summary>
    /// The type's timezone, or the query's session timezone followed by the handshake timezone when unspecified.
    /// </summary>
    /// <remarks>
    /// ClickHouse accepts fixed offsets .NET cannot represent — <c>Fixed/UTC+19:00:00</c> is past
    /// <see cref="TimeZoneInfo"/>'s ±14 hours, and <c>Fixed/UTC+05:30:15</c> is not a whole number of minutes.
    /// The counts such a column carries still read through the <see cref="IColumn{T}"/> view; only this property
    /// and the instants below need the zone, so only they report it.
    /// </remarks>
    /// <exception cref="FormatException">The column's timezone cannot be represented on this platform.</exception>
    TimeZoneInfo TimeZone { get; }

    /// <summary>
    /// The number of decimal digits of sub-second precision the stored count carries: the <c>scale</c> of
    /// <c>DateTime64(scale)</c>, and <c>0</c> for <c>DateTime</c>, which counts whole seconds.
    /// </summary>
    int Scale { get; }

    /// <summary>Reads one instant; precision above scale 7 is truncated to 100 ns.</summary>
    /// <param name="row">The row index, from 0 to <see cref="IColumn.RowCount"/> - 1.</param>
    /// <returns>The instant the row's stored count names, to 100 ns.</returns>
    DateTimeOffset GetDateTimeOffset(int row);

    /// <summary>
    /// Reads the whole column as instants, offset to <see cref="TimeZone"/>.
    /// </summary>
    /// <remarks>Lossy above <see cref="Scale"/> 7, for the reason given on
    /// <see cref="GetDateTimeOffset"/>.</remarks>
    /// <returns>A new array of <see cref="IColumn.RowCount"/> instants. Unlike the borrowed spans elsewhere on
    /// the block tier, this array is the caller's and outlives the block.</returns>
    DateTimeOffset[] ToDateTimeOffsets();
}
