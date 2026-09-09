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
