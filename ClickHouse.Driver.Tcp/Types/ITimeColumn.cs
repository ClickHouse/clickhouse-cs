using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Converts decoded <c>Time</c> and <c>Time64</c> counts to offsets from midnight. Values may be negative or
/// longer than a day; the underlying <see cref="IColumn{T}"/> view retains exact counts.
/// </summary>
public interface ITimeColumn : IColumn
{
    /// <summary>
    /// The number of decimal digits of sub-second precision the stored count carries: the <c>scale</c> of
    /// <c>Time64(scale)</c>, and <c>0</c> for <c>Time</c>, which counts whole seconds.
    /// </summary>
    int Scale { get; }

    /// <summary>Reads one offset from midnight; precision above scale 7 is truncated to 100 ns.</summary>
    /// <param name="row">The row index, from 0 to <see cref="IColumn.RowCount"/> - 1.</param>
    /// <returns>The offset from midnight the row's stored count names, to 100 ns.</returns>
    TimeSpan GetTimeSpan(int row);

    /// <summary>
    /// Reads the whole column as offsets from midnight.
    /// </summary>
    /// <remarks>Lossy above <see cref="Scale"/> 7, for the reason given on
    /// <see cref="GetTimeSpan"/>.</remarks>
    /// <returns>A new array of <see cref="IColumn.RowCount"/> offsets. Unlike the borrowed spans elsewhere on
    /// the block tier, this array is the caller's and outlives the block.</returns>
    TimeSpan[] ToTimeSpans();
}
