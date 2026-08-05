using System;
using ClickHouse.Driver.Formats;
using NodaTime;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Decodes a result-set row, capturing for every column that carries one (see <see cref="IInstantReader"/>)
/// the instant its value was stored as, and turns that instant into a <see cref="DateTimeOffset"/> on
/// request. This keeps instant handling with the date/time types: a reader owns one instance and forwards
/// to it, instead of tracking instants itself.
/// </summary>
internal sealed class RowInstantReader
{
    private readonly IInstantReader[] columns; // Null entry: column carries no instant, read it plainly
    private readonly Instant?[] instants; // Instants captured for the current row, by ordinal

    private RowInstantReader(IInstantReader[] columns)
    {
        this.columns = columns;
        instants = new Instant?[columns.Length];
    }

    /// <summary>
    /// Returns a reader for <paramref name="types"/>, or <see langword="null"/> when none of them carries an
    /// instant, in which case the caller keeps its plain decode loop rather than paying for capture that
    /// would never report anything.
    /// </summary>
    internal static RowInstantReader Create(ClickHouseType[] types)
    {
        IInstantReader[] columns = null;
        for (var i = 0; i < types.Length; i++)
        {
            if (types[i] is IInstantReader { ReportsInstant: true } instantReader)
            {
                columns ??= new IInstantReader[types.Length];
                columns[i] = instantReader;
            }
        }

        return columns == null ? null : new RowInstantReader(columns);
    }

    /// <summary>
    /// Reads one row into <paramref name="data"/>, capturing the instant of every column that carries one.
    /// Columns that carry none read exactly as they would without capture.
    /// </summary>
    internal void ReadRow(ExtendedBinaryReader reader, ClickHouseType[] types, object[] data)
    {
        for (var i = 0; i < data.Length; i++)
        {
            var instantReader = columns[i];
            if (instantReader == null)
                data[i] = types[i].Read(reader);
            else
                data[i] = instantReader.ReadWithInstant(reader, out instants[i]);
        }
    }

    /// <summary>
    /// Reports the <see cref="DateTimeOffset"/> of the instant captured for <paramref name="ordinal"/> in the
    /// current row, interpreted in <paramref name="type"/>'s timezone. Returns <see langword="false"/> when
    /// the column carried no instant, so the caller falls back to coercing the decoded value.
    /// <paramref name="visibleValue"/> is the value the caller actually sees when it may differ from the
    /// decoded one (a read-value converter can replace it); the captured instant is used only while it still
    /// describes that value.
    /// </summary>
    internal bool TryGetDateTimeOffset(int ordinal, AbstractDateTimeType type, DateTime? visibleValue, out DateTimeOffset result)
    {
        if (instants[ordinal] is Instant instant &&
            (visibleValue is not DateTime visible || type.ToDateTime(instant) == visible))
        {
            result = type.ToDateTimeOffset(instant);
            return true;
        }

        result = default;
        return false;
    }
}
