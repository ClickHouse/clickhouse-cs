using System;
using ClickHouse.Driver.Formats;
using NodaTime;

namespace ClickHouse.Driver.Types;

/// <summary>
/// The <see cref="ITypedReader{T}"/> a date/time column's slot decodes through, which keeps the instant the
/// current row's value was stored as. <c>GetDateTimeOffset</c> needs that instant: the decoded
/// <see cref="DateTime"/> is a wall clock, and inside a DST fall-back hour one wall clock occurs at two
/// offsets, so re-interpreting it in the column timezone cannot tell the two apart and resolves to the
/// earlier one — a different instant for the second occurrence.
///
/// <para>One instance per column per reader, so the captured instant is per-result-set state and never
/// shared: the resolved <see cref="ClickHouseType"/> itself is cached and reused across readers, rows and
/// array elements, and could not hold it.</para>
/// </summary>
internal sealed class InstantCapturingReader : ITypedReader<DateTime>
{
    private readonly AbstractDateTimeType type;
    private readonly IInstantReader instantReader; // Same object as type; typed separately to avoid a per-row cast

    private Instant captured;

    private InstantCapturingReader(AbstractDateTimeType type, IInstantReader instantReader)
    {
        this.type = type;
        this.instantReader = instantReader;
    }

    /// <summary>
    /// Returns a capturing reader for <paramref name="type"/>, or <see langword="null"/> when the type
    /// decodes no instant (<c>Date</c>/<c>Date32</c>, and everything that is not a date/time type), in which
    /// case the column keeps its plain typed reader.
    /// </summary>
    internal static InstantCapturingReader TryCreate(ClickHouseType type)
        => type is AbstractDateTimeType dateTimeType && type is IInstantReader instantReader &&
           type.FrameworkType == typeof(DateTime) // Same rule the typed binders follow: never bind a slot to a representation the boxed read would not have produced
            ? new InstantCapturingReader(dateTimeType, instantReader)
            : null;

    /// <summary>
    /// Decodes the value exactly as the type's own <see cref="DateTime"/> read does — same bytes consumed,
    /// same value produced — while keeping the instant it came from.
    /// </summary>
    public DateTime ReadValue(ExtendedBinaryReader reader)
    {
        captured = instantReader.ReadInstant(reader);
        return type.ToDateTime(captured);
    }

    /// <summary>
    /// Reports the captured instant as a <see cref="DateTimeOffset"/> in the column timezone, preserving the
    /// instant and giving it the offset that instant actually had.
    /// <paramref name="visibleValue"/> is the value the caller sees when it can differ from the decoded one
    /// (an <see cref="ADO.Readers.IReadValueConverter"/> can replace it); the captured instant is used only
    /// while it still describes that value, so a converter that changes the value falls back to coercion.
    /// </summary>
    internal bool TryGetDateTimeOffset(DateTime? visibleValue, out DateTimeOffset result)
    {
        if (visibleValue is DateTime visible && type.ToDateTime(captured) != visible)
        {
            result = default;
            return false;
        }

        result = type.ToDateTimeOffset(captured);
        return true;
    }
}
