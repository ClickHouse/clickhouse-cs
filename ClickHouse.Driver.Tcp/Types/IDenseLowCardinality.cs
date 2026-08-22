namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Marks a <c>LowCardinality</c> column whose dictionary and keys a <em>nullable</em> low-cardinality codec may
/// re-emit verbatim, with no rebuild. Carries no members of its own — the dictionary and keys come from
/// <see cref="ILowCardinalityColumn{T}"/>; this interface adds only the write-eligibility claim.
///
/// <para>
/// Implemented solely by the nullable low-cardinality columns. The non-nullable
/// <see cref="LowCardinalityColumn{T}"/> exposes the same pair for reading but is deliberately <em>not</em> a dense
/// source under a nullable codec, because its dictionary reserves a single default slot rather than the two slots
/// (NULL + default) a <c>LowCardinality(Nullable(T))</c> dictionary requires — re-emitting it verbatim would make
/// the reader read slot 0 back as NULL.
/// </para>
/// </summary>
/// <typeparam name="T">The dictionary's CLR element type (the bare inner type; never made nullable).</typeparam>
internal interface IDenseLowCardinality<T> : ILowCardinalityColumn<T>
{
}
