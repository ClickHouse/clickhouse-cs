namespace ClickHouse.Driver.Types;

/// <summary>
/// Column types that are pass-through on the RowBinary wire: their
/// <see cref="ClickHouseType.Read(Formats.ExtendedBinaryReader)"/> and
/// <see cref="ClickHouseType.Write(Formats.ExtendedBinaryWriter, object)"/> delegate straight to the
/// wrapped type, and they report the wrapped type's <see cref="ClickHouseType.FrameworkType"/>.
///
/// Any fast path that dispatches on the concrete column type has to look through these, or a wrapped
/// column silently falls back to the boxed path even though it decodes identically to a bare one.
/// </summary>
internal static class TransparentWrapper
{
    /// <summary>
    /// Returns the innermost non-wrapper type, or <paramref name="type"/> itself if it is not a wrapper.
    /// <see cref="NullableType"/> is deliberately <i>not</i> unwrapped: it prefixes a null marker byte and
    /// so is not wire-transparent.
    /// </summary>
    public static ClickHouseType Unwrap(ClickHouseType type)
    {
        while (true)
        {
            var inner = type switch
            {
                LowCardinalityType lowCardinality => lowCardinality.UnderlyingType,
                SimpleAggregateFunctionType simpleAggregate => simpleAggregate.UnderlyingType,
                ObjectType obj => obj.UnderlyingType,
                _ => null,
            };

            if (inner is null)
                return type;

            type = inner;
        }
    }
}
