using System;
using ClickHouse.Driver.Formats;
using NodaTime;

namespace ClickHouse.Driver.Types;

internal abstract class ClickHouseType
{
    public abstract Type FrameworkType { get; }

    public abstract object Read(ExtendedBinaryReader reader);

    /// <summary>
    /// Reads a value exactly like <see cref="Read(ExtendedBinaryReader)"/>, additionally reporting in
    /// <paramref name="instant"/> the absolute instant it decoded, for types that encode one. The default
    /// reports <see langword="null"/>, meaning the value carries no instant, and callers fall back to
    /// their own handling.
    /// </summary>
    internal virtual object ReadWithInstant(ExtendedBinaryReader reader, out Instant? instant)
    {
        instant = null;
        return Read(reader);
    }

    public abstract void Write(ExtendedBinaryWriter writer, object value);

    public abstract override string ToString();

    /// <summary>
    /// Returns whether this type can write the given value.
    /// The default checks exact type equality against <see cref="FrameworkType"/>.
    /// Override for types that share a FrameworkType but need value-based disambiguation (e.g. IPv4 vs IPv6).
    /// </summary>
    public virtual bool CanWrite(object value) => FrameworkType == value?.GetType();

    protected static object ClearDBNull(object value) => value is DBNull ? null : value;
}
