using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal abstract class ClickHouseType
{
    public abstract Type FrameworkType { get; }

    public abstract object Read(ExtendedBinaryReader reader);

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
