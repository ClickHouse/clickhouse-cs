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
    /// Identifies everything about this type instance that steers <see cref="Write"/>, for callers that
    /// cache per-type state (the POCO insert writer cache). Defaults to <see cref="ToString()"/>, which
    /// spells out the full type declaration for almost every type.
    /// Override when <c>ToString()</c> omits state <see cref="Write"/> depends on (<see cref="JsonType"/>
    /// and its path hints), and in a type that composes other types, so a nested lossy signature — not the
    /// child's <c>ToString()</c> — is what surfaces in the parent's signature.
    /// </summary>
    internal virtual string CacheSignature => ToString();

    /// <summary>
    /// Signature of a type which composes other types. Keeps <see cref="ToString()"/> while every child
    /// renders itself fully, so an alias type (Point, Ring, Geometry) keeps the distinct name its
    /// <c>ToString()</c> spells instead of collapsing onto the structural form of its type name.
    /// Only a child which hides state its own <c>ToString()</c> omits (a hinted <see cref="JsonType"/>)
    /// makes the composed form take over.
    /// </summary>
    /// <param name="compose">Builds the signature out of the children's signatures, in order</param>
    /// <param name="children">Types this one composes</param>
    protected string ComposeCacheSignature(Func<string[], string> compose, params ClickHouseType[] children)
    {
        var signatures = new string[children.Length];
        var hidesState = false;
        for (var i = 0; i < children.Length; i++)
        {
            signatures[i] = children[i].CacheSignature;
            hidesState |= signatures[i] != children[i].ToString();
        }

        return hidesState ? compose(signatures) : ToString();
    }

    /// <summary>
    /// Returns whether this type can write the given value.
    /// The default checks exact type equality against <see cref="FrameworkType"/>.
    /// Override for types that share a FrameworkType but need value-based disambiguation (e.g. IPv4 vs IPv6).
    /// </summary>
    public virtual bool CanWrite(object value) => FrameworkType == value?.GetType();

    protected static object ClearDBNull(object value) => value is DBNull ? null : value;
}
