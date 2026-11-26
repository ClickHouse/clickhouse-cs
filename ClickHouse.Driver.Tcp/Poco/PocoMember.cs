using System;
using System.Reflection;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// One mapped property of a POCO type, resolved once per type by <see cref="PocoTypeDescriptor"/>.
///
/// <para>
/// A member is kept whichever direction it can serve, because the descriptor is shared by both: a query needs a
/// settable property (<see cref="CanSet"/>), an insert needs a gettable one (<see cref="CanGet"/>), and a type may
/// well support only one — an immutable POCO with getter-only properties inserts but cannot be materialized. Each
/// plan filters on the flag it needs, so a type is never rejected for lacking the direction it is not being used
/// in.
/// </para>
/// </summary>
internal sealed class PocoMember
{
    /// <summary>Builds the member metadata for one property.</summary>
    /// <param name="property">The property, already known to be mapped (public, instance, not an indexer).</param>
    /// <param name="columnName">The column name to match on: the attribute's name, or the property's.</param>
    public PocoMember(PropertyInfo property, string columnName)
    {
        Property = property;
        ColumnName = columnName;

        Type memberType = property.PropertyType;
        MemberType = memberType;
        NullableUnderlyingType = Nullable.GetUnderlyingType(memberType);
        CanAssignNull = !memberType.IsValueType || NullableUnderlyingType is not null;

        MethodInfo getMethod = property.GetMethod;
        CanGet = getMethod is not null && getMethod.IsPublic;

        // An init-only setter is a real setter to reflection, and invoking it outside an object initializer works,
        // but doing so would defeat the immutability the author asked for — so treat it as unsettable, the same
        // call the HTTP client's POCO reader makes.
        MethodInfo setMethod = property.SetMethod;
        CanSet = setMethod is not null && setMethod.IsPublic && !IsInitOnly(setMethod);
    }

    /// <summary>The reflected property, used to compile the typed getter/setter access.</summary>
    public PropertyInfo Property { get; }

    /// <summary>The CLR property name, for diagnostics.</summary>
    public string MemberName => Property.Name;

    /// <summary>The CLR property type, e.g. <see cref="long"/> or <c>int?</c>.</summary>
    public Type MemberType { get; }

    /// <summary>The ClickHouse column name this member matches on, before the matcher's case/underscore tiers.</summary>
    public string ColumnName { get; }

    /// <summary>
    /// The underlying type when <see cref="MemberType"/> is a <see cref="Nullable{T}"/>, otherwise null.
    /// </summary>
    public Type NullableUnderlyingType { get; }

    /// <summary>
    /// Whether null can be assigned — true for a reference type or a <see cref="Nullable{T}"/>. Decides whether a
    /// <c>Nullable(T)</c> column can target this member without a null ever having nowhere to go.
    /// </summary>
    public bool CanAssignNull { get; }

    /// <summary>Whether the property has a public getter, so it can be an insert source.</summary>
    public bool CanGet { get; }

    /// <summary>
    /// Whether the property has a public, non-init setter, so a query can materialize into it.
    /// </summary>
    public bool CanSet { get; }

    /// <summary>
    /// Names why <see cref="CanSet"/> is false, for the message a query-side plan builder throws. Never called
    /// when the member is settable.
    /// </summary>
    /// <returns>The reason, as a noun phrase.</returns>
    public string DescribeWhyNotSettable()
    {
        MethodInfo setMethod = Property.SetMethod;
        if (setMethod is null)
        {
            return "it has no setter";
        }

        return IsInitOnly(setMethod) ? "its setter is init-only" : "its setter is not public";
    }

    // C# marks an init-only setter with a required custom modifier on its return parameter; there is no
    // dedicated reflection flag for it.
    private static bool IsInitOnly(MethodInfo setMethod)
        => Array.Exists(
            setMethod.ReturnParameter.GetRequiredCustomModifiers(),
            static modifier => modifier.FullName == "System.Runtime.CompilerServices.IsExternalInit");
}
