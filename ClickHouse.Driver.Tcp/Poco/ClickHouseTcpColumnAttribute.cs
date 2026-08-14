using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Maps a property to a differently-named ClickHouse column on the native-TCP client's POCO paths.
///
/// <para>
/// Without this attribute a property is matched against the column of the same name, then case-insensitively,
/// then ignoring underscores — so <c>UserId</c> already finds a <c>user_id</c> column. Use the attribute only when
/// the names differ by more than that.
/// </para>
///
/// <para>
/// This is deliberately not <c>ClickHouse.Driver.ClickHouseColumnAttribute</c>: the two clients are separate
/// assemblies, and the native-TCP client cannot see the other one's types. The HTTP attribute also carries an
/// explicit ClickHouse <c>Type</c>, which exists only to skip that client's schema-probe query; the native protocol
/// sends the target types with the insert itself, so there is nothing here for such a property to do.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ClickHouseTcpColumnAttribute : Attribute
{
    /// <summary>
    /// The ClickHouse column name this property maps to. Leave unset to match on the property name.
    /// </summary>
    public string Name { get; set; }
}
