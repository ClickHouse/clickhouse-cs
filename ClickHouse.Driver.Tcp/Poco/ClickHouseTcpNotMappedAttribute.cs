using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Excludes a property from the native-TCP client's POCO paths, in both directions: no column is matched to it on
/// a query, and it is never read as an insert source.
///
/// <para>
/// This is deliberately not <c>ClickHouse.Driver.ClickHouseNotMappedAttribute</c> — see
/// <see cref="ClickHouseTcpColumnAttribute"/> for why the two clients carry their own attributes.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ClickHouseTcpNotMappedAttribute : Attribute
{
}
