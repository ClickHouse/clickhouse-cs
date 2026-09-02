namespace ClickHouse.Driver.Tcp;

/// <summary>
/// One value bound to a <c>{name:Type}</c> placeholder in a parameterized query.
/// </summary>
/// <param name="Name">The parameter name, without braces or a type. Must not be null or empty.</param>
/// <param name="Value">The value. Null and <see cref="System.DBNull"/> both send the null marker.</param>
/// <param name="ClickHouseType">
/// The type used to format the value, or null to use the query's <c>{Name:Type}</c> placeholder. Must not be
/// empty.
/// </param>
/// <remarks>
/// This override affects formatting only; the server still uses the type declared in the query. A mismatch may
/// fail to parse or change the value.
/// </remarks>
public sealed record ClickHouseTcpParameter(string Name, object Value, string ClickHouseType = null);
