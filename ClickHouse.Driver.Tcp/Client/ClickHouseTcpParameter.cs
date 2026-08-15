namespace ClickHouse.Driver.Tcp;

/// <summary>
/// One value bound to a <c>{name:Type}</c> placeholder in a parameterized query.
/// </summary>
/// <param name="Name">The parameter name, without braces or a type. Must not be null or empty.</param>
/// <param name="Value">The value. Null and <see cref="System.DBNull"/> both send the null marker.</param>
/// <param name="ClickHouseType">
/// The ClickHouse type to format the value as, or null to take the type from the query's <c>{Name:Type}</c>
/// placeholder. Set this only when the placeholder does not give the type the value should be formatted as —
/// most queries need no override, because the placeholder the server reads is the same one the client reads.
/// </param>
/// <remarks>
/// The type does not travel on the wire. The server takes it from the query text, and the client uses it only
/// to choose how to write the value, so an override that disagrees with the placeholder makes the server parse
/// text it did not expect.
/// </remarks>
public sealed record ClickHouseTcpParameter(string Name, object Value, string ClickHouseType = null);
