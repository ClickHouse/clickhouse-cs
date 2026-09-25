namespace ClickHouse.Driver.Tcp;

/// <summary>
/// One value bound to a <c>{name:Type}</c> placeholder in a parameterized query.
/// </summary>
/// <param name="Name">The parameter name, without braces or a type. Must not be null or empty.</param>
/// <param name="Value">
/// The value. Null and <see cref="System.DBNull"/> both send the null marker. A sequence must be replayable:
/// picking a <c>Variant</c> alternative reads it once to decide, and formatting reads it again. A one-shot
/// sequence such as a LINQ query or an iterator is empty on the second read and sends <c>[]</c>. Pass an array
/// or a materialized collection.
/// </param>
/// <param name="ClickHouseType">
/// The type used to format the value, or null to use the query's <c>{Name:Type}</c> placeholder. Must not be
/// empty.
/// </param>
/// <remarks>
/// This override affects formatting only; the server still uses the type declared in the query. A mismatch may
/// fail to parse or change the value.
/// </remarks>
public sealed record ClickHouseTcpParameter(string Name, object Value, string ClickHouseType = null);
