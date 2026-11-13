using System.Text.Json.Serialization;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Json;

/// <summary>
/// Source-generated serialization metadata for the JSON the driver itself reads, so it needs no reflection.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower,
    NumberHandling = JsonNumberHandling.AllowReadingFromString)]
[JsonSerializable(typeof(QueryStats))]
internal sealed partial class DriverJsonContext : JsonSerializerContext
{
}
