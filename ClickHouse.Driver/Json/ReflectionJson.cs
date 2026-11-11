using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace ClickHouse.Driver.Json;

/// <summary>
/// Serializes a caller's object to JSON through reflection-based <see cref="JsonSerializer"/>.
/// <para>
/// A trimmed or native AOT app turns reflection-based serialization off unless it opts back in (the SDK sets
/// <c>JsonSerializerIsReflectionEnabledByDefault</c> to false), and <see cref="JsonSerializer"/> then throws
/// <see cref="System.InvalidOperationException"/> instead of serializing. Such an app passes JSON as a
/// <see cref="string"/> or a <see cref="System.Text.Json.Nodes.JsonNode"/>, which bypass this.
/// </para>
/// <para>
/// The suppressions below rest on that default. A trimmed app that opts back in serializes through reflection with
/// no warning from here, and has to keep the serialized types' properties itself.
/// </para>
/// </summary>
internal static class ReflectionJson
{
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection-based serialization is off by default in trimmed apps; see the class summary.")]
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Reflection-based serialization is off by default in native AOT apps; see the class summary.")]
    public static string Serialize(object value) => JsonSerializer.Serialize(value);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection-based serialization is off by default in trimmed apps; see the class summary.")]
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Reflection-based serialization is off by default in native AOT apps; see the class summary.")]
    public static JsonElement SerializeToElement(object value) => JsonSerializer.SerializeToElement(value);
}
