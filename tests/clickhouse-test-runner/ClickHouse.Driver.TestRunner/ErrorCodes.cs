namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// Maps the error names of ClickHouse to their codes. The table is in <c>ErrorCodes.g.cs</c>, which
/// <c>scripts/gen-error-codes.sh</c> generates from the upstream checkout.
/// </summary>
internal static partial class ErrorCodes
{
    public const int SyntaxError = 62;

    // Lazy: the initializers of static fields in two files of a partial class run in no defined order.
    private static readonly Lazy<Dictionary<int, string>> Names = new(() => Codes.ToDictionary(kv => kv.Value, kv => kv.Key));

    public static int? CodeOf(string name) => Codes.TryGetValue(name, out var code) ? code : null;

    public static string? NameOf(int code) => Names.Value.TryGetValue(code, out var name) ? name : null;
}
