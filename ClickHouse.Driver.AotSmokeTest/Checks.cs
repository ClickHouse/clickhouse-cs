using System.Collections;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;

namespace ClickHouse.Driver.AotSmokeTest;

/// <summary>Runs named checks and reports each one; a check fails by throwing.</summary>
internal sealed class Checks
{
    private readonly List<string> failures = [];

    public int Passed { get; private set; }

    public IReadOnlyList<string> Failures => failures;

    public async Task Run(string name, Func<Task> check)
    {
        try
        {
            await check();
            Passed++;
            Console.WriteLine($"PASS  {name}");
        }
        catch (Exception e)
        {
            failures.Add(name);
            Console.WriteLine($"FAIL  {name}");
            Console.WriteLine("      " + e.ToString().Replace("\n", "\n      "));
        }
    }

    public static void Equal(string expected, string actual, string what = "value")
    {
        if (expected != actual)
            throw new InvalidOperationException($"Expected {what} <{expected}>, got <{actual}>");
    }

    public static void True(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }

    public static async Task<TException> Throws<TException>(Func<Task> action)
        where TException : Exception
    {
        try
        {
            await action();
        }
        catch (TException e)
        {
            return e;
        }

        throw new InvalidOperationException($"Expected {typeof(TException).Name}, but nothing was thrown");
    }

    /// <summary>
    /// Renders a value independently of its CLR representation, which differs between the JIT and native AOT for
    /// Map and Tuple: dictionaries and JSON objects by sorted key, tuples and sequences by element.
    /// </summary>
    public static string Show(object value) => value switch
    {
        null or DBNull => "NULL",
        string s => s,
        byte[] bytes => "0x" + Convert.ToHexString(bytes),
        JsonObject json => "{" + string.Join(",", json
            .Select(property => $"{JsonValue.Create(property.Key).ToJsonString()}:{Show(property.Value)}")
            .Order(StringComparer.Ordinal)) + "}",
        JsonArray json => "[" + string.Join(",", json.Select(Show)) + "]",
        JsonNode json => json.ToJsonString(),
        DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture) + $" {dt.Kind}",
        IDictionary dictionary => "{" + string.Join(", ", dictionary.Keys.Cast<object>()
            .Select(key => $"{Show(key)}: {Show(dictionary[key])}")
            .Order(StringComparer.Ordinal)) + "}",
        ITuple tuple => "(" + string.Join(", ", Enumerable.Range(0, tuple.Length).Select(i => Show(tuple[i]))) + ")",
        IEnumerable sequence => "[" + string.Join(", ", sequence.Cast<object>().Select(Show)) + "]",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };
}
