using System.Reflection;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Discovers and runs examples using reflection.
/// Supports fuzzy matching by class name.
/// </summary>
public static class ExampleRunner
{
    private static readonly List<ExampleInfo> _examples = DiscoverExamples();

    /// <summary>
    /// Information about a discovered example.
    /// </summary>
    public record ExampleInfo(string ClassName, Type Type, MethodInfo RunMethod)
    {
        /// <summary>
        /// Normalized form for matching (lowercase, no underscores).
        /// </summary>
        public string NormalizedName { get; } = Normalize(ClassName);
    }

    /// <summary>
    /// Gets all discovered examples.
    /// </summary>
    public static IReadOnlyList<ExampleInfo> Examples => _examples;

    /// <summary>
    /// Finds examples matching the given filter using fuzzy matching.
    /// Matches against any substring of the normalized class name.
    /// </summary>
    public static List<ExampleInfo> FindMatches(string filter)
    {
        var normalizedFilter = Normalize(filter);
        return _examples
            .Where(e => e.NormalizedName.Contains(normalizedFilter))
            .ToList();
    }

    /// <summary>
    /// Runs a single example.
    /// </summary>
    public static async Task RunExample(ExampleInfo example)
    {
        Console.WriteLine($"Running: {example.ClassName}");
        await (Task)example.RunMethod.Invoke(null, null)!;
    }

    /// <summary>
    /// Lists all available examples to the console.
    /// </summary>
    public static void ListExamples()
    {
        Console.WriteLine("Available examples:\n");

        foreach (var example in _examples.OrderBy(e => e.ClassName))
        {
            Console.WriteLine($"  - {example.ClassName}");
        }

        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run                         Run all examples");
        Console.WriteLine("  dotnet run -- --list               List available examples");
        Console.WriteLine("  dotnet run -- --filter <pattern>   Run examples matching pattern");
        Console.WriteLine("  dotnet run -- <pattern>            Shorthand for --filter");
        Console.WriteLine();
        Console.WriteLine("Filter examples:");
        Console.WriteLine("  dotnet run -- basicusage           Match by class name");
        Console.WriteLine("  dotnet run -- bulk                 Partial match");
    }

    /// <summary>
    /// Prints suggestions for a filter that didn't match.
    /// </summary>
    public static void PrintNoMatchError(string filter)
    {
        Console.WriteLine($"Error: No examples found matching '{filter}'\n");

        var normalizedFilter = Normalize(filter);
        var suggestions = _examples
            .Select(e => (Example: e, Score: GetSimilarityScore(normalizedFilter, e.NormalizedName)))
            .Where(x => x.Score > 0)
            .OrderByDescending(x => x.Score)
            .Take(3)
            .ToList();

        if (suggestions.Count > 0)
        {
            Console.WriteLine("Did you mean:");
            foreach (var (example, _) in suggestions)
            {
                Console.WriteLine($"  - {example.ClassName}");
            }
            Console.WriteLine();
        }

        Console.WriteLine("Use --list to see all available examples.");
    }

    private static List<ExampleInfo> DiscoverExamples()
    {
        return Assembly.GetExecutingAssembly()
            .GetTypes()
            .Where(t => t.Namespace == "ClickHouse.Driver.Examples"
                     && t.IsClass
                     && t.IsAbstract && t.IsSealed  // static class
                     && t.Name != "Program"
                     && t.Name != "ExampleRunner")
            .Select(t => (Type: t, RunMethod: t.GetMethod("Run", BindingFlags.Public | BindingFlags.Static)))
            .Where(x => x.RunMethod != null && x.RunMethod.ReturnType == typeof(Task))
            .Select(x => new ExampleInfo(x.Type.Name, x.Type, x.RunMethod!))
            .OrderBy(e => e.ClassName)
            .ToList();
    }

    private static string Normalize(string input)
    {
        return input.Replace("_", "").Replace("-", "").ToLowerInvariant();
    }

    private static int GetSimilarityScore(string filter, string target)
    {
        int score = 0;
        int filterIndex = 0;

        foreach (var c in target)
        {
            if (filterIndex < filter.Length && c == filter[filterIndex])
            {
                score++;
                filterIndex++;
            }
        }

        return score;
    }
}
