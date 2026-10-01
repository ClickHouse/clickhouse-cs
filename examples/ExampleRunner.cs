using System.Reflection;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Discovers and runs examples using reflection.
/// Supports fuzzy matching by class name.
/// </summary>
public static class ExampleRunner
{
    // These sets must be initialized before _examples because discovery reads them.
    // Preflight both endpoints for examples that use both clients.
    private static readonly HashSet<string> _crossTransport = new(StringComparer.Ordinal)
    {
        "TcpMigratingFromHttp",
    };

    // Self-contained examples do not need the configured server.
    private static readonly HashSet<string> _selfContained = new(StringComparer.Ordinal)
    {
        "Testcontainers",
        "TcpTestcontainers",
    };

    // These examples configure their own endpoint instead of using ExampleConfig.
    private static readonly HashSet<string> _customEndpoint = new(StringComparer.Ordinal)
    {
        "ConnectionStringConfiguration",
        "CreateTableCloud",
        "DependencyInjection",
        "JwtAuthentication",
        "TcpTls",
    };

    // Run infrastructure-dependent examples only when a filter selects them explicitly.
    private static readonly HashSet<string> _optIn = new(StringComparer.Ordinal)
    {
        "CreateTableCluster",
        "CreateTableCloud",
        "JwtAuthentication",
    };

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

        /// <summary>The transport indicated by the example's class-name prefix.</summary>
        public ExampleTransport Transport { get; } = GetTransport(ClassName);

        /// <summary>The endpoints that preflight must check.</summary>
        public IReadOnlyList<ExampleTransport> RequiredTransports { get; } = GetRequiredTransports(ClassName);

        /// <summary>Whether an unfiltered run includes this example.</summary>
        public bool RunsByDefault { get; } = !_optIn.Contains(ClassName);
    }

    /// <summary>
    /// Gets all discovered examples.
    /// </summary>
    public static IReadOnlyList<ExampleInfo> Examples => _examples;

    /// <summary>
    /// Gets the examples that use one transport and that a run naming no example includes.
    /// </summary>
    /// <param name="transport">The transport to select.</param>
    /// <returns>The matching examples, in class-name order.</returns>
    public static List<ExampleInfo> ForTransport(ExampleTransport transport)
        => _examples.Where(e => e.Transport == transport && e.RunsByDefault).ToList();

    /// <summary>
    /// Finds examples matching the given filter using fuzzy matching.
    /// Matches against any substring of the normalized class name.
    /// </summary>
    /// <param name="filter">The pattern to match.</param>
    /// <param name="transport">The transport to restrict the match to, or null for either.</param>
    /// <returns>The matching examples, in class-name order.</returns>
    public static List<ExampleInfo> FindMatches(string filter, ExampleTransport? transport = null)
    {
        var normalizedFilter = Normalize(filter);
        return _examples
            .Where(e => e.NormalizedName.Contains(normalizedFilter))
            .Where(e => transport is null || e.Transport == transport)
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
    public static void ListExamples(ExampleTransport? transport = null)
    {
        // Keep opt-in examples discoverable even though an unfiltered run skips them.
        var listed = _examples.Where(e => transport is null || e.Transport == transport).ToList();

        Console.WriteLine(transport is { } named ? $"Available {named} examples:\n" : "Available examples:\n");

        foreach (var example in listed.OrderBy(e => e.ClassName))
        {
            Console.WriteLine(example.RunsByDefault
                ? $"  - {example.ClassName}"
                : $"  - {example.ClassName}   (--filter only: needs a cluster, Cloud, or a token)");
        }

        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run                         Run all examples");
        Console.WriteLine("  dotnet run -- --http               Run only the HTTP examples");
        Console.WriteLine("  dotnet run -- --tcp                Run only the native protocol examples");
        Console.WriteLine("  dotnet run -- --list               List available examples");
        Console.WriteLine("  dotnet run -- --list --tcp         List one transport's examples");
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

    private static ExampleTransport GetTransport(string className)
        => className.StartsWith("Tcp", StringComparison.Ordinal)
            ? ExampleTransport.Tcp
            : ExampleTransport.Http;

    private static IReadOnlyList<ExampleTransport> GetRequiredTransports(string className)
    {
        if (_selfContained.Contains(className) || _customEndpoint.Contains(className))
        {
            return [];
        }

        if (_crossTransport.Contains(className))
        {
            return [ExampleTransport.Http, ExampleTransport.Tcp];
        }

        return [GetTransport(className)];
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
