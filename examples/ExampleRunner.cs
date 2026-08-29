using System.Reflection;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Discovers and runs examples using reflection.
/// Supports fuzzy matching by class name.
/// </summary>
public static class ExampleRunner
{
    /// <summary>
    /// Examples that talk to both interfaces, so their class-name prefix understates what they need.
    /// Declared before <c>_examples</c>: static initializers run in order, and discovery reads this.
    /// </summary>
    private static readonly HashSet<string> _crossTransport = new(StringComparer.Ordinal)
    {
        "TcpMigratingFromHttp",
        "TcpOpenTelemetry",
    };

    /// <summary>
    /// Examples that start their own server, so the configured endpoint is not theirs and preflight must not
    /// hold them up. Declared before <c>_examples</c> for the same reason as <c>_crossTransport</c>.
    /// </summary>
    private static readonly HashSet<string> _selfContained = new(StringComparer.Ordinal)
    {
        "Testcontainers",
        "TcpTestcontainers",
    };

    /// <summary>
    /// Examples needing infrastructure an ordinary server does not have, so neither an unfiltered run
    /// nor a transport run includes them. An explicit <c>--filter</c> still reaches them. Keep this in
    /// step with the list in <c>AGENTS.md</c>.
    /// </summary>
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

        /// <summary>
        /// Which transport the example is filed under. Read from the class name, because every
        /// example shares one namespace and so a native-protocol example cannot reuse an HTTP
        /// example's class name — the <c>Tcp</c> prefix that keeps them apart is the signal.
        /// </summary>
        public ExampleTransport Transport { get; } = ClassName.StartsWith("Tcp", StringComparison.Ordinal)
            ? ExampleTransport.Tcp
            : ExampleTransport.Http;

        /// <summary>
        /// Every endpoint the example needs to reach, which is not always the one it is filed under:
        /// an example comparing the two transports needs both.
        /// </summary>
        public IReadOnlyList<ExampleTransport> RequiredTransports { get; } = _selfContained.Contains(ClassName)
            ? []
            : _crossTransport.Contains(ClassName)
                ? [ExampleTransport.Http, ExampleTransport.Tcp]
                : [ClassName.StartsWith("Tcp", StringComparison.Ordinal) ? ExampleTransport.Tcp : ExampleTransport.Http];

        /// <summary>
        /// Whether a run that names no example includes it. False for one needing a cluster, Cloud
        /// credentials or a token, which only an explicit filter should reach.
        /// </summary>
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
        // Lists the opt-in examples too, marked. They are what a reader is most likely to be looking
        // for by name, since no unfiltered run ever prints them.
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
