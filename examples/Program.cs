namespace ClickHouse.Driver.Examples;

class Program
{
    static async Task Main(string[] args)
    {
        // Check if running in CI or non-interactive mode
        bool isInteractive = Environment.UserInteractive && !Console.IsInputRedirected;

        Console.WriteLine("ClickHouse C# Driver Examples");
        Console.WriteLine("==============================\n");

        try
        {
            var filter = ParseArgs(args, out bool showList, out ExampleTransport? transport);

            if (showList)
            {
                ExampleRunner.ListExamples(transport);
                return;
            }

            if (filter != null)
            {
                await RunFiltered(filter, isInteractive);
            }
            else if (transport is { } only)
            {
                await RunTransport(only, isInteractive);
            }
            else
            {
                if (!await ExamplePreflight.CheckAsync(ExampleRunner.Examples))
                {
                    Environment.Exit(1);
                }

                await RunAllExamples(isInteractive);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n\nERROR: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            Environment.Exit(1);
        }
    }

    private static async Task RunFiltered(string filter, bool isInteractive)
    {
        var matches = ExampleRunner.FindMatches(filter);

        if (matches.Count == 0)
        {
            ExampleRunner.PrintNoMatchError(filter);
            Environment.Exit(1);

        }

        Console.WriteLine($"Found {matches.Count} matching example(s):\n");

        if (!await ExamplePreflight.CheckAsync(matches))
        {
            Environment.Exit(1);
        }

        foreach (var example in matches)
        {
            await ExampleRunner.RunExample(example);
            WaitForUser(isInteractive);
            Console.WriteLine("\n");
        }
    }

    private static async Task RunTransport(ExampleTransport transport, bool isInteractive)
    {
        var selected = ExampleRunner.ForTransport(transport);

        Console.WriteLine($"Running {selected.Count} {transport} example(s):\n");

        // The named transport plus whatever the selection needs beyond it, so that asking for one
        // with none written yet still reports whether its endpoint answers, and an example
        // comparing the two transports still gets both checked.
        var needed = selected.SelectMany(e => e.RequiredTransports).Append(transport).Distinct().ToArray();

        if (!await ExamplePreflight.CheckAsync(needed))
        {
            Environment.Exit(1);
        }

        foreach (var example in selected)
        {
            await ExampleRunner.RunExample(example);
            WaitForUser(isInteractive);
            Console.WriteLine("\n");
        }
    }

    private static async Task RunAllExamples(bool isInteractive)
    {
        // Core Usage & Configuration
        Console.WriteLine("\n" + new string('=', 70));
        Console.WriteLine("CORE USAGE & CONFIGURATION");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(BasicUsage)}");
        await BasicUsage.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ConnectionStringConfiguration)}");
        await ConnectionStringConfiguration.Run();
        WaitForUser(isInteractive);

#if NET7_0_OR_GREATER
        Console.WriteLine($"\n\nRunning: {nameof(DependencyInjection)}");
        await DependencyInjection.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(AspNetHealthChecks)}");
        await AspNetHealthChecks.Run();
        WaitForUser(isInteractive);
#endif

        Console.WriteLine($"\n\nRunning: {nameof(HttpClientConfiguration)}");
        await HttpClientConfiguration.Run();
        WaitForUser(isInteractive);

        // Creating Tables
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("CREATING TABLES");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(CreateTableSingleNode)}");
        await CreateTableSingleNode.Run();
        WaitForUser(isInteractive);

        // Inserting Data
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("INSERTING DATA");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(SimpleDataInsert)}");
        await SimpleDataInsert.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(BulkInsert)}");
        await BulkInsert.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(AsyncInsert)}");
        await AsyncInsert.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(RawStreamInsert)}");
        await RawStreamInsert.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(InsertFromSelect)}");
        await InsertFromSelect.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(EphemeralColumns)}");
        await EphemeralColumns.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(UpsertsWithReplacingMergeTree)}");
        await UpsertsWithReplacingMergeTree.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(SchemaOptimization)}");
        await SchemaOptimization.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(PocoInsert)}");
        await PocoInsert.Run();
        WaitForUser(isInteractive);

        // Selecting Data
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("SELECTING DATA");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(BasicSelect)}");
        await BasicSelect.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(SelectMetadata)}");
        await SelectMetadata.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(SelectWithParameterBinding)}");
        await SelectWithParameterBinding.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ExportToFile)}");
        await ExportToFile.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(CompressedRawExport)}");
        await CompressedRawExport.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(PocoSelect)}");
        await PocoSelect.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ResponseCompression)}");
        await ResponseCompression.Run();
        WaitForUser(isInteractive);

        // Data Types
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("DATA TYPES");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(SimpleTypes)}");
        await SimpleTypes.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(DateTimeHandling)}");
        await DateTimeHandling.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ComplexTypes)}");
        await ComplexTypes.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(StringHandling)}");
        await StringHandling.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(JsonType)}");
        await JsonType.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(GeometryTypes)}");
        await GeometryTypes.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(QBitSimilaritySearch)}");
        await QBitSimilaritySearch.Run();
        WaitForUser(isInteractive);

        // ORM Integration
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("ORM INTEGRATION");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(DapperExample)}");
        await DapperExample.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(Linq2DbExample)}");
        await Linq2DbExample.Run();
        WaitForUser(isInteractive);

        // Advanced
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("ADVANCED FEATURES");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(QueryIdUsage)}");
        await QueryIdUsage.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(SessionIdUsage)}");
        await SessionIdUsage.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(LongRunningQueries)}");
        await LongRunningQueries.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(CustomSettings)}");
        await CustomSettings.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(QueryStatistics)}");
        await QueryStatistics.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(Roles)}");
        await Roles.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(CustomHeaders)}");
        await CustomHeaders.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(QueryCancellation)}");
        await QueryCancellation.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ReadOnlyUsers)}");
        await ReadOnlyUsers.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(RetriesAndDeduplication)}");
        await RetriesAndDeduplication.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(Compression)}");
        await Compression.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ParameterTypeResolver)}");
        await ParameterTypeResolver.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ParameterFormatter)}");
        await ParameterFormatter.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(ReadValueConverter)}");
        await ReadValueConverter.Run();
        WaitForUser(isInteractive);

        // Troubleshooting
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("TROUBLESHOOTING");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(LoggingConfiguration)}");
        await LoggingConfiguration.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(NetworkTracing)}");
        await NetworkTracing.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(OpenTelemetryTracing)}");
        await OpenTelemetryTracing.Run();
        WaitForUser(isInteractive);

        // Testing
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("TESTING");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(Testcontainers)}");
        await Testcontainers.Run();
        WaitForUser(isInteractive);

        // Native Protocol: Core Usage & Configuration
        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("NATIVE PROTOCOL: CORE USAGE & CONFIGURATION");
        Console.WriteLine(new string('=', 70) + "\n");

        Console.WriteLine($"Running: {nameof(TcpBasicUsage)}");
        await TcpBasicUsage.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(TcpConnectionString)}");
        await TcpConnectionString.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(TcpDependencyInjection)}");
        await TcpDependencyInjection.Run();
        WaitForUser(isInteractive);

        Console.WriteLine($"\n\nRunning: {nameof(TcpMigratingFromHttp)}");
        await TcpMigratingFromHttp.Run();
        WaitForUser(isInteractive);

        Console.WriteLine("\n\n" + new string('=', 70));
        Console.WriteLine("ALL EXAMPLES COMPLETED SUCCESSFULLY!");
        Console.WriteLine(new string('=', 70));
    }

    private static string? ParseArgs(string[] args, out bool showList, out ExampleTransport? transport)
    {
        showList = false;
        transport = null;
        string? filter = null;

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (arg == "--list" || arg == "-l")
            {
                showList = true;
                continue;
            }

            if (arg == "--http")
            {
                transport = ExampleTransport.Http;
                continue;
            }

            if (arg == "--tcp")
            {
                transport = ExampleTransport.Tcp;
                continue;
            }

            if (arg == "--filter" || arg == "-f")
            {
                if (i + 1 < args.Length)
                {
                    filter = args[i + 1];
                    i++;
                }
                else
                {
                    Console.WriteLine("Error: --filter requires a value");
                    Environment.Exit(1);
                }
            }
            else if (!arg.StartsWith("-"))
            {
                // Positional argument = filter shorthand
                filter ??= arg;
            }
        }

        return filter;
    }

    private static void WaitForUser(bool isInteractive)
    {
        if (isInteractive)
        {
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }
        else
        {
            Console.WriteLine(); // Just add a blank line in non-interactive mode
        }
    }
}
