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

            // Data Types
            Console.WriteLine("\n\n" + new string('=', 70));
            Console.WriteLine("DATA TYPES");
            Console.WriteLine(new string('=', 70) + "\n");

            Console.WriteLine($"Running: {nameof(ComplexTypes)}");
            await ComplexTypes.Run();
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

            Console.WriteLine("\n\n" + new string('=', 70));
            Console.WriteLine("ALL EXAMPLES COMPLETED SUCCESSFULLY!");
            Console.WriteLine(new string('=', 70));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n\nERROR: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            Environment.Exit(1);
        }
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
