using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The POCO read plan in a process with dynamic code off: the test assembly in a child process whose runtime
/// configuration sets <c>System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeSupported</c> to false (what the
/// <c>DynamicCodeSupport</c> MSBuild property sets). There <see cref="RuntimeFeature.IsDynamicCodeCompiled"/> is false,
/// so the plan chooses <see cref="PocoScatterTier.Fill"/> by itself, <c>Expression.Compile</c> interprets the compiled
/// constructor of the rows, and Reflection.Emit is not available. The child gives the rows of the compiled loop of this
/// process.
/// </summary>
[TestFixture]
public class PocoReadNoDynamicCodeTests
{
    /// <summary>The argument that makes the entry point of the test assembly run <see cref="Read"/>.</summary>
    internal const string Argument = "--read-poco-rows";

    private const string DynamicCodeSwitch = "System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeSupported";

    // The columns of the rows: dictionaries, nullable values, arrays, a calendar conversion, the read rules of POCO
    // mapping (an enum ordinal, a cast to object), a map, a tuple and a type that reads only as its canonical type.
    private static readonly (string Name, string Type)[] Columns =
    {
        ("Lc", "LowCardinality(String)"),
        ("LcBytes", "LowCardinality(String)"),
        ("LcNullable", "LowCardinality(Nullable(String))"),
        ("Number", "Nullable(Int32)"),
        ("Items", "Array(String)"),
        ("Counts", "Array(Nullable(Int32))"),
        ("Seen", "Nullable(DateTime('UTC'))"),
        ("Level", "Enum8('a' = 1, 'b' = 2)"),
        ("Boxed", "Int32"),
        ("Pairs", "Map(String, Int32)"),
        ("Pair", "Tuple(Int32, String)"),
        ("Variant", "Variant(String, UInt64)"),
    };

    internal enum Level : sbyte
    {
        A = 1,
        B = 2,
    }

    [Test]
    public async Task Materialize_ProcessWithoutDynamicCode_ChoosesTheFillTierAndReadsTheRowsOfTheCompiledLoop()
    {
        var here = new StringWriter();
        Read(here);
        string[] expected = Lines(here.ToString());

        string[] child = await ReadInAProcessWithoutDynamicCodeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(expected[0], Is.EqualTo("dynamic code compiled: True, tier: Emit"), "this process");
            Assert.That(child[0], Is.EqualTo("dynamic code compiled: False, tier: Fill"), "the child process");
            Assert.That(child.Skip(1), Is.EqualTo(expected.Skip(1)), "the rows");
            Assert.That(expected.Length, Is.EqualTo(1 + SampleColumns.RowCount + 1), "the rows and the NULL failure");
        });
    }

    /// <summary>
    /// Reads the sample rows through the plan that the runtime chooses, in windows of two rows, and writes one line for
    /// each row, then the NULL failure of a nullable column read as a value type.
    /// </summary>
    /// <param name="output">Receives the lines.</param>
    /// <returns>The exit code of the child process.</returns>
    internal static int Read(TextWriter output)
    {
        output.WriteLine($"dynamic code compiled: {RuntimeFeature.IsDynamicCodeCompiled}, tier: {PocoColumnScatterFactory.SelectTier(null)}");
        var registry = new PocoTypeRegistry();

        using Block rows = BlockOf(Columns);
        foreach (SampleRow row in Materialize<SampleRow>(registry, rows))
        {
            output.WriteLine(string.Join(" | ", typeof(SampleRow).GetProperties().Select(property => $"{property.Name}={Describe(property.GetValue(row))}")));
        }

        using Block nulls = BlockOf(new[] { ("Number", "Nullable(Int32)") });
        try
        {
            Materialize<NumberRow>(registry, nulls);
            output.WriteLine("no failure");
        }
        catch (InvalidOperationException e)
        {
            output.WriteLine($"{e.GetType().Name}: {e.Message} (inner exception: {e.InnerException?.GetType().Name ?? "none"})");
        }

        return 0;
    }

    // Reads all the rows in windows of two rows, as rows 1000 and on of a result.
    private static List<T> Materialize<T>(PocoTypeRegistry registry, Block block)
        where T : class
    {
        PocoReadPlan<T> plan = registry.ReadPlanFor<T>(block, forcedTier: null);
        var all = new List<T>();
        var window = new T[2];
        for (int start = 0; start < block.RowCount; start += window.Length)
        {
            int count = Math.Min(window.Length, block.RowCount - start);
            plan.Materialize(block, window, start, count, rowOffset: 1_000 + start);
            all.AddRange(window.Take(count));
        }

        return all;
    }

    private static Block BlockOf((string Name, string Type)[] columns)
    {
        IColumn[] decoded = columns.Select(column => PocoReadPlanTests.Decoded(SampleColumns.Build(column.Name, column.Type))).ToArray();
        return new Block(string.Empty, BlockInfo.Default, SampleColumns.RowCount, decoded, ColumnCodecRegistry.Default, new ResolveContext { ServerTimezone = "UTC" });
    }

    private static string Describe(object value) => value switch
    {
        null => "null",
        string text => $"\"{text}\"",
        byte[] bytes => Convert.ToHexString(bytes),
        DateTimeOffset instant => instant.ToString("o", CultureInfo.InvariantCulture),
        IEnumerable items => $"[{string.Join(", ", items.Cast<object>().Select(Describe))}]",
        IFormattable formattable => $"{formattable.ToString(null, CultureInfo.InvariantCulture)} ({value.GetType().Name})",
        _ => $"{value} ({value.GetType().Name})",
    };

    private static string[] Lines(string text) => text.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);

    // Runs the entry point of this assembly with the runtime configuration of the tests and dynamic code off. The output
    // streams are read while the process runs, and a process that does not end in 2 minutes is killed with its children.
    private static async Task<string[]> ReadInAProcessWithoutDynamicCodeAsync()
    {
        string assembly = typeof(PocoReadNoDynamicCodeTests).Assembly.Location;
        string directory = Path.GetDirectoryName(assembly);
        string name = Path.GetFileNameWithoutExtension(assembly);
        JsonNode configuration = JsonNode.Parse(File.ReadAllText(Path.Combine(directory, name + ".runtimeconfig.json")));
        JsonObject options = configuration["runtimeOptions"].AsObject();
        if (options["configProperties"] is not JsonObject properties)
        {
            properties = new JsonObject();
            options["configProperties"] = properties;
        }

        properties[DynamicCodeSwitch] = false;
        string runtimeConfig = Path.Combine(Path.GetTempPath(), $"{name}.{Guid.NewGuid():N}.runtimeconfig.json");
        File.WriteAllText(runtimeConfig, configuration.ToJsonString());
        try
        {
            var start = new ProcessStartInfo(DotnetHost())
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            foreach (string argument in new[] { "exec", "--runtimeconfig", runtimeConfig, "--depsfile", Path.Combine(directory, name + ".deps.json"), assembly, Argument })
            {
                start.ArgumentList.Add(argument);
            }

            using Process process = Process.Start(start);
            Task<string> output = process.StandardOutput.ReadToEndAsync();
            Task<string> error = process.StandardError.ReadToEndAsync();
            bool ended = true;
            using (var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2)))
            {
                try
                {
                    await process.WaitForExitAsync(timeout.Token);
                }
                catch (OperationCanceledException)
                {
                    ended = false;
                    process.Kill(entireProcessTree: true);
                    await process.WaitForExitAsync();
                }
            }

            string text = await output;
            string errors = await error;
            Assert.That(ended, Is.True, $"the child process did not end in 2 minutes: {errors}");
            Assert.That(process.ExitCode, Is.Zero, $"the child process failed: {errors}");
            return Lines(text);
        }
        finally
        {
            File.Delete(runtimeConfig);
        }
    }

    // The dotnet host that runs this process: the SDK names it for its child processes; else the host of the shared
    // runtime that this process uses.
    private static string DotnetHost()
    {
        string host = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
        if (!string.IsNullOrEmpty(host) && File.Exists(host))
        {
            return host;
        }

        string executable = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "dotnet.exe" : "dotnet";
        if (Path.GetFileName(Environment.ProcessPath) == executable)
        {
            return Environment.ProcessPath;
        }

        // <root>/shared/Microsoft.NETCore.App/<version>/
        string runtime = RuntimeEnvironment.GetRuntimeDirectory();
        return Path.GetFullPath(Path.Combine(runtime, "..", "..", "..", executable));
    }

    /// <summary>A row of every column of the sample block.</summary>
    internal sealed class SampleRow
    {
        public string Lc { get; set; }

        public byte[] LcBytes { get; set; }

        public string LcNullable { get; set; }

        public int? Number { get; set; }

        public string[] Items { get; set; }

        public int?[] Counts { get; set; }

        public DateTimeOffset? Seen { get; set; }

        public Level Level { get; set; }

        public object Boxed { get; set; }

        public KeyValuePair<string, int>[] Pairs { get; set; }

        public (int, string) Pair { get; set; }

        public object Variant { get; set; }
    }

    /// <summary>A nullable column read as a value type.</summary>
    internal sealed class NumberRow
    {
        public int Number { get; set; }
    }
}
