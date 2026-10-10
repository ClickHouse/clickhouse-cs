using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using static ClickHouse.Driver.Tcp.Tests.Poco.PocoWritePlanTests;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The row inserts in a process with dynamic code off: the test assembly in a child process whose runtime configuration
/// sets <c>System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeSupported</c> to false
/// (<see cref="PocoReadNoDynamicCodeTests.RunInAProcessWithoutDynamicCodeAsync"/>). There the POCO write plan chooses
/// <see cref="PocoGatherTier.Delegate"/> by itself, and the converter trees write the gathered columns. The child gives the
/// bytes of the compiled gather of this process.
/// </summary>
[TestFixture]
public class PocoWriteNoDynamicCodeTests
{
    /// <summary>The argument that makes the entry point of the test assembly run <see cref="Write"/>.</summary>
    internal const string Argument = "--write-poco-rows";

    // The columns: a dictionary, a value type into Nullable, an enum ordinal, an array, a calendar conversion with NULLs, a
    // cast to object, a tuple, and a nullable value type into a type with no NULL.
    private static readonly (string Name, string Type)[] Columns =
    {
        ("Lc", "LowCardinality(String)"),
        ("Number", "Nullable(Int32)"),
        ("Level", "Enum8('a' = 1, 'b' = 2)"),
        ("Items", "Array(String)"),
        ("Seen", "Nullable(DateTime('UTC'))"),
        ("Boxed", "Variant(Int32, String)"),
        ("Pair", "Tuple(Int32, String)"),
        ("Count", "Int64"),
    };

    internal enum Level : sbyte
    {
        A = 1,
        B = 2,
    }

    [Test]
    public async Task Insert_ProcessWithoutDynamicCode_ChoosesTheDelegateTierAndWritesTheBytesOfTheCompiledGather()
    {
        var here = new StringWriter();
        Write(here);
        string[] expected = PocoReadNoDynamicCodeTests.Lines(here.ToString());

        string[] child = await PocoReadNoDynamicCodeTests.RunInAProcessWithoutDynamicCodeAsync(Argument);

        Assert.Multiple(() =>
        {
            Assert.That(expected[0], Is.EqualTo("dynamic code compiled: True, tier: Compiled"), "this process");
            Assert.That(child[0], Is.EqualTo("dynamic code compiled: False, tier: Delegate"), "the child process");
            Assert.That(child.Skip(1), Is.EqualTo(expected.Skip(1)), "the bytes and the failure");
            Assert.That(expected.Length, Is.EqualTo(1 + 2 + 1 + 1), "two POCO blocks, one untyped block and the NULL failure");
        });
    }

    /// <summary>
    /// Inserts the sample rows through the POCO write plan that the runtime chooses, in blocks of two rows, and the same
    /// values as untyped rows, and writes one line of hexadecimal bytes for each block, then the failure of a NULL in a
    /// column that cannot hold it.
    /// </summary>
    /// <param name="output">Receives the lines.</param>
    /// <returns>The exit code of the child process.</returns>
    internal static int Write(TextWriter output)
    {
        output.WriteLine($"dynamic code compiled: {RuntimeFeature.IsDynamicCodeCompiled}, tier: {PocoColumnBuilderFactory.SelectTier(null)}");
        var registry = new PocoTypeRegistry();
        Block schema = SchemaOf(Columns.Select(column => Target(column.Name, column.Type)).ToArray());

        SampleRow[] rows =
        {
            new() { Lc = "x", Number = 1, Level = Level.A, Items = new[] { "i" }, Seen = DateTimeOffset.FromUnixTimeSeconds(1), Boxed = "s", Pair = (1, "p"), Count = 10 },
            new() { Lc = "y", Number = 2, Level = Level.B, Items = Array.Empty<string>(), Seen = null, Boxed = "t", Pair = (2, "q"), Count = 20 },
            new() { Lc = "x", Number = 3, Level = Level.A, Items = new[] { "j", "k" }, Seen = DateTimeOffset.FromUnixTimeSeconds(3), Boxed = "u", Pair = (3, "r"), Count = 30 },
            new() { Lc = "z", Number = 4, Level = Level.B, Items = new[] { "l" }, Seen = DateTimeOffset.FromUnixTimeSeconds(4), Boxed = "v", Pair = (4, "s"), Count = null },
        };

        // The last row has a NULL Count, which Int64 cannot hold: the first blocks go out, the last fails at gather.
        using (var buffer = PocoRowBuffer<SampleRow>.Create(rows.Take(3).ToArray(), "rows", blockRows: 2, CancellationToken.None))
        using (PocoInsertSource<SampleRow> source = registry.WritePlanFor<SampleRow>(schema).CreateSource(buffer, blockRows: 2))
        {
            for (int start = 0; start < 3; start += 2)
            {
                source.Gather(start, Math.Min(2, 3 - start));
                output.WriteLine(Convert.ToHexString(Insert(schema, source)));
            }
        }

        object[][] untyped = rows.Take(3)
            .Select(row => new object[] { row.Lc, row.Number, row.Level, row.Items, row.Seen, row.Boxed, row.Pair, row.Count })
            .ToArray();
        using (var buffer = PocoRowBuffer<object[]>.Create(untyped, "rows", untyped.Length, CancellationToken.None))
        using (PocoInsertSource<object[]> source = UntypedRowColumns.CreateSource(schema, buffer, untyped.Length))
        {
            source.Gather(0, untyped.Length);
            output.WriteLine(Convert.ToHexString(Insert(schema, source)));
        }

        try
        {
            using var buffer = PocoRowBuffer<SampleRow>.Create(rows, "rows", rows.Length, CancellationToken.None);
            using PocoInsertSource<SampleRow> source = registry.WritePlanFor<SampleRow>(schema).CreateSource(buffer, rows.Length);
            source.Gather(0, rows.Length);
            output.WriteLine("no failure");
        }
        catch (InvalidOperationException e)
        {
            output.WriteLine($"{e.GetType().Name}: {e.Message}");
        }

        return 0;
    }

    /// <summary>A row of every column of the sample block.</summary>
    internal sealed class SampleRow
    {
        public string Lc { get; set; }

        public int Number { get; set; }

        public Level Level { get; set; }

        public string[] Items { get; set; }

        public DateTimeOffset? Seen { get; set; }

        public string Boxed { get; set; }

        public (int, string) Pair { get; set; }

        public long? Count { get; set; }
    }
}
