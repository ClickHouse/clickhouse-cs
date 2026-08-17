using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// The safety net for the two write paths that used to be unified by densify: an <em>ergonomic</em> column (the
/// jagged/nullable/tuple form a caller builds) and the <em>dense</em> column read back from the wire must encode to
/// byte-identical output. Densify guaranteed this structurally by projecting the ergonomic form into the dense one
/// before every write; now the ergonomic form is written directly through lazy views, so this asserts the two paths
/// still agree.
/// </summary>
[TestFixture]
public class WritePathEquivalenceTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    // Every case: an ergonomic column of the given type. The dense counterpart is obtained by round-tripping it
    // through the codec (write + read back), then both are written via WriteFull and the bytes compared.
    private static IEnumerable<TestCaseData> Cases()
    {
        yield return Case("Array(UInt32)", new ArrayColumn<uint[]>("c", "Array(UInt32)", new[] { new uint[] { 1, 2, 3 }, Array.Empty<uint>(), new uint[] { 4 } }));
        yield return Case("Array(String)", new ArrayColumn<string[]>("c", "Array(String)", new[] { new[] { "a", "bb" }, Array.Empty<string>(), new[] { "héllo✓" } }));
        yield return Case("Array(Nullable(Int32))", new ArrayColumn<int?[]>("c", "Array(Nullable(Int32))", new[] { new int?[] { 1, null, 3 }, new int?[] { null } }));
        yield return Case("Array(Array(UInt32))", new ArrayColumn<uint[][]>("c", "Array(Array(UInt32))", new[] { new[] { new uint[] { 1, 2 }, new uint[] { 3 } }, Array.Empty<uint[]>() }));
        yield return Case("Nullable(Int32)", new ArrayColumn<int?>("c", "Nullable(Int32)", new int?[] { 1, null, 3, null }));
        yield return Case("Nullable(String)", new ArrayColumn<string>("c", "Nullable(String)", new[] { "a", null, "c" }));
        yield return Case("Tuple(Int32, String)", new ArrayColumn<(int, string)>("c", "Tuple(Int32, String)", new[] { (1, "a"), (2, "bb"), (3, "ccc") }));
        yield return Case("Map(String, Int32)", new ArrayColumn<KeyValuePair<string, int>[]>("c", "Map(String, Int32)", new[]
        {
            new[] { new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("b", 2) },
            Array.Empty<KeyValuePair<string, int>>(),
        }));

        // Lifted ergonomic columns: the row's elements arrive in a CLR type the child codec converts, so the write goes
        // through a shape resolved from the row's type rather than the codec's own. The dense read-back is always in the
        // canonical type, so byte equality here is what proves a lifted write encodes exactly as the canonical one does.
        var stamps = new[] { DateTimeOffset.FromUnixTimeSeconds(0).UtcDateTime, DateTimeOffset.FromUnixTimeSeconds(1_705_314_600).UtcDateTime };
        yield return Case("Array(DateTime('UTC'))", new ArrayColumn<DateTime[]>("c", "Array(DateTime('UTC'))", new[] { stamps, Array.Empty<DateTime>() }));
        yield return Case("Array(Array(DateTime('UTC')))", new ArrayColumn<DateTime[][]>("c", "Array(Array(DateTime('UTC')))", new[] { new[] { stamps, Array.Empty<DateTime>() }, Array.Empty<DateTime[]>() }));
        yield return Case("Array(Nullable(DateTime('UTC')))", new ArrayColumn<DateTime?[]>("c", "Array(Nullable(DateTime('UTC')))", new[] { new DateTime?[] { stamps[0], null, stamps[1] }, new DateTime?[] { null } }));
        yield return Case("Tuple(DateTime('UTC'), String)", new ArrayColumn<(DateTime, string)>("c", "Tuple(DateTime('UTC'), String)", new[] { (stamps[0], "a"), (stamps[1], "bb") }));
        yield return Case("Map(String, DateTime('UTC'))", new ArrayColumn<KeyValuePair<string, DateTime>[]>("c", "Map(String, DateTime('UTC'))", new[]
        {
            new[] { new KeyValuePair<string, DateTime>("a", stamps[0]), new KeyValuePair<string, DateTime>("b", stamps[1]) },
            Array.Empty<KeyValuePair<string, DateTime>>(),
        }));

        // A lift through two levels, over an inner that has a state prefix of its own: the Array resolves a shape for
        // DateTime, and the LowCardinality it hands the flattened view resolves one too.
        yield return Case("Array(LowCardinality(DateTime('UTC')))", new ArrayColumn<DateTime[]>("c", "Array(LowCardinality(DateTime('UTC')))", new[] { new[] { stamps[0], stamps[1], stamps[0] }, Array.Empty<DateTime>() }));
        yield return Case("LowCardinality(DateTime('UTC'))", new ArrayColumn<DateTime>("c", "LowCardinality(DateTime('UTC'))", new[] { stamps[0], stamps[1], stamps[0] }));
    }

    private static TestCaseData Case(string type, IColumn ergonomic) => new TestCaseData(type, ergonomic).SetName($"WritePathEquivalence({type})");

    [TestCaseSource(nameof(Cases))]
    public async Task WriteFull_ErgonomicAndDenseReadback_ProduceIdenticalBytes(string type, IColumn ergonomic)
    {
        IColumnCodec codec = Resolve(type);

        byte[] ergonomicBytes = await CodecTestHarness.WriteAsync(w => codec.WriteFull(w, ergonomic));

        using IColumn dense = await ReadBackAsync(codec, ergonomicBytes, ergonomic, type);
        byte[] denseBytes = await CodecTestHarness.WriteAsync(w => codec.WriteFull(w, dense));

        Assert.That(denseBytes, Is.EqualTo(ergonomicBytes), $"Ergonomic and dense write paths diverged for {type}.");
    }

    // Reads the column back from its own encoded body, consuming the state prefix first exactly as the block layer
    // does, so composite codecs with a dictionary/type-list prefix (LowCardinality, Dynamic) decode correctly.
    private static async Task<IColumn> ReadBackAsync(IColumnCodec codec, byte[] bytes, IColumn ergonomic, string type)
    {
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        return await codec.ReadColumnAsync(reader, ergonomic.Name, type, ergonomic.RowCount, CodecTestHarness.None);
    }
}
