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
