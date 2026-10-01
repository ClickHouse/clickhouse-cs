using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Prices the three readings of a <c>String</c> column, which is the column type where the choice
/// costs the most.
/// </summary>
/// <remarks>
/// <para>
/// A decoded <c>String</c> column holds every row's bytes in one blob with per-row offsets, and
/// <see cref="IStringColumn"/> reads them in place. The typed surface decodes each row as UTF-8
/// instead, which allocates one string per row and is what almost every caller wants — the point of
/// this table is what that convenience costs, not to argue against it.
/// </para>
/// <para>
/// The arms measure byte length against <c>string.Length</c>. The values are ASCII, so the two counts
/// agree; on non-ASCII data they would not, and only the byte reading would be lossless.
/// </para>
/// </remarks>
[BenchmarkCategory(BenchmarkCategories.TcpRegression)]
[Config(typeof(ComparisonConfig))]
[MemoryDiagnoser(true)]
public class TcpStringRead
{
    private ClickHouseTcpClient client;

    [Params(500_000)]
    public int Count { get; set; }

    private string Sql =>
        $"SELECT concat('city', toString(number % 100000)) AS v FROM system.numbers LIMIT {Count}";

    [GlobalSetup]
    public void Setup() => client = BenchmarkServer.CreateTcpClient();

    [GlobalCleanup]
    public void Cleanup() => client?.Dispose();

    /// <summary>The offsets alone: a length per row with nothing read and nothing decoded.</summary>
    [Benchmark(Baseline = BenchmarkModes.MethodBaseline)]
    public async Task<long> BorrowedOffsets()
    {
        long total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            var offsets = ((IStringColumn)block[0]).Offsets;
            for (int row = 0; row < block.RowCount; row++)
            {
                total += offsets[row + 1] - offsets[row];
            }
        }

        return total;
    }

    /// <summary>A borrowed byte slice per row, which is the undecoded reading of the same data.</summary>
    [Benchmark]
    public async Task<long> BorrowedBytes()
    {
        long total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            var column = (IStringColumn)block[0];
            for (int row = 0; row < block.RowCount; row++)
            {
                total += column.GetBytes(row).Length;
            }
        }

        return total;
    }

    /// <summary>The typed surface: one UTF-8 decode and one string per row.</summary>
    [Benchmark]
    public async Task<long> DecodedStrings()
    {
        long total = 0;
        await foreach (Block block in client.StreamAsync(Sql))
        {
            foreach (string value in block.Column<string>(0).Values)
            {
                total += value.Length;
            }
        }

        return total;
    }
}
