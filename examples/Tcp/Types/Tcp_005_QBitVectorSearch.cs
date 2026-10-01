using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Stores vectors in QBit columns and reads their transposed bit planes.</summary>
public static class TcpQBitVectorSearch
{
    private const string TableName = "example_tcp_qbit";
    private static readonly Version QBitFrom = new(25, 11);

    private static readonly (string Word, float[] Vector)[] Corpus =
    {
        ("apple", new[] { 0.9f, 0.1f, 0.8f, 0.2f, 0.7f }),
        ("banana", new[] { 0.85f, 0.15f, 0.75f, 0.25f, 0.65f }),
        ("dog", new[] { 0.1f, 0.9f, 0.2f, 0.8f, 0.3f }),
    };

    public static async Task Run()
    {
        var builder = ExampleConfig.TcpBuilder();

        // QBit is setting-gated on the earliest server versions that provide it.
        builder["set_allow_experimental_qbit_type"] = 1;

        await using var client = new ClickHouseTcpClient(builder.ToOptions());
        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();

        if (server.Version < QBitFrom)
        {
            Console.WriteLine($"QBit requires ClickHouse {QBitFrom} or newer; found {server.Version}.");
            return;
        }

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");

        try
        {
            await client.ExecuteAsync($"""
                CREATE TABLE {TableName}
                (word String, vector QBit(Float32, 5))
                ENGINE = MergeTree
                ORDER BY word
                """);

            await client.InsertAsync(
                $"INSERT INTO {TableName} (word, vector) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create(
                        "word",
                        Corpus.Select(item => item.Word).ToArray()),
                    ClickHouseTcpColumn.Create(
                        "vector",
                        Corpus.Select(item => item.Vector).ToArray()),
                });

            Console.WriteLine("Nearest vectors:");

            // The last argument keeps 16 high-order bit planes for the approximate distance.
            await foreach (object[] row in client.QueryAsync($"""
                SELECT word,
                       L2DistanceTransposed(vector, [0.9, 0.1, 0.8, 0.2, 0.7], 16) AS distance
                FROM {TableName}
                ORDER BY distance
                """))
            {
                Console.WriteLine($"  {row[0]}: {row[1]}");
            }

            await foreach (Block block in client.StreamAsync(
                $"SELECT vector FROM {TableName} ORDER BY word"))
            {
                var qbit = (IQBitColumn)block["vector"];
                Console.WriteLine(
                    $"QBit layout: dimension={qbit.Dimension}, bit width={qbit.BitWidth}, " +
                    $"bytes per row={qbit.BytesPerRow}");

                // Each plane contains one bit from every vector element, packed for every row.
                ReadOnlySpan<byte> signPlane = qbit.GetPlane(qbit.BitWidth - 1);
                Console.WriteLine($"Sign plane: {Convert.ToHexString(signPlane)}");

                float[] firstVector = block.Column<float[]>("vector")[0];
                Console.WriteLine($"Materialized vector: [{string.Join(", ", firstVector)}]");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }
}
