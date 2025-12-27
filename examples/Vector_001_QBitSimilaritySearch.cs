using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using QBit vectors for similarity search in ClickHouse.
/// QBit is a quantized vector type that provides efficient storage with configurable precision.
/// This example shows semantic similarity search with different precision levels using L2DistanceTransposed.
/// </summary>
public static class QBitSimilaritySearch
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("=== QBit Similarity Search with Different Precision Levels ===\n");

        var tableName = "example_qbit_similarity";

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName}
            (
                word String,
                vec QBit(Float32, 5)
            )
            ENGINE = MergeTree
            ORDER BY word
        ");

        // Insert sample word embeddings (simplified 5-dimensional vectors)
        // In practice, these would come from an embedding model
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} VALUES
            ('apple',  [0.9, 0.1, 0.8, 0.2, 0.7]),
            ('banana', [0.85, 0.15, 0.75, 0.25, 0.65]),
            ('orange', [0.88, 0.12, 0.78, 0.22, 0.68]),
            ('dog',    [0.1, 0.9, 0.2, 0.8, 0.3]),
            ('horse',  [0.15, 0.85, 0.25, 0.75, 0.35]),
            ('cat',    [0.12, 0.88, 0.22, 0.78, 0.32])
        ");

        Console.WriteLine("Inserted 6 words with 5-dimensional QBit(Float32, 5) embeddings\n");

        // Query vector: looking for words similar to "apple"
        var queryVector = "[0.9, 0.1, 0.8, 0.2, 0.7]";

        // Example 1: High precision search (32 bits per component)
        Console.WriteLine("=== High Precision Search (32 bits) ===");
        Console.WriteLine("Using L2DistanceTransposed with precision=32\n");

        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT
                word,
                L2DistanceTransposed(vec, {queryVector}, 32) AS distance
            FROM {tableName}
            ORDER BY distance
        "))
        {
            Console.WriteLine("Word\t\tDistance");
            Console.WriteLine("----\t\t--------");

            while (reader.Read())
            {
                var word = reader.GetString(0);
                var distance = reader.GetFloat(1);
                Console.WriteLine($"{word,-12}\t{distance:F6}");
            }
        }

        // Example 2: Low precision search (12 bits per component) - faster but less accurate
        Console.WriteLine("\n=== Low Precision Search (12 bits) ===");
        Console.WriteLine("Using L2DistanceTransposed with precision=12\n");

        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT
                word,
                L2DistanceTransposed(vec, {queryVector}, 12) AS distance
            FROM {tableName}
            ORDER BY distance
        "))
        {
            Console.WriteLine("Word\t\tDistance");
            Console.WriteLine("----\t\t--------");

            while (reader.Read())
            {
                var word = reader.GetString(0);
                var distance = reader.GetFloat(1);
                Console.WriteLine($"{word,-12}\t{distance:F6}");
            }
        }

        // Read vector data back as float[]
        Console.WriteLine("\n=== Reading QBit Data ===\n");
        using (var reader = await connection.ExecuteReaderAsync($"SELECT word, vec FROM {tableName} LIMIT 3"))
        {
            while (reader.Read())
            {
                var word = reader.GetString(0);
                var vec = (float[])reader.GetValue(1);
                Console.WriteLine($"{word}: [{string.Join(", ", vec.Select(v => v.ToString("F2")))}]");
            }
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nCleaned up table '{tableName}'");
    }
}
