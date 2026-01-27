using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates how to create tables on an on-premises ClickHouse cluster.
/// Shows ReplicatedMergeTree with ON CLUSTER clause and macros.
/// </summary>
public static class CreateTableCluster
{
    public static async Task Run()
    {
        // For cluster operations, connect to any node in the cluster
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("Creating tables on a ClickHouse cluster\n");

        // Example 1: ReplicatedMergeTree with ON CLUSTER
        Console.WriteLine("1. Creating a ReplicatedMergeTree table on cluster:");
        Console.WriteLine("   (This example shows the DDL pattern - actual execution requires a cluster)\n");

        // Sample macro definitions are located in `.docker/clickhouse/cluster/serverN_config.xml`
        var clusterTableDDL = @"
            CREATE TABLE IF NOT EXISTS example_cluster_table
            ON CLUSTER '{cluster}'
            (
                id UInt64,
                name String,
                created_at DateTime DEFAULT now()
            )
            ENGINE = ReplicatedMergeTree(
                '/clickhouse/{cluster}/tables/{database}/{table}/{shard}',
                '{replica}'
            )
            ORDER BY (id)
        ";

        connection.ExecuteStatementAsync(clusterTableDDL);
    }
}
