using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Verifies signed zero by comparing the bits stored by the server.
/// </summary>
[TestFixture]
[Category("Integration")]
public class FloatSpecialValueIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <param name="columnType">The float column to write into.</param>
    /// <param name="reinterpret">The function that reads the stored value back as its raw bits.</param>
    /// <param name="negativeZeroBits">The bit pattern of a negative zero at that width.</param>
    /// <param name="experimental">Whether the type needs its experimental flag.</param>
    [TestCase("Float32", "reinterpretAsUInt32", 2147483648UL, false)]
    [TestCase("Float64", "reinterpretAsUInt64", 9223372036854775808UL, false)]
    [TestCase("BFloat16", "reinterpretAsUInt16", 32768UL, true)]
    public async Task InsertAsync_NegativeZero_KeepsItsSignBitOnTheServer(
        string columnType,
        string reinterpret,
        ulong negativeZeroBits,
        bool experimental)
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        IReadOnlyDictionary<string, string> settings = experimental
            ? new Dictionary<string, string>(StringComparer.Ordinal) { ["allow_experimental_bfloat16_type"] = "1" }
            : null;
        var options = settings is null ? null : new ClickHouseTcpQueryOptions { Settings = settings };

        // Memory avoids MergeTree's signed-zero normalization on ClickHouse 25.8.
        string table = $"tcp_float_special_test_{Guid.NewGuid():N}";
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, v {columnType}) ENGINE = Memory", options, None);
        try
        {
            IColumn[] columns =
            [
                PrimitiveColumn<ulong>.FromValues("id", "UInt64", [1, 2]),
                columnType == "Float64"
                    ? new ArrayColumn<double>("v", columnType, [-0d, 0d])
                    : new ArrayColumn<float>("v", columnType, [-0f, 0f]),
            ];

            await client.InsertAsync(
                $"INSERT INTO {table} (id, v) VALUES",
                columns,
                new ClickHouseTcpInsertOptions { Settings = settings },
                None);

            var stored = new ulong[2];
            await foreach (object[] row in client.QueryAsync(
                $"SELECT id, {reinterpret}(v) FROM {table} ORDER BY id", options, None))
            {
                stored[Convert.ToInt32(row[0]) - 1] = Convert.ToUInt64(row[1]);
            }

            Assert.Multiple(() =>
            {
                Assert.That(stored[0], Is.EqualTo(negativeZeroBits), "the sign bit of a zero has to survive the write");
                Assert.That(stored[1], Is.Zero, "and the positive zero must not acquire one");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }
}
