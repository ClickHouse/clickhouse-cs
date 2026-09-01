using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Signed zero, asserted against the server's rendering of what was stored.
///
/// <para>
/// The corpus carries <c>-0.0</c> in the float cases, but its comparison cannot see the sign: in .NET
/// <c>(-0.0).Equals(0.0)</c> is true, so a write that dropped the sign bit would round-trip as equal. The
/// server's text form distinguishes them, and it is the only oracle here that does. <c>NaN</c> and the
/// infinities need no test of their own, being ordinary corpus values that compare unequal to everything else.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class FloatSpecialValueIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    /// <param name="columnType">The float column to write into.</param>
    /// <param name="settings">Whether the type needs its experimental flag.</param>
    [TestCase("Float32", false)]
    [TestCase("Float64", false)]
    [TestCase("BFloat16", true)]
    public async Task InsertAsync_NegativeZero_KeepsItsSignOnTheServer(string columnType, bool experimental)
    {
        await using ClickHouseTcpClient client = TcpServerFixture.CreateClient();
        var options = experimental
            ? new ClickHouseTcpQueryOptions { Settings = new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal) { ["allow_experimental_bfloat16_type"] = "1" } }
            : null;

        string table = $"tcp_float_special_test_{Guid.NewGuid():N}";
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, v {columnType}) ENGINE = MergeTree ORDER BY id", options, None);
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
                new ClickHouseTcpInsertOptions { Settings = options?.Settings },
                None);

            var rendered = new string[2];
            await foreach (object[] row in client.QueryAsync($"SELECT id, toString(v) FROM {table} ORDER BY id", options, None))
            {
                rendered[Convert.ToInt32(row[0]) - 1] = (string)row[1];
            }

            Assert.Multiple(() =>
            {
                Assert.That(rendered[0], Is.EqualTo("-0"), "the sign bit of a zero has to survive the write");
                Assert.That(rendered[1], Is.EqualTo("0"), "and the positive zero must not acquire one");
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }
}
