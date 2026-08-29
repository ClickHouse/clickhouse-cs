using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Pins <see cref="ClickHouseTcpTypes"/> against the operations it predicts. The point of an interrogative API is
/// that its answer is the operation's answer, so each case asks the question and then does the thing: builds a
/// column of the candidate CLR type and inserts it, or reads a column of the candidate type back. A prediction
/// that disagrees with the outcome is worse than no prediction at all, because a caller would trust it.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpTypesIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static IEnumerable<TestCaseData> WriteCandidates()
    {
        yield return Candidate("UInt64", 7UL);
        yield return Candidate("UInt64", "seven");
        yield return Candidate("String", "seven");
        yield return Candidate("FixedString(4)", new byte[] { 1, 2, 3, 4 });
        yield return Candidate("FixedString(4)", "abcd");
        yield return Candidate("Date", new DateOnly(2024, 6, 15));
        yield return Candidate("Date", new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Utc));
        yield return Candidate("Enum8('a' = -1, 'b' = 127)", "a");
        yield return Candidate("Enum8('a' = -1, 'b' = 127)", (sbyte)-1);
        yield return Candidate("Decimal(9, 2)", 1.25m);
        yield return Candidate("Decimal(38, 2)", 1.25m);
        yield return Candidate("Array(Nullable(DateTime('UTC')))", new DateTime?[] { new(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc), null });
        yield return Candidate("Array(Nullable(DateTime('UTC')))", new[] { new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc) });
    }

    private static IEnumerable<TestCaseData> ReadCandidates()
    {
        yield return new TestCaseData("toUInt32(1)", typeof(uint));
        yield return new TestCaseData("toUInt32(1)", typeof(long));
        yield return new TestCaseData("CAST(1 AS Enum8('a' = 1, 'b' = 2))", typeof(string));
        yield return new TestCaseData("CAST(1 AS Enum8('a' = 1, 'b' = 2))", typeof(sbyte));
        yield return new TestCaseData("toDateTime64('2024-06-15 14:00:00.125', 3, 'UTC')", typeof(DateTime));
        yield return new TestCaseData("toDateTime64('2024-06-15 14:00:00.125', 3, 'UTC')", typeof(TimeSpan));
        yield return new TestCaseData("CAST('1.25' AS Decimal(38, 2))", typeof(decimal));
        yield return new TestCaseData("[toDateTime('2024-06-15 14:00:00', 'UTC')]", typeof(DateTime[]));
        yield return new TestCaseData("map('k', toUInt32(1))", typeof(KeyValuePair<string, uint>[]));
    }

    [TestCaseSource(nameof(WriteCandidates))]
    public async Task CanWrite_ItsAnswer_IsWhetherTheInsertGoesThrough(string clickHouseType, Type elementType, Func<string, IColumn> build)
    {
        bool predicted = ClickHouseTcpTypes.CanWrite(clickHouseType, elementType);

        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value {clickHouseType}) ENGINE = Memory", cancellationToken: None);

            Exception failure = null;
            try
            {
                await client.InsertAsync($"INSERT INTO {table} (value) VALUES", new[] { build("value") }, cancellationToken: None);
            }
            catch (Exception e)
            {
                failure = e;
            }

            Assert.That(
                failure is null,
                Is.EqualTo(predicted),
                predicted
                    ? $"CanWrite said a {elementType} column writes to {clickHouseType}, but the insert failed: {failure?.Message}"
                    : $"CanWrite said a {elementType} column does not write to {clickHouseType}, but the insert went through");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [TestCaseSource(nameof(ReadCandidates))]
    public async Task CanRead_ItsAnswer_IsWhetherReadAsGoesThrough(string expression, Type elementType)
    {
        await using var client = TcpServerFixture.CreateClient();

        await foreach (Block block in client.StreamAsync($"SELECT {expression} AS value", cancellationToken: None))
        {
            bool predicted = ClickHouseTcpTypes.CanRead(block[0].TypeName, elementType);

            Exception failure = null;
            try
            {
                ReadAs(block, elementType);
            }
            catch (TargetInvocationException e)
            {
                failure = e.InnerException;
            }

            Assert.That(
                failure is null,
                Is.EqualTo(predicted),
                predicted
                    ? $"CanRead said '{block[0].TypeName}' reads as {elementType}, but ReadAs failed: {failure?.Message}"
                    : $"CanRead said '{block[0].TypeName}' does not read as {elementType}, but ReadAs succeeded");

            if (!predicted)
            {
                Assert.That(failure, Is.TypeOf<InvalidCastException>());
            }
        }
    }

    // The candidate CLR type has to be the sample's static type, so the column is built by a generic helper rather
    // than from a boxed value.
    private static TestCaseData Candidate<T>(string clickHouseType, T sample)
        => new TestCaseData(clickHouseType, typeof(T), (Func<string, IColumn>)(name => ClickHouseTcpColumn.Create(name, new[] { sample })))
            .SetArgDisplayNames(clickHouseType, typeof(T).Name);

    // Block.ReadAs<T> with a runtime type, the way a caller cannot but this test must.
    private static void ReadAs(Block block, Type elementType)
        => typeof(Block)
            .GetMethod(nameof(Block.ReadAs), new[] { typeof(int) })
            .MakeGenericMethod(elementType)
            .Invoke(block, new object[] { 0 });

    private static string UniqueTableName() => $"tcp_types_test_{Guid.NewGuid():N}";
}
