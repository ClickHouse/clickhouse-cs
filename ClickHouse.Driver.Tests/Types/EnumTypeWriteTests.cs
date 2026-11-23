using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Copy;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class EnumTypeWriteTests : AbstractConnectionTestFixture
{
    private const string NullMessage = "A null is not a label of '{0}'.";

    private const string OrdinalMessage = "The type '{0}' declares no member with the ordinal {1}.";

    private enum Status : sbyte
    {
        Active = 1,
    }

    private class EnumPoco
    {
        public string Label { get; set; }

        public Status Code { get; set; }
    }

    // The server stores a RowBinary enum ordinal without checking it against the members.
    private static IEnumerable<TestCaseData> NonMemberCases()
    {
        yield return new TestCaseData("Enum8('active' = 1)", null, string.Format(NullMessage, "Enum8('active' = 1)"))
            .SetName("{m}(Null_Enum8)");
        yield return new TestCaseData("Enum8('active' = 1)", DBNull.Value, string.Format(NullMessage, "Enum8('active' = 1)"))
            .SetName("{m}(DBNull_Enum8)");
        yield return new TestCaseData("Enum8('zero' = 0, 'one' = 1)", null, string.Format(NullMessage, "Enum8('zero' = 0, 'one' = 1)"))
            .SetName("{m}(Null_Enum8WithZeroMember)");
        yield return new TestCaseData("Enum8('active' = 1)", (sbyte)0, string.Format(OrdinalMessage, "Enum8('active' = 1)", 0))
            .SetName("{m}(UndeclaredOrdinal_Enum8)");
        yield return new TestCaseData("Enum8('active' = 1)", 5, string.Format(OrdinalMessage, "Enum8('active' = 1)", 5))
            .SetName("{m}(UndeclaredIntOrdinal_Enum8)");
        yield return new TestCaseData("Enum16('x' = 100)", null, string.Format(NullMessage, "Enum16('x' = 100)"))
            .SetName("{m}(Null_Enum16)");
        yield return new TestCaseData("Enum16('x' = 100)", (short)7, string.Format(OrdinalMessage, "Enum16('x' = 100)", 7))
            .SetName("{m}(UndeclaredOrdinal_Enum16)");
        yield return new TestCaseData("Array(Enum8('active' = 1))", new object[] { "active", null }, string.Format(NullMessage, "Enum8('active' = 1)"))
            .SetName("{m}(NullElement_ArrayOfEnum8)");
        yield return new TestCaseData("Array(Enum8('active' = 1))", new sbyte[] { 1, 0 }, string.Format(OrdinalMessage, "Enum8('active' = 1)", 0))
            .SetName("{m}(UndeclaredElement_TypedArrayOfEnum8)");
    }

    [Test]
    [TestCaseSource(nameof(NonMemberCases))]
    public async Task InsertBinaryAsync_ValueNotAnEnumMember_ThrowsAndStoresNothing(string columnType, object value, string expectedMessage)
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (id UInt8, e {columnType}) ENGINE Memory");

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(() =>
            client.InsertBinaryAsync(table, ["id", "e"], [[(byte)1, value]]));

        var stored = await client.ExecuteScalarAsync($"SELECT count() FROM {table}");

        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.TypeOf<ArgumentException>());
            Assert.That(ex.InnerException.Message, Does.StartWith(expectedMessage));
            Assert.That(stored, Is.EqualTo(0UL));
        });
    }

    private static IEnumerable<TestCaseData> NonMemberPocoCases()
    {
        yield return new TestCaseData(new EnumPoco { Label = null, Code = Status.Active }, string.Format(NullMessage, "Enum8('active' = 1)"))
            .SetName("{m}(NullStringProperty)");
        yield return new TestCaseData(new EnumPoco { Label = "active", Code = (Status)0 }, string.Format(OrdinalMessage, "Enum8('active' = 1)", 0))
            .SetName("{m}(UndeclaredEnumProperty)");
    }

    [Test]
    [TestCaseSource(nameof(NonMemberPocoCases))]
    public async Task InsertBinaryAsyncPoco_ValueNotAnEnumMember_ThrowsAndStoresNothing(object row, string expectedMessage)
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (Label Enum8('active' = 1), Code Enum8('active' = 1)) ENGINE Memory");
        client.RegisterBinaryInsertType<EnumPoco>();

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(() => client.InsertBinaryAsync(table, new[] { (EnumPoco)row }));

        var stored = await client.ExecuteScalarAsync($"SELECT count() FROM {table}");

        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.TypeOf<ArgumentException>());
            Assert.That(ex.InnerException.Message, Does.StartWith(expectedMessage));
            Assert.That(stored, Is.EqualTo(0UL));
        });
    }

    private static IEnumerable<TestCaseData> DeclaredOrdinalCases()
    {
        yield return new TestCaseData("Enum8('zero' = 0, 'one' = 1)", (sbyte)0, "zero").SetName("{m}(ZeroOrdinal_Enum8)");
        yield return new TestCaseData("Enum8('min' = -128, 'max' = 127)", -128, "min").SetName("{m}(MinOrdinal_Enum8)");
        yield return new TestCaseData("Enum16('min' = -32768, 'max' = 32767)", (short)32767, "max").SetName("{m}(MaxOrdinal_Enum16)");
    }

    [Test]
    [TestCaseSource(nameof(DeclaredOrdinalCases))]
    public async Task InsertBinaryAsync_DeclaredEnumOrdinal_StoresThatMember(string columnType, object ordinal, string expectedLabel)
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (e {columnType}) ENGINE Memory");

        await client.InsertBinaryAsync(table, ["e"], [[ordinal]]);

        Assert.That(await client.ExecuteScalarAsync($"SELECT toString(e) FROM {table}"), Is.EqualTo(expectedLabel));
    }

    // A bare Enum8 in ColumnTypes resolves to a type without members, so the client has nothing to check against.
    [Test]
    public async Task InsertBinaryAsync_EnumColumnTypeWithoutMembers_WritesTheOrdinal()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (e Enum8('active' = 1)) ENGINE Memory");
        var options = new InsertOptions { ColumnTypes = new Dictionary<string, string> { ["e"] = "Enum8" } };

        await client.InsertBinaryAsync(table, ["e"], [[(sbyte)1]], options);

        Assert.That(await client.ExecuteScalarAsync($"SELECT toString(e) FROM {table}"), Is.EqualTo("active"));
    }
}
