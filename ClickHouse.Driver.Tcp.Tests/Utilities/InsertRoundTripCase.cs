using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Sample columns for the INSERT → SELECT round-trip integration tests, in the spirit of the HTTP suite's
/// <c>TestCases</c>: one place that enumerates a representative value set per supported type, so a single
/// parameterized test exercises every type rather than a hand-written test each. The test creates a matching
/// one-column table, inserts the case's column, selects it back, and asserts the read-back column equals the
/// expected column. Covers the types the native codecs support today — the fixed-width integers, the raw enum
/// aliases, <c>String</c>, and <c>DateTime</c>.
///
/// <para>
/// A case usually inserts and reads back the same CLR type, so the inserted column doubles as the expected one.
/// Some cases differ, though — inserting a <c>DateTimeOffset</c> into a <c>DateTime</c> column reads back a
/// <c>DateTime</c> — so a case carries both an insert-column builder and an expected-column builder; the common
/// factories set them to the same builder.
/// </para>
/// </summary>
public sealed class InsertRoundTripCase
{
    private readonly Func<string, IColumn> buildInsert;
    private readonly Func<string, IColumn> buildExpected;

    private InsertRoundTripCase(string label, string clickHouseType, Func<string, IColumn> buildInsert, Func<string, IColumn> buildExpected)
    {
        Label = label;
        ClickHouseType = clickHouseType;
        this.buildInsert = buildInsert;
        this.buildExpected = buildExpected;
    }

    /// <summary>The ClickHouse type for the target column (used both to create the table and as the column header).</summary>
    public string ClickHouseType { get; }

    private string Label { get; }

    /// <summary>Builds the column to insert, stamped with <paramref name="columnName"/>.</summary>
    /// <param name="columnName">The column name, which must match the target table column.</param>
    /// <returns>The column to insert.</returns>
    internal IColumn BuildInsertColumn(string columnName) => buildInsert(columnName);

    /// <summary>Builds the column whose values the read-back column is expected to equal.</summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>The expected column.</returns>
    internal IColumn BuildExpectedColumn(string columnName) => buildExpected(columnName);

    public override string ToString() => Label;

    /// <summary>All round-trip cases, for use as an NUnit <c>TestCaseSource</c>.</summary>
    public static IEnumerable<InsertRoundTripCase> Cases()
    {
        yield return Primitive("UInt8", new byte[] { 0, 1, 128, 255 });
        yield return Primitive("Int8", new sbyte[] { -128, -1, 0, 127 });
        yield return Primitive("UInt16", new ushort[] { 0, 258, ushort.MaxValue });
        yield return Primitive("Int16", new short[] { short.MinValue, -1, 0, short.MaxValue });
        yield return Primitive("UInt32", new uint[] { 0, 1, uint.MaxValue });
        yield return Primitive("Int32", new[] { int.MinValue, -1, 0, int.MaxValue });
        yield return Primitive("UInt64", new ulong[] { 0, 1, ulong.MaxValue });
        yield return Primitive("Int64", new[] { long.MinValue, -1, 0, long.MaxValue });
        yield return Primitive("UInt128", new[] { UInt128.Zero, UInt128.One, UInt128.MaxValue });
        yield return Primitive("Int128", new[] { Int128.MinValue, -Int128.One, Int128.Zero, Int128.MaxValue });
        yield return Primitive("UInt256", new[] { UInt256.Zero, UInt256.FromBigInteger(1), UInt256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)) });
        yield return Primitive("Int256", new[] { Int256.FromBigInteger(-System.Numerics.BigInteger.Pow(2, 200)), Int256.FromBigInteger(-1), Int256.Zero, Int256.FromBigInteger(System.Numerics.BigInteger.Pow(2, 200)) });

        // Enum columns are inserted and read as their raw underlying ordinals; the ordinals must be declared members.
        yield return Primitive("Enum8('a' = -1, 'b' = 127)", new sbyte[] { -1, 127 });
        yield return Primitive("Enum16('x' = -32768, 'y' = 32767)", new short[] { -32768, 32767 });

        yield return Strings("String", string.Empty, "hello", "héllo✓", "a\0b", new string('x', 500));

        yield return DateTimes(
            "DateTime",
            new DateTime(1988, 8, 28, 11, 22, 33, DateTimeKind.Utc),
            new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            DateTime.UnixEpoch);

        // A DateTimeOffset inserted into a DateTime column round-trips as its UTC instant (read back as DateTime).
        yield return DateTimeOffsets(
            "DateTime",
            new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)),
            new DateTimeOffset(1988, 8, 28, 11, 22, 33, TimeSpan.FromHours(-8)));
    }

    private static InsertRoundTripCase Primitive<T>(string clickHouseType, T[] values)
        where T : unmanaged
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => PrimitiveColumn<T>.FromValues(name, clickHouseType, values));

    private static InsertRoundTripCase Strings(string clickHouseType, params string[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<string>(name, clickHouseType, values));

    private static InsertRoundTripCase DateTimes(string clickHouseType, params DateTime[] values)
        => Same($"{clickHouseType} [{values.Length} rows]", clickHouseType, name => new ArrayColumn<DateTime>(name, clickHouseType, values));

    private static InsertRoundTripCase DateTimeOffsets(string clickHouseType, params DateTimeOffset[] values)
        => new(
            $"{clickHouseType} <- DateTimeOffset [{values.Length} rows]",
            clickHouseType,
            name => new ArrayColumn<DateTimeOffset>(name, clickHouseType, values),
            name => new ArrayColumn<DateTime>(name, clickHouseType, Array.ConvertAll(values, v => v.UtcDateTime)));

    /// <summary>A case that inserts and reads back the same column — the common shape.</summary>
    private static InsertRoundTripCase Same(string label, string clickHouseType, Func<string, IColumn> build)
        => new(label, clickHouseType, build, build);
}
