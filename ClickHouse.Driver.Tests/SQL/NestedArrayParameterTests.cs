using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// End-to-end coverage for nested array (<c>Array(Array(T))</c>) parameter support.
/// Exercises both jagged and rectangular multidimensional CLR array shapes on the write path,
/// and confirms reads always materialise as jagged regardless of the value originally inserted.
/// </summary>
public class NestedArrayParameterTests : AbstractConnectionTestFixture
{
    private static async Task<object> SelectEcho(ClickHouseConnection conn, string typeHint, object value)
    {
        using var command = conn.CreateCommand();
        command.CommandText = $"SELECT {{p:{typeHint}}} as result";
        command.AddParameter("p", value);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True, "Expected one row back from echo SELECT");
        return reader.GetValue(0);
    }

    // ----- SELECT echo round-trips (jagged) -----

    [Test]
    public async Task ExecuteReaderAsync_JaggedByteArray2D_RoundTrips()
    {
        var input = new byte[][] { new byte[] { 1, 2 }, new byte[] { 3, 4, 5 } };
        var result = (byte[][])await SelectEcho(connection, "Array(Array(UInt8))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedInt32Array2D_RoundTrips()
    {
        var input = new int[][] { new[] { 1, 2 }, new[] { 3, 4 }, new[] { 5 } };
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedStringArray2D_RoundTrips()
    {
        var input = new string[][] { new[] { "alpha", "beta" }, new[] { "gamma" } };
        var result = (string[][])await SelectEcho(connection, "Array(Array(String))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedStringArrayWithEscapes_PreservesQuotesAndBackslashes()
    {
        var input = new string[][] { new[] { "a'b", "c\\d" }, new[] { "no escapes" } };
        var result = (string[][])await SelectEcho(connection, "Array(Array(String))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedInt32Array3D_RoundTrips()
    {
        var input = new int[][][]
        {
            new int[][] { new[] { 1, 2 }, new[] { 3 } },
            new int[][] { new[] { 4, 5, 6 } },
        };
        var result = (int[][][])await SelectEcho(connection, "Array(Array(Array(Int32)))", input);
        Assert.That(result.Length, Is.EqualTo(input.Length));
        for (var i = 0; i < input.Length; i++)
        {
            Assert.That(result[i], Is.EqualTo(input[i]));
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_RaggedJaggedArray_DoesNotRequireRectangularity()
    {
        var input = new int[][] { new[] { 1, 2, 3 }, new[] { 4 }, new int[0], new[] { 5, 6 } };
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_NullableInnerElements_RoundTrips()
    {
        var input = new int?[][] { new int?[] { 1, null, 3 }, new int?[] { null }, new int?[] { 4, 5 } };
        var result = (int?[][])await SelectEcho(connection, "Array(Array(Nullable(Int32)))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_EmptyOuterArray_RoundTrips()
    {
        var input = Array.Empty<int[]>();
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task ExecuteReaderAsync_OuterArrayWithEmptyInner_RoundTrips()
    {
        var input = new int[][] { new int[0], new[] { 1, 2 }, new int[0] };
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_ListOfList_RoundTrips()
    {
        var input = new List<List<int>> { new() { 1, 2 }, new() { 3, 4, 5 } };
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result.Length, Is.EqualTo(input.Count));
        for (var i = 0; i < input.Count; i++)
        {
            Assert.That(result[i], Is.EqualTo(input[i].ToArray()));
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_ListOfArray_RoundTrips()
    {
        var input = new List<int[]> { new[] { 10, 20 }, new[] { 30 } };
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result[0], Is.EqualTo(input[0]));
        Assert.That(result[1], Is.EqualTo(input[1]));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedIPv4Array_RoundTrips()
    {
        var input = new IPAddress[][]
        {
            new[] { IPAddress.Parse("10.0.0.1"), IPAddress.Parse("10.0.0.2") },
            new[] { IPAddress.Parse("192.168.0.1") },
        };
        var result = (IPAddress[][])await SelectEcho(connection, "Array(Array(IPv4))", input);
        Assert.That(result.Length, Is.EqualTo(input.Length));
        for (var i = 0; i < input.Length; i++)
        {
            Assert.That(result[i], Is.EqualTo(input[i]));
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedFloat64Array_RoundTrips()
    {
        var input = new double[][] { new[] { 1.5, 2.25 }, new[] { 3.125, 4.0625, 5.0 } };
        var result = (double[][])await SelectEcho(connection, "Array(Array(Float64))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    // ----- SELECT echo round-trips (multidim) -----

    [Test]
    public async Task ExecuteReaderAsync_Multidim2DByteArray_AcceptedAsNestedAndReadsAsJagged()
    {
        var input = new byte[,] { { 1, 2, 3 }, { 4, 5, 6 } };
        var result = (byte[][])await SelectEcho(connection, "Array(Array(UInt8))", input);
        Assert.That(result.Length, Is.EqualTo(2));
        Assert.That(result[0], Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(result[1], Is.EqualTo(new byte[] { 4, 5, 6 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_Multidim2DInt32Array_AcceptedAsNested()
    {
        var input = new int[,] { { 10, 20 }, { 30, 40 }, { 50, 60 } };
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result.Length, Is.EqualTo(3));
        Assert.That(result[0], Is.EqualTo(new[] { 10, 20 }));
        Assert.That(result[1], Is.EqualTo(new[] { 30, 40 }));
        Assert.That(result[2], Is.EqualTo(new[] { 50, 60 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_Multidim2DStringArray_AcceptedAsNested()
    {
        var input = new string[,] { { "a", "b" }, { "c", "d" } };
        var result = (string[][])await SelectEcho(connection, "Array(Array(String))", input);
        Assert.That(result[0], Is.EqualTo(new[] { "a", "b" }));
        Assert.That(result[1], Is.EqualTo(new[] { "c", "d" }));
    }

    [Test]
    public async Task ExecuteReaderAsync_Multidim3DInt32Array_AcceptedAsNested()
    {
        var input = new int[2, 2, 2]
        {
            { { 1, 2 }, { 3, 4 } },
            { { 5, 6 }, { 7, 8 } },
        };
        var result = (int[][][])await SelectEcho(connection, "Array(Array(Array(Int32)))", input);
        Assert.That(result.Length, Is.EqualTo(2));
        Assert.That(result[0][0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(result[0][1], Is.EqualTo(new[] { 3, 4 }));
        Assert.That(result[1][0], Is.EqualTo(new[] { 5, 6 }));
        Assert.That(result[1][1], Is.EqualTo(new[] { 7, 8 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_EmptyMultidimOuter_RoundTripsAsEmptyJagged()
    {
        var input = new int[0, 5];
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task ExecuteReaderAsync_MultidimWithZeroInnerDim_YieldsEmptyInnerRows()
    {
        var input = new int[3, 0];
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result.Length, Is.EqualTo(3));
        Assert.That(result.All(r => r.Length == 0), Is.True);
    }

    [Test]
    public async Task ExecuteReaderAsync_LargerMultidim_RoundTrips()
    {
        const int rows = 20;
        const int cols = 30;
        var input = new int[rows, cols];
        for (var r = 0; r < rows; r++)
        {
            for (var c = 0; c < cols; c++)
            {
                input[r, c] = r * cols + c;
            }
        }
        var result = (int[][])await SelectEcho(connection, "Array(Array(Int32))", input);
        Assert.That(result.Length, Is.EqualTo(rows));
        for (var r = 0; r < rows; r++)
        {
            for (var c = 0; c < cols; c++)
            {
                Assert.That(result[r][c], Is.EqualTo(input[r, c]));
            }
        }
    }

    // ----- Auto type inference (no explicit hint) -----

    [Test]
    public async Task ExecuteReaderAsync_JaggedByteArray_WithAutoTypeInferenceViaAtParam_RoundTrips()
    {
        // No {p:Type} hint — type must be inferred from the byte[][] value as Array(Array(UInt8)).
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT @p as result";
        command.AddParameter("p", new byte[][] { new byte[] { 1, 2 }, new byte[] { 3 } });
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = (byte[][])reader.GetValue(0);
        Assert.That(result[0], Is.EqualTo(new byte[] { 1, 2 }));
        Assert.That(result[1], Is.EqualTo(new byte[] { 3 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_Multidim2DByteArray_WithAutoTypeInferenceViaAtParam_RoundTrips()
    {
        // Multidim must also be inferable now that TypeConverter is rank-aware.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT @p as result";
        command.AddParameter("p", new byte[,] { { 7, 8 }, { 9, 10 } });
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = (byte[][])reader.GetValue(0);
        Assert.That(result[0], Is.EqualTo(new byte[] { 7, 8 }));
        Assert.That(result[1], Is.EqualTo(new byte[] { 9, 10 }));
    }

    // ----- INSERT into table + read back -----

    [Test]
    public async Task ShouldInsertParameterized2DJaggedArrayIntoTable()
    {
        const string table = "test.nested_array_2d_jagged";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (arr Array(Array(Int32))) ENGINE Memory");

        var value = new int[][] { new[] { 1, 2 }, new[] { 3, 4, 5 } };
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = $"INSERT INTO {table} VALUES ({{values:Array(Array(Int32))}})";
            insert.AddParameter("values", value);
            await insert.ExecuteNonQueryAsync();
        }

        using var reader = await connection.ExecuteReaderAsync($"SELECT arr FROM {table}");
        Assert.That(reader.Read(), Is.True);
        var actual = (int[][])reader.GetValue(0);
        Assert.That(actual, Is.EqualTo(value));
    }

    [Test]
    public async Task ShouldInsertParameterized2DMultidimArrayIntoTable()
    {
        const string table = "test.nested_array_2d_multidim";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (arr Array(Array(UInt8))) ENGINE Memory");

        var value = new byte[,] { { 1, 2, 3 }, { 4, 5, 6 } };
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = $"INSERT INTO {table} VALUES ({{values:Array(Array(UInt8))}})";
            insert.AddParameter("values", value);
            await insert.ExecuteNonQueryAsync();
        }

        using var reader = await connection.ExecuteReaderAsync($"SELECT arr FROM {table}");
        Assert.That(reader.Read(), Is.True);
        var actual = (byte[][])reader.GetValue(0);
        Assert.That(actual[0], Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(actual[1], Is.EqualTo(new byte[] { 4, 5, 6 }));
    }

    [Test]
    public async Task ShouldInsertParameterized3DJaggedArrayIntoTable()
    {
        const string table = "test.nested_array_3d_jagged";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (arr Array(Array(Array(Int32)))) ENGINE Memory");

        var value = new int[][][]
        {
            new int[][] { new[] { 1, 2 }, new[] { 3 } },
            new int[][] { new[] { 4, 5, 6 } },
        };
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = $"INSERT INTO {table} VALUES ({{values:Array(Array(Array(Int32)))}})";
            insert.AddParameter("values", value);
            await insert.ExecuteNonQueryAsync();
        }

        using var reader = await connection.ExecuteReaderAsync($"SELECT arr FROM {table}");
        Assert.That(reader.Read(), Is.True);
        var actual = (int[][][])reader.GetValue(0);
        Assert.That(actual.Length, Is.EqualTo(value.Length));
        for (var i = 0; i < value.Length; i++)
        {
            for (var j = 0; j < value[i].Length; j++)
            {
                Assert.That(actual[i][j], Is.EqualTo(value[i][j]));
            }
        }
    }

    [Test]
    public async Task ShouldInsertParameterized3DMultidimArrayIntoTable()
    {
        const string table = "test.nested_array_3d_multidim";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (arr Array(Array(Array(Int32)))) ENGINE Memory");

        var value = new int[1, 2, 3] { { { 1, 2, 3 }, { 4, 5, 6 } } };
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = $"INSERT INTO {table} VALUES ({{values:Array(Array(Array(Int32)))}})";
            insert.AddParameter("values", value);
            await insert.ExecuteNonQueryAsync();
        }

        using var reader = await connection.ExecuteReaderAsync($"SELECT arr FROM {table}");
        Assert.That(reader.Read(), Is.True);
        var actual = (int[][][])reader.GetValue(0);
        Assert.That(actual.Length, Is.EqualTo(1));
        Assert.That(actual[0][0], Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(actual[0][1], Is.EqualTo(new[] { 4, 5, 6 }));
    }

    [Test]
    public async Task ShouldInsertParameterizedNestedNullableInnerIntoTable()
    {
        const string table = "test.nested_array_nullable_inner";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {table} (arr Array(Array(Nullable(Int32)))) ENGINE Memory");

        var value = new int?[][] { new int?[] { 1, null, 3 }, new int?[] { null, 5 } };
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = $"INSERT INTO {table} VALUES ({{values:Array(Array(Nullable(Int32)))}})";
            insert.AddParameter("values", value);
            await insert.ExecuteNonQueryAsync();
        }

        using var reader = await connection.ExecuteReaderAsync($"SELECT arr FROM {table}");
        Assert.That(reader.Read(), Is.True);
        var actual = (int?[][])reader.GetValue(0);
        Assert.That(actual, Is.EqualTo(value));
    }

    // ----- Broader inner types (jagged) -----

    [Test]
    public async Task ExecuteReaderAsync_JaggedUUIDArray_RoundTrips()
    {
        var input = new Guid[][]
        {
            new[] { Guid.Parse("11111111-1111-1111-1111-111111111111"), Guid.Parse("22222222-2222-2222-2222-222222222222") },
            new[] { Guid.Parse("33333333-3333-3333-3333-333333333333") },
        };
        var result = (Guid[][])await SelectEcho(connection, "Array(Array(UUID))", input);
        Assert.That(result.Length, Is.EqualTo(input.Length));
        for (var i = 0; i < input.Length; i++)
        {
            Assert.That(result[i], Is.EqualTo(input[i]));
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedDateTimeArray_RoundTrips()
    {
        // DateTime values must be Unspecified-kind to round-trip without timezone re-interpretation.
        var input = new DateTime[][]
        {
            new[] { new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Unspecified), new DateTime(2026, 1, 2, 13, 30, 0, DateTimeKind.Unspecified) },
            new[] { new DateTime(2026, 6, 15, 9, 0, 0, DateTimeKind.Unspecified) },
        };
        var result = (DateTime[][])await SelectEcho(connection, "Array(Array(DateTime))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedFixedStringArray_RoundTrips()
    {
        var input = new string[][] { new[] { "abcd", "efgh" }, new[] { "wxyz" } };
        var result = (string[][])await SelectEcho(connection, "Array(Array(FixedString(4)))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedBooleanArray_RoundTrips()
    {
        var input = new bool[][] { new[] { true, false, true }, new[] { false } };
        var result = (bool[][])await SelectEcho(connection, "Array(Array(Bool))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedInt64Array_RoundTrips()
    {
        var input = new long[][] { new[] { long.MinValue, 0L, long.MaxValue }, new[] { 42L } };
        var result = (long[][])await SelectEcho(connection, "Array(Array(Int64))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedUInt64Array_RoundTrips()
    {
        var input = new ulong[][] { new[] { 0UL, ulong.MaxValue }, new[] { 100UL } };
        var result = (ulong[][])await SelectEcho(connection, "Array(Array(UInt64))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedArrayWithLowCardinalityInner_RoundTrips()
    {
        var input = new string[][] { new[] { "x", "y", "x" }, new[] { "z" } };
        var result = (string[][])await SelectEcho(connection, "Array(Array(LowCardinality(String)))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedArrayOfTuple_RoundTrips()
    {
        // Inner Tuple(Int32, String) recurses through both ArrayType and TupleType formatters.
        var input = new[]
        {
            new[] { Tuple.Create(1, "a"), Tuple.Create(2, "b") },
            new[] { Tuple.Create(3, "c") },
        };
        var result = (Array)await SelectEcho(connection, "Array(Array(Tuple(Int32, String)))", input);
        Assert.That(result.Length, Is.EqualTo(input.Length));
    }

    // ----- Decimal / Date inner types (dedicated formatter branches) -----

    [Test]
    public async Task ExecuteReaderAsync_JaggedDecimalArray_RoundTrips()
    {
        // Decimal hits a special formatter branch (HttpParameterFormatter handles
        // ClickHouseDecimal / string / decimal value variants separately).
        var input = new decimal[][]
        {
            new[] { 1.25m, -2.5m, 3.125m },
            new[] { 0m, 0.001m },
        };
        var result = (Array)await SelectEcho(connection, "Array(Array(Decimal(18,3)))", input);
        Assert.That(result.Length, Is.EqualTo(input.Length));
        for (var i = 0; i < input.Length; i++)
        {
            var row = (Array)result.GetValue(i);
            Assert.That(row.Length, Is.EqualTo(input[i].Length));
            for (var j = 0; j < input[i].Length; j++)
            {
                Assert.That((decimal)(ClickHouse.Driver.Numerics.ClickHouseDecimal)row.GetValue(j), Is.EqualTo(input[i][j]));
            }
        }
    }

    [Test]
    public async Task ExecuteReaderAsync_JaggedDateArray_RoundTrips()
    {
        // Date is a different ClickHouse type from DateTime with its own formatter branch
        // (yyyy-MM-dd; no time component). Use Unspecified-kind to skip TZ shifting.
        var input = new DateTime[][]
        {
            new[] { new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), new DateTime(2026, 12, 31, 0, 0, 0, DateTimeKind.Unspecified) },
            new[] { new DateTime(2020, 6, 15, 0, 0, 0, DateTimeKind.Unspecified) },
        };
        var result = (DateTime[][])await SelectEcho(connection, "Array(Array(Date))", input);
        Assert.That(result, Is.EqualTo(input));
    }

    // ----- Deeper nesting -----

    [Test]
    public async Task ExecuteReaderAsync_FourDeepJaggedArray_RoundTrips()
    {
        var input = new int[][][][]
        {
            new int[][][]
            {
                new int[][] { new[] { 1, 2 } },
                new int[][] { new[] { 3 }, new[] { 4, 5 } },
            },
        };
        var result = (int[][][][])await SelectEcho(connection, "Array(Array(Array(Array(Int32))))", input);
        Assert.That(result.Length, Is.EqualTo(1));
        Assert.That(result[0][0][0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(result[0][1][0], Is.EqualTo(new[] { 3 }));
        Assert.That(result[0][1][1], Is.EqualTo(new[] { 4, 5 }));
    }

    [Test]
    public async Task ExecuteReaderAsync_FourDeepMultidimArray_RoundTrips()
    {
        // 4-rank multidim — each rank gets sliced one at a time on the write path.
        var input = new int[1, 1, 1, 2] { { { { 7, 8 } } } };
        var result = (int[][][][])await SelectEcho(connection, "Array(Array(Array(Array(Int32))))", input);
        Assert.That(result.Length, Is.EqualTo(1));
        Assert.That(result[0][0][0], Is.EqualTo(new[] { 7, 8 }));
    }

    // ----- Multidim with reference-type inner -----

    [Test]
    public async Task ExecuteReaderAsync_MultidimGuidArray_RoundTrips()
    {
        var input = new Guid[2, 2];
        input[0, 0] = Guid.Parse("11111111-1111-1111-1111-111111111111");
        input[0, 1] = Guid.Parse("22222222-2222-2222-2222-222222222222");
        input[1, 0] = Guid.Parse("33333333-3333-3333-3333-333333333333");
        input[1, 1] = Guid.Parse("44444444-4444-4444-4444-444444444444");
        var result = (Guid[][])await SelectEcho(connection, "Array(Array(UUID))", input);
        Assert.That(result[0], Is.EqualTo(new[] { input[0, 0], input[0, 1] }));
        Assert.That(result[1], Is.EqualTo(new[] { input[1, 0], input[1, 1] }));
    }

    [Test]
    public async Task ExecuteReaderAsync_MultidimDateTimeArray_RoundTrips()
    {
        var input = new DateTime[1, 2]
        {
            { new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), new DateTime(2026, 1, 2, 12, 30, 45, DateTimeKind.Unspecified) },
        };
        var result = (DateTime[][])await SelectEcho(connection, "Array(Array(DateTime))", input);
        Assert.That(result[0], Is.EqualTo(new[] { input[0, 0], input[0, 1] }));
    }

    [Test]
    public async Task ExecuteReaderAsync_Multidim3DStringArray_RoundTrips()
    {
        var input = new string[2, 1, 2]
        {
            { { "a", "b" } },
            { { "c", "d" } },
        };
        var result = (string[][][])await SelectEcho(connection, "Array(Array(Array(String)))", input);
        Assert.That(result.Length, Is.EqualTo(2));
        Assert.That(result[0][0], Is.EqualTo(new[] { "a", "b" }));
        Assert.That(result[1][0], Is.EqualTo(new[] { "c", "d" }));
    }

    // ----- ClickHouseClient (primary API) coverage -----

    [Test]
    public async Task ClientExecuteReaderAsync_JaggedByteArray_RoundTripsViaPrimaryApi()
    {
        var input = new byte[][] { new byte[] { 1, 2 }, new byte[] { 3 } };
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("p", input);
        using var reader = await client.ExecuteReaderAsync(
            "SELECT {p:Array(Array(UInt8))} as result", parameters);
        Assert.That(reader.Read(), Is.True);
        var result = (byte[][])reader.GetValue(0);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task ClientExecuteReaderAsync_Multidim2DByteArray_RoundTripsViaPrimaryApi()
    {
        var input = new byte[,] { { 1, 2, 3 }, { 4, 5, 6 } };
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("p", input);
        using var reader = await client.ExecuteReaderAsync(
            "SELECT {p:Array(Array(UInt8))} as result", parameters);
        Assert.That(reader.Read(), Is.True);
        var result = (byte[][])reader.GetValue(0);
        Assert.That(result[0], Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(result[1], Is.EqualTo(new byte[] { 4, 5, 6 }));
    }

    [Test]
    public async Task ClientInsertBinaryAsync_JaggedArrayColumn_RoundTripsViaPrimaryApi()
    {
        var tableName = SanitizeTableName($"nested_client_jagged_{Guid.NewGuid():N}");
        var fqn = $"test.{tableName}";
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {fqn}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {fqn} (id Int32, arr Array(Array(Int32))) ENGINE Memory");
        try
        {
            var rows = new List<object[]>
            {
                new object[] { 0, new int[][] { new[] { 1, 2 }, new[] { 3 } } },
                new object[] { 1, new int[][] { new[] { 4, 5, 6 } } },
            };
            await client.InsertBinaryAsync(fqn, new[] { "id", "arr" }, rows);

            using var reader = await client.ExecuteReaderAsync($"SELECT arr FROM {fqn} ORDER BY id");
            Assert.That(reader.Read(), Is.True);
            Assert.That((int[][])reader.GetValue(0), Is.EqualTo(rows[0][1]));
            Assert.That(reader.Read(), Is.True);
            Assert.That((int[][])reader.GetValue(0), Is.EqualTo(rows[1][1]));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {fqn}");
        }
    }

    [Test]
    public async Task ClientInsertBinaryAsync_MultidimArrayColumn_RoundTripsViaPrimaryApi()
    {
        var tableName = SanitizeTableName($"nested_client_multidim_{Guid.NewGuid():N}");
        var fqn = $"test.{tableName}";
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {fqn}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {fqn} (id Int32, arr Array(Array(UInt8))) ENGINE Memory");
        try
        {
            var matrix = new byte[,] { { 10, 20 }, { 30, 40 } };
            await client.InsertBinaryAsync(
                fqn,
                new[] { "id", "arr" },
                new List<object[]> { new object[] { 0, matrix } });

            using var reader = await client.ExecuteReaderAsync($"SELECT arr FROM {fqn}");
            Assert.That(reader.Read(), Is.True);
            var got = (byte[][])reader.GetValue(0);
            Assert.That(got[0], Is.EqualTo(new byte[] { 10, 20 }));
            Assert.That(got[1], Is.EqualTo(new byte[] { 30, 40 }));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {fqn}");
        }
    }

    [Test]
    public async Task ClientInsertBinaryAsync_Multidim3DArrayColumn_RoundTripsViaPrimaryApi()
    {
        var tableName = SanitizeTableName($"nested_client_3d_{Guid.NewGuid():N}");
        var fqn = $"test.{tableName}";
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {fqn}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {fqn} (id Int32, arr Array(Array(Array(Int32)))) ENGINE Memory");
        try
        {
            var cube = new int[2, 2, 2] { { { 1, 2 }, { 3, 4 } }, { { 5, 6 }, { 7, 8 } } };
            await client.InsertBinaryAsync(
                fqn,
                new[] { "id", "arr" },
                new List<object[]> { new object[] { 0, cube } });

            using var reader = await client.ExecuteReaderAsync($"SELECT arr FROM {fqn}");
            Assert.That(reader.Read(), Is.True);
            var got = (int[][][])reader.GetValue(0);
            Assert.That(got[0][0], Is.EqualTo(new[] { 1, 2 }));
            Assert.That(got[0][1], Is.EqualTo(new[] { 3, 4 }));
            Assert.That(got[1][0], Is.EqualTo(new[] { 5, 6 }));
            Assert.That(got[1][1], Is.EqualTo(new[] { 7, 8 }));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {fqn}");
        }
    }

    // ----- GetFieldValue<T> with multidim T (materialises jagged result as rectangular) -----

    [Test]
    public async Task GetFieldValueMultidim_Rank2RectangularByteArray_MaterialisesAsMultidim()
    {
        var input = new byte[][] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(UInt8))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var result = reader.GetFieldValue<byte[,]>(0);
        Assert.That(result.Rank, Is.EqualTo(2));
        Assert.That(result.GetLength(0), Is.EqualTo(2));
        Assert.That(result.GetLength(1), Is.EqualTo(3));
        Assert.That(result[0, 0], Is.EqualTo(1));
        Assert.That(result[0, 2], Is.EqualTo(3));
        Assert.That(result[1, 1], Is.EqualTo(5));
        Assert.That(result[1, 2], Is.EqualTo(6));
    }

    [Test]
    public async Task GetFieldValueMultidim_RoundTripMultidimInsertReadAsMultidim_PreservesShape()
    {
        // Insert as multidim, read back as multidim — both ends are T[,].
        var inserted = new int[2, 3] { { 10, 20, 30 }, { 40, 50, 60 } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Int32))} as result";
        command.AddParameter("p", inserted);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var result = reader.GetFieldValue<int[,]>(0);
        Assert.That(result, Is.EqualTo(inserted));
    }

    [Test]
    public async Task GetFieldValueMultidim_Rank3RectangularInt32_MaterialisesAsRank3()
    {
        var input = new int[2, 2, 2] { { { 1, 2 }, { 3, 4 } }, { { 5, 6 }, { 7, 8 } } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Array(Int32)))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var result = reader.GetFieldValue<int[,,]>(0);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task GetFieldValueMultidim_StringElementType_MaterialisesAsRank2OfString()
    {
        var input = new string[2, 2] { { "a", "b" }, { "c", "d" } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(String))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var result = reader.GetFieldValue<string[,]>(0);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public async Task GetFieldValueMultidim_NullableInner_PreservesNullsAndShape()
    {
        var input = new int?[][] { new int?[] { 1, null, 3 }, new int?[] { null, 5, 6 } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Nullable(Int32)))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var result = reader.GetFieldValue<int?[,]>(0);
        Assert.That(result.GetLength(0), Is.EqualTo(2));
        Assert.That(result.GetLength(1), Is.EqualTo(3));
        Assert.That(result[0, 1], Is.Null);
        Assert.That(result[1, 0], Is.Null);
        Assert.That(result[1, 2], Is.EqualTo(6));
    }

    [Test]
    public async Task GetFieldValueMultidim_RaggedServerData_ThrowsInvalidOperationException()
    {
        // Server returns a ragged value; the caller asked for rectangular materialisation.
        var raggedInput = new int[][] { new[] { 1, 2, 3 }, new[] { 4 } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Int32))} as result";
        command.AddParameter("p", raggedInput);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var ex = Assert.Throws<InvalidOperationException>(
            () => reader.GetFieldValue<int[,]>(0));
        Assert.That(ex!.Message, Does.Contain("rectangular"));
    }

    [Test]
    public async Task GetFieldValueMultidim_EmptyOuter_ReturnsEmptyRectangular()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Int32))} as result";
        command.AddParameter("p", Array.Empty<int[]>());
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var result = reader.GetFieldValue<int[,]>(0);
        Assert.That(result.GetLength(0), Is.EqualTo(0));
    }

    [Test]
    public async Task GetFieldValueMultidim_InsertedAsMultidimReadAsMultidim_FullRoundTrip()
    {
        // The headline use case: schema is rectangular by construction, so both ends use T[,].
        const string table = "test.nested_multidim_roundtrip";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {table}");
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE {table} (id Int32, m Array(Array(UInt8))) ENGINE Memory");

        var multidim = new byte[,] { { 100, 101, 102 }, { 200, 201, 202 } };
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = $"INSERT INTO {table} VALUES (0, {{m:Array(Array(UInt8))}})";
            insert.AddParameter("m", multidim);
            await insert.ExecuteNonQueryAsync();
        }

        using var reader = await connection.ExecuteReaderAsync($"SELECT m FROM {table}");
        Assert.That(reader.Read(), Is.True);
        var result = reader.GetFieldValue<byte[,]>(0);
        Assert.That(result, Is.EqualTo(multidim));
    }

    // ----- Negative tests -----

    [Test]
    public void ExecuteReaderAsync_ScalarForNestedArrayType_ThrowsWithUsefulMessage()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {m_value:Array(Array(UInt8))} as result";
        command.AddParameter("m_value", (byte)219);
        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await command.ExecuteReaderAsync());
        Assert.That(ex!.Message, Does.Contain("m_value"));
        // The pre-fix error message ("Cannot convert 219 to Array(UInt8)") dropped the outer
        // type — the whole point of issue #320's diagnostic complaint. Require the full type.
        Assert.That(ex.Message, Does.Contain("Array(Array(UInt8))"));
    }

    [Test]
    public void ExecuteReaderAsync_FlatArrayForNestedArrayType_ThrowsWithUsefulMessage()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Int32))} as result";
        command.AddParameter("p", new[] { 1, 2, 3 });
        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await command.ExecuteReaderAsync());
        Assert.That(ex!.Message, Does.Contain("'p'"));
        Assert.That(ex.Message, Does.Contain("Array(Array(Int32))"));
    }
}
