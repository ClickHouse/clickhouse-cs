using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Numerics;
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

    // ----- SELECT echo round-trips: parameterised over (typeHint, input, jaggedExpected) -----
    //
    // ClickHouse always materialises Array(Array(T)) as jagged on read, so the expected shape
    // is the canonical jagged form. Inputs vary: jagged T[][] (same as expected), rectangular
    // multidim T[,], List<List<T>>, etc. — all round-trip through the same wire format.

    public static IEnumerable<TestCaseData> SelectEchoCases()
    {
        // ---- Jagged inputs ----
        yield return Case("Array(Array(UInt8))",
            new byte[][] { new byte[] { 1, 2 }, new byte[] { 3, 4, 5 } });
        yield return Case("Array(Array(Int32))",
            new int[][] { new[] { 1, 2 }, new[] { 3, 4 }, new[] { 5 } });
        yield return Case("Array(Array(String))",
            new string[][] { new[] { "alpha", "beta" }, new[] { "gamma" } });
        yield return Case("Array(Array(String))",
            new string[][] { new[] { "a'b", "c\\d" }, new[] { "no escapes" } })
            .SetName("SelectEcho_JaggedString_WithQuotesAndBackslashes");
        yield return Case("Array(Array(Array(Int32)))",
            new int[][][]
            {
                new int[][] { new[] { 1, 2 }, new[] { 3 } },
                new int[][] { new[] { 4, 5, 6 } },
            });
        yield return Case("Array(Array(Int32))",
            new int[][] { new[] { 1, 2, 3 }, new[] { 4 }, new int[0], new[] { 5, 6 } })
            .SetName("SelectEcho_Jagged_Ragged_DoesNotRequireRectangularity");
        yield return Case("Array(Array(Nullable(Int32)))",
            new int?[][] { new int?[] { 1, null, 3 }, new int?[] { null }, new int?[] { 4, 5 } });
        yield return Case("Array(Array(Int32))",
            Array.Empty<int[]>())
            .SetName("SelectEcho_Jagged_EmptyOuter");
        yield return Case("Array(Array(Int32))",
            new int[][] { new int[0], new[] { 1, 2 }, new int[0] })
            .SetName("SelectEcho_Jagged_OuterWithEmptyInner");

        // ---- IList/List<> inputs — same wire format as jagged ----
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new List<List<int>> { new() { 1, 2 }, new() { 3, 4, 5 } },
            (object)new int[][] { new[] { 1, 2 }, new[] { 3, 4, 5 } })
            .SetName("SelectEcho_ListOfList");
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new List<int[]> { new[] { 10, 20 }, new[] { 30 } },
            (object)new int[][] { new[] { 10, 20 }, new[] { 30 } })
            .SetName("SelectEcho_ListOfArray");

        // ---- Inner reference types via jagged ----
        yield return Case("Array(Array(IPv4))",
            new IPAddress[][]
            {
                new[] { IPAddress.Parse("10.0.0.1"), IPAddress.Parse("10.0.0.2") },
                new[] { IPAddress.Parse("192.168.0.1") },
            });
        yield return Case("Array(Array(UUID))",
            new Guid[][]
            {
                new[] { Guid.Parse("11111111-1111-1111-1111-111111111111"), Guid.Parse("22222222-2222-2222-2222-222222222222") },
                new[] { Guid.Parse("33333333-3333-3333-3333-333333333333") },
            });
        yield return Case("Array(Array(DateTime))",
            new DateTime[][]
            {
                new[] { new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Unspecified), new DateTime(2026, 1, 2, 13, 30, 0, DateTimeKind.Unspecified) },
                new[] { new DateTime(2026, 6, 15, 9, 0, 0, DateTimeKind.Unspecified) },
            });
        yield return Case("Array(Array(FixedString(4)))",
            new string[][] { new[] { "abcd", "efgh" }, new[] { "wxyz" } });
        yield return Case("Array(Array(LowCardinality(String)))",
            new string[][] { new[] { "x", "y", "x" }, new[] { "z" } });
        yield return Case("Array(Array(Date))",
            new DateTime[][]
            {
                new[] { new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), new DateTime(2026, 12, 31, 0, 0, 0, DateTimeKind.Unspecified) },
                new[] { new DateTime(2020, 6, 15, 0, 0, 0, DateTimeKind.Unspecified) },
            });

        // ---- Rectangular multidim inputs → expected jagged form ----
        yield return new TestCaseData(
            "Array(Array(UInt8))",
            (object)new byte[,] { { 1, 2, 3 }, { 4, 5, 6 } },
            (object)new byte[][] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } })
            .SetName("SelectEcho_Multidim2D_UInt8");
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new int[,] { { 10, 20 }, { 30, 40 }, { 50, 60 } },
            (object)new int[][] { new[] { 10, 20 }, new[] { 30, 40 }, new[] { 50, 60 } })
            .SetName("SelectEcho_Multidim2D_Int32");
        yield return new TestCaseData(
            "Array(Array(String))",
            (object)new string[,] { { "a", "b" }, { "c", "d" } },
            (object)new string[][] { new[] { "a", "b" }, new[] { "c", "d" } })
            .SetName("SelectEcho_Multidim2D_String");
        yield return new TestCaseData(
            "Array(Array(Array(Int32)))",
            (object)new int[2, 2, 2]
            {
                { { 1, 2 }, { 3, 4 } },
                { { 5, 6 }, { 7, 8 } },
            },
            (object)new int[][][]
            {
                new int[][] { new[] { 1, 2 }, new[] { 3, 4 } },
                new int[][] { new[] { 5, 6 }, new[] { 7, 8 } },
            })
            .SetName("SelectEcho_Multidim3D_Int32");
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new int[0, 5],
            (object)Array.Empty<int[]>())
            .SetName("SelectEcho_MultidimEmptyOuter");
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new int[3, 0],
            (object)new int[][] { new int[0], new int[0], new int[0] })
            .SetName("SelectEcho_MultidimZeroInnerDim");

        // ---- Multidim with reference-type inner ----
        var guids = new Guid[2, 2];
        guids[0, 0] = Guid.Parse("11111111-1111-1111-1111-111111111111");
        guids[0, 1] = Guid.Parse("22222222-2222-2222-2222-222222222222");
        guids[1, 0] = Guid.Parse("33333333-3333-3333-3333-333333333333");
        guids[1, 1] = Guid.Parse("44444444-4444-4444-4444-444444444444");
        yield return new TestCaseData(
            "Array(Array(UUID))",
            (object)guids,
            (object)new Guid[][]
            {
                new[] { guids[0, 0], guids[0, 1] },
                new[] { guids[1, 0], guids[1, 1] },
            })
            .SetName("SelectEcho_MultidimGuid");

        var datetimes = new DateTime[1, 2]
        {
            { new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified), new DateTime(2026, 1, 2, 12, 30, 45, DateTimeKind.Unspecified) },
        };
        yield return new TestCaseData(
            "Array(Array(DateTime))",
            (object)datetimes,
            (object)new DateTime[][] { new[] { datetimes[0, 0], datetimes[0, 1] } })
            .SetName("SelectEcho_MultidimDateTime");

        yield return new TestCaseData(
            "Array(Array(Array(String)))",
            (object)new string[2, 1, 2]
            {
                { { "a", "b" } },
                { { "c", "d" } },
            },
            (object)new string[][][]
            {
                new string[][] { new[] { "a", "b" } },
                new string[][] { new[] { "c", "d" } },
            })
            .SetName("SelectEcho_Multidim3DString");

        // ---- 4D deeply-nested (jagged + multidim) ----
        yield return Case("Array(Array(Array(Array(Int32))))",
            new int[][][][]
            {
                new int[][][]
                {
                    new int[][] { new[] { 1, 2 } },
                    new int[][] { new[] { 3 }, new[] { 4, 5 } },
                },
            });
        yield return new TestCaseData(
            "Array(Array(Array(Array(Int32))))",
            (object)new int[1, 1, 1, 2] { { { { 7, 8 } } } },
            (object)new int[][][][] { new int[][][] { new int[][] { new[] { 7, 8 } } } })
            .SetName("SelectEcho_Multidim4D_Int32");

        // ---- Local helper: jagged inputs whose expected = input ----
        TestCaseData Case(string typeHint, object value) =>
            new TestCaseData(typeHint, value, value);
    }

    [Test]
    [TestCaseSource(nameof(SelectEchoCases))]
    public async Task SelectEcho_RoundTripsAsJagged(string typeHint, object input, object expectedJagged)
    {
        var result = await SelectEcho(connection, typeHint, input);
        Assert.That(result, Is.EqualTo(expectedJagged));
    }

    // ----- Cases too specialised to fit the source above -----

    [Test]
    public async Task ExecuteReaderAsync_LargerMultidim_RoundTrips()
    {
        // 20×30 explicit loop — exercises non-trivial leaf count without bloating the source table.
        const int rows = 20;
        const int cols = 30;
        var input = new int[rows, cols];
        for (var r = 0; r < rows; r++)
        {
            for (var c = 0; c < cols; c++)
            {
                input[r, c] = (r * cols) + c;
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

    [Test]
    public async Task ExecuteReaderAsync_JaggedDecimalArray_RoundTrips()
    {
        // Decimal returns as ClickHouseDecimal — needs explicit unwrap per cell.
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
                Assert.That((decimal)(ClickHouseDecimal)row.GetValue(j), Is.EqualTo(input[i][j]));
            }
        }
    }

    // Non-zero-bound coverage lives in MultiDimArrayHelperTests where the unit test asserts
    // byte-exact equality with the zero-bound equivalent — strictly stronger than a SELECT-echo.

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

    // ----- INSERT into table + read back: parameterised over (ddlType, value, expectedJagged) -----

    public static IEnumerable<TestCaseData> InsertReadbackCases()
    {
        yield return new TestCaseData(
            "Array(Array(Int32))",
            (object)new int[][] { new[] { 1, 2 }, new[] { 3, 4, 5 } },
            (object)new int[][] { new[] { 1, 2 }, new[] { 3, 4, 5 } })
            .SetName("InsertReadback_2DJagged_Int32");
        yield return new TestCaseData(
            "Array(Array(UInt8))",
            (object)new byte[,] { { 1, 2, 3 }, { 4, 5, 6 } },
            (object)new byte[][] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } })
            .SetName("InsertReadback_2DMultidim_UInt8");
        yield return new TestCaseData(
            "Array(Array(Array(Int32)))",
            (object)new int[][][]
            {
                new int[][] { new[] { 1, 2 }, new[] { 3 } },
                new int[][] { new[] { 4, 5, 6 } },
            },
            (object)new int[][][]
            {
                new int[][] { new[] { 1, 2 }, new[] { 3 } },
                new int[][] { new[] { 4, 5, 6 } },
            })
            .SetName("InsertReadback_3DJagged_Int32");
        yield return new TestCaseData(
            "Array(Array(Array(Int32)))",
            (object)new int[1, 2, 3] { { { 1, 2, 3 }, { 4, 5, 6 } } },
            (object)new int[][][] { new int[][] { new[] { 1, 2, 3 }, new[] { 4, 5, 6 } } })
            .SetName("InsertReadback_3DMultidim_Int32");
        yield return new TestCaseData(
            "Array(Array(Nullable(Int32)))",
            (object)new int?[][] { new int?[] { 1, null, 3 }, new int?[] { null, 5 } },
            (object)new int?[][] { new int?[] { 1, null, 3 }, new int?[] { null, 5 } })
            .SetName("InsertReadback_2DJagged_NullableInner");
    }

    [Test]
    [TestCaseSource(nameof(InsertReadbackCases))]
    public async Task InsertParameterizedIntoTable_ReadsBackAsJagged(string ddlType, object input, object expectedJagged)
    {
        var tableName = SanitizeTableName($"nested_insert_{Guid.NewGuid():N}");
        var fqn = $"test.{tableName}";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {fqn}");
        await connection.ExecuteStatementAsync($"CREATE TABLE {fqn} (arr {ddlType}) ENGINE Memory");

        try
        {
            using (var insert = connection.CreateCommand())
            {
                insert.CommandText = $"INSERT INTO {fqn} VALUES ({{values:{ddlType}}})";
                insert.AddParameter("values", input);
                await insert.ExecuteNonQueryAsync();
            }

            using var reader = await connection.ExecuteReaderAsync($"SELECT arr FROM {fqn}");
            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetValue(0), Is.EqualTo(expectedJagged));
        }
        finally
        {
            await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {fqn}");
        }
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
        // Shape-validation failure (data, not type) -> InvalidOperationException.
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
    public async Task GetFieldValueMultidim_NullColumn_ThrowsInvalidCastExceptionPerDbDataReaderContract()
    {
        // Type-mismatch case (null where T[,] expected) must match DbDataReader contract: InvalidCastException,
        // not InvalidOperationException. Mirrors how other typed getters like GetDateTime behave.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT CAST(NULL AS Nullable(Int32)) as result";
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int[,]>(0));
    }

    [Test]
    public async Task GetFieldValueMultidim_ScalarColumn_ThrowsInvalidCastExceptionPerDbDataReaderContract()
    {
        // A scalar (non-list) value asked for as T[,] is a type mismatch, not a shape problem.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 42 as result";
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int[,]>(0));
    }

    [Test]
    public async Task GetFieldValueMultidim_ShallowSource_ThrowsInvalidCastException()
    {
        // Server returns a 1D Array(Int32); caller asks for int[,]. The source's structural
        // depth doesn't match the target rank — type mismatch, not shape failure.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT [1, 2, 3]::Array(Int32) as result";
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var ex = Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int[,]>(0));
        Assert.That(ex!.Message, Does.Contain("[0]"));
        Assert.That(ex.Message, Does.Contain("Int32[,]"));
    }

    [Test]
    public async Task GetFieldValueMultidim_WrongLeafType_ThrowsInvalidCastException()
    {
        // Server returns a well-shaped Array(Array(String)); caller asks for int[,]. The leaf
        // mismatch must surface as InvalidCastException (not InvalidOperationException), with the
        // column ordinal and target CLR type in the message — that's the GetFieldValue<T>
        // contract this test pins.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT [['a','b'],['c','d']]::Array(Array(String)) as result";
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var ex = Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int[,]>(0));
        Assert.That(ex!.Message, Does.Contain("[0]"));
        Assert.That(ex.Message, Does.Contain("Int32[,]"));
    }

    [Test]
    public async Task GetFieldValueMultidim_NullLeafIntoValueTypeTarget_ThrowsInvalidCastException()
    {
        // Server returns a rectangular Array(Array(Nullable(Int32))) with a NULL leaf; caller
        // asks for a non-nullable int[,]. Null leaf into a value-type slot must surface as
        // InvalidCastException with the ordinal and target type in the message.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT [[1, NULL], [3, 4]]::Array(Array(Nullable(Int32))) as result";
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var ex = Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int[,]>(0));
        Assert.That(ex!.Message, Does.Contain("[0]"));
        Assert.That(ex.Message, Does.Contain("Int32[,]"));
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

    // Rank-mismatch message contracts are covered directly against ResolveLeafType
    // in MultiDimArrayHelperTests; the SQL hop adds no signal.
}
