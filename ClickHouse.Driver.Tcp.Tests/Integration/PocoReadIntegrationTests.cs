using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Real-server coverage for <c>QueryAsync&lt;T&gt;</c>, including every round-trip corpus type and POCO-specific
/// projections, mapping and lifetime behavior.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class PocoReadIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly IReadOnlyDictionary<string, string> TimeSettings = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["enable_time_time64_type"] = "1",
        ["allow_experimental_time_time64_type"] = "1",
    };

    [TestCaseSource(typeof(InsertRoundTripCase), nameof(InsertRoundTripCase.Cases))]
    public async Task QueryAsync_EveryCorpusType_MaterializesTheColumnIntoTheProperty(InsertRoundTripCase testCase)
    {
        TcpServerFixture.SkipIfCloudLocksASetting(testCase.Settings);

        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = testCase.Settings };
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (value {testCase.ClickHouseType}) ENGINE = Memory", options, None);

            IColumn insert = testCase.BuildInsertColumn("value");
            await client.InsertAsync(
                $"INSERT INTO {table} (value) VALUES",
                new[] { insert },
                new ClickHouseTcpInsertOptions { Settings = testCase.Settings },
                None);

            IColumn expected = testCase.BuildExpectedColumn("value");
            object[] read = await ReadColumnAsRowsAsync(client, $"SELECT value FROM {table}", options, ElementTypeOf(expected));

            Assert.That(read, Has.Length.EqualTo(expected.RowCount), "row count");
            for (int row = 0; row < expected.RowCount; row++)
            {
                Assert.That(read[row], Is.EqualTo(expected.GetValue(row)), $"row {row}");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task QueryAsync_OnePocoWithDeepCompositeColumns_MaterializesEveryProperty()
    {
        // The corpus proves these codecs and their constituent shapes one column at a time through Row<T>. This puts
        // deeper compositions together in one plan, with independent offsets, state prefixes and null sentinels.
        const string deepBytesType = "Array(Array(Array(Array(Array(UInt8)))))";
        const string nullableDeepType = "Array(Array(Array(Nullable(UInt32))))";
        const string mixedTupleType = "Tuple(Array(Nullable(Int32)), Map(String, Array(UInt16)), Tuple())";
        const string mapOfTupleArraysType = "Map(String, Array(Tuple(Nullable(Int32), Array(String))))";
        const string arrayOfMapsType = "Array(Map(String, Array(UInt64)))";
        const string variantArrayType = "Array(Variant(Array(UInt64), String))";
        const string dynamicTupleType = "Tuple(Dynamic, Array(Tuple(Dynamic, String)))";
        const string nestedRecordsType = "Nested(label String, values Array(Nullable(Int32)), marker Tuple(UInt8, String))";

        byte[][][][][][] deepBytes =
        {
            new byte[][][][][]
            {
                new byte[][][][]
                {
                    new byte[][][]
                    {
                        new byte[][] { new byte[] { 1, 2 }, Array.Empty<byte>() },
                        Array.Empty<byte[]>(),
                    },
                    Array.Empty<byte[][]>(),
                },
                Array.Empty<byte[][][]>(),
            },
            Array.Empty<byte[][][][]>(),
            new byte[][][][][]
            {
                new byte[][][][]
                {
                    new byte[][][]
                    {
                        new byte[][] { Array.Empty<byte>(), new byte[] { 3, 4, 5 } },
                    },
                },
            },
        };

        uint?[][][][] nullableDeep =
        {
            new uint?[][][]
            {
                new uint?[][] { new uint?[] { 1, null }, Array.Empty<uint?>() },
                Array.Empty<uint?[]>(),
            },
            Array.Empty<uint?[][]>(),
            new uint?[][][]
            {
                new uint?[][] { new uint?[] { null, uint.MaxValue } },
            },
        };

        (int?[] Numbers, KeyValuePair<string, ushort[]>[] Lookup, ValueTuple Empty)[] mixedTuples =
        {
            (
                new int?[] { 1, null },
                Pairs<string, ushort[]>(("filled", new ushort[] { 1, ushort.MaxValue }), ("empty", Array.Empty<ushort>())),
                default),
            (Array.Empty<int?>(), Array.Empty<KeyValuePair<string, ushort[]>>(), default),
            (
                new int?[] { int.MinValue, null },
                Pairs<string, ushort[]>(("zero", new ushort[] { 0 })),
                default),
        };

        KeyValuePair<string, (int?, string[])[]>[][] mapOfTupleArrays =
        {
            Pairs<string, (int?, string[])[]>(
                ("filled", new (int?, string[])[] { (1, new[] { "a", string.Empty }), (null, Array.Empty<string>()) }),
                ("empty", Array.Empty<(int?, string[])>())),
            Array.Empty<KeyValuePair<string, (int?, string[])[]>>(),
            Pairs<string, (int?, string[])[]>(
                ("unicode", new (int?, string[])[] { (null, new[] { "héllo✓" }) })),
        };

        KeyValuePair<string, ulong[]>[][][] arrayOfMaps =
        {
            new KeyValuePair<string, ulong[]>[][]
            {
                Pairs<string, ulong[]>(("a", new ulong[] { 1, 2 })),
                Array.Empty<KeyValuePair<string, ulong[]>>(),
            },
            Array.Empty<KeyValuePair<string, ulong[]>[]>(),
            new KeyValuePair<string, ulong[]>[][]
            {
                Pairs<string, ulong[]>(("empty", Array.Empty<ulong>()), ("max", new[] { ulong.MaxValue })),
            },
        };

        object[][] variantArrays =
        {
            new object[] { new ulong[] { 1, 2 }, "x", null, Array.Empty<ulong>() },
            Array.Empty<object>(),
            new object[] { string.Empty, null, new[] { ulong.MaxValue } },
        };

        (object Head, (object Value, string Label)[] Tail)[] dynamicTuples =
        {
            (42UL, new (object, string)[] { ("inner", "text"), (null, string.Empty) }),
            (null, Array.Empty<(object, string)>()),
            (
                Pairs<string, uint>(("key", 7)),
                new (object, string)[] { (new ulong[] { 1, 2 }, "array"), (7UL, "number") }),
        };

        object[][][] nestedRecords =
        {
            new object[][]
            {
                new object[] { "first", new int?[] { 1, null }, ((byte)1, "a") },
                new object[] { "second", Array.Empty<int?>(), ((byte)2, string.Empty) },
            },
            Array.Empty<object[]>(),
            new object[][]
            {
                new object[] { "third", new int?[] { null, int.MinValue }, ((byte)3, "héllo✓") },
            },
        };

        IColumn[] columns =
        {
            PrimitiveColumn<byte>.FromValues("Id", "UInt8", new byte[] { 0, 1, 2 }),
            new ArrayColumn<byte[][][][][]>("DeepBytes", deepBytesType, deepBytes),
            new ArrayColumn<uint?[][][]>("NullableDeep", nullableDeepType, nullableDeep),
            new ArrayColumn<(int?[], KeyValuePair<string, ushort[]>[], ValueTuple)>("MixedTuple", mixedTupleType, mixedTuples),
            new ArrayColumn<KeyValuePair<string, (int?, string[])[]>[]>("MapOfTupleArrays", mapOfTupleArraysType, mapOfTupleArrays),
            new ArrayColumn<KeyValuePair<string, ulong[]>[][]>("ArrayOfMaps", arrayOfMapsType, arrayOfMaps),
            new ArrayColumn<object[]>("VariantArray", variantArrayType, variantArrays),
            new ArrayColumn<(object, (object, string)[])>("DynamicTuple", dynamicTupleType, dynamicTuples),
            new NestedColumn(
                "NestedRecords",
                nestedRecordsType,
                new[] { "label", "values", "marker" },
                new IColumn[]
                {
                    new ArrayColumn<string>("NestedRecords", "String", new[] { "first", "second", "third" }),
                    new ArrayColumn<int?[]>("NestedRecords", "Array(Nullable(Int32))", new[]
                    {
                        new int?[] { 1, null },
                        Array.Empty<int?>(),
                        new int?[] { null, int.MinValue },
                    }),
                    new ArrayColumn<(byte, string)>("NestedRecords", "Tuple(UInt8, String)", new[]
                    {
                        ((byte)1, "a"),
                        ((byte)2, string.Empty),
                        ((byte)3, "héllo✓"),
                    }),
                },
                new[] { 0, 2, 2, 3 },
                rowCount: 3,
                pooledOffsets: false,
                ownsFields: false),
        };

        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["flatten_nested"] = "0",
            ["allow_experimental_variant_type"] = "1",
            ["allow_suspicious_variant_types"] = "1",
            ["allow_experimental_dynamic_type"] = "1",
            ["output_format_native_use_flattened_dynamic_and_json_serialization"] = "1",
        };
        var queryOptions = new ClickHouseTcpQueryOptions { Settings = settings };
        var insertOptions = new ClickHouseTcpInsertOptions { Settings = settings };
        string table = UniqueTableName();
        string schema = string.Join(", ", new[]
        {
            "Id UInt8",
            $"DeepBytes {deepBytesType}",
            $"NullableDeep {nullableDeepType}",
            $"MixedTuple {mixedTupleType}",
            $"MapOfTupleArrays {mapOfTupleArraysType}",
            $"ArrayOfMaps {arrayOfMapsType}",
            $"VariantArray {variantArrayType}",
            $"DynamicTuple {dynamicTupleType}",
            $"NestedRecords {nestedRecordsType}",
        });

        await using var client = TcpServerFixture.CreateClient();
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} ({schema}) ENGINE = Memory", queryOptions, None);
            await client.InsertAsync(
                $"INSERT INTO {table} (Id, DeepBytes, NullableDeep, MixedTuple, MapOfTupleArrays, ArrayOfMaps, VariantArray, DynamicTuple, NestedRecords) VALUES",
                columns,
                insertOptions,
                None);

            List<CompositeStressRow> rows = await client
                .QueryAsync<CompositeStressRow>($"SELECT * FROM {table} ORDER BY Id", queryOptions, None)
                .ToListAsync();

            Assert.That(rows, Has.Count.EqualTo(3));
            Assert.Multiple(() =>
            {
                for (int row = 0; row < rows.Count; row++)
                {
                    Assert.That(rows[row].Id, Is.EqualTo((byte)row), $"row {row}: Id");
                    Assert.That(rows[row].DeepBytes, Is.EqualTo(deepBytes[row]), $"row {row}: DeepBytes");
                    Assert.That(rows[row].NullableDeep, Is.EqualTo(nullableDeep[row]), $"row {row}: NullableDeep");
                    Assert.That(rows[row].MixedTuple.Item1, Is.EqualTo(mixedTuples[row].Numbers), $"row {row}: MixedTuple.Item1");
                    Assert.That(rows[row].MixedTuple.Item2, Is.EqualTo(mixedTuples[row].Lookup), $"row {row}: MixedTuple.Item2");
                    Assert.That(rows[row].MixedTuple.Item3, Is.EqualTo(mixedTuples[row].Empty), $"row {row}: MixedTuple.Item3");
                    Assert.That(rows[row].MapOfTupleArrays, Is.EqualTo(mapOfTupleArrays[row]), $"row {row}: MapOfTupleArrays");
                    Assert.That(rows[row].ArrayOfMaps, Is.EqualTo(arrayOfMaps[row]), $"row {row}: ArrayOfMaps");
                    Assert.That(rows[row].VariantArray, Is.EqualTo(variantArrays[row]), $"row {row}: VariantArray");
                    Assert.That(rows[row].DynamicTuple.Item1, Is.EqualTo(dynamicTuples[row].Head), $"row {row}: DynamicTuple.Item1");
                    Assert.That(rows[row].DynamicTuple.Item2, Is.EqualTo(dynamicTuples[row].Tail), $"row {row}: DynamicTuple.Item2");
                    Assert.That(rows[row].NestedRecords, Is.EqualTo(nestedRecords[row]), $"row {row}: NestedRecords");
                }
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task QueryAsync_CalendarAndEnumProperties_ReadTheProjectionsNotTheWireValues()
    {
        // The corpus reads each of these columns as its raw wire value (epoch seconds, a scaled count, an ordinal),
        // which is a different assertion from the one a POCO makes: here every property asks for the reading a
        // caller would actually declare.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions { Settings = TimeSettings };
        string table = UniqueTableName();
        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (Stamp DateTime('UTC'), Precise DateTime64(3, 'UTC'), Day Date, Clock Time, Fine Time64(3), Level Enum8('low' = -1, 'high' = 127)) ENGINE = Memory",
                options,
                None);

            var stamp = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc);
            DateTimeOffset precise = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);
            var day = new DateOnly(2024, 1, 15);
            var clock = new TimeSpan(12, 34, 56);
            var fine = new TimeSpan(0, 1, 2, 3, 456);

            IColumn[] columns =
            {
                new ArrayColumn<DateTime>("Stamp", "DateTime('UTC')", new[] { stamp }),
                new ArrayColumn<DateTimeOffset>("Precise", "DateTime64(3, 'UTC')", new[] { precise }),
                new ArrayColumn<DateOnly>("Day", "Date", new[] { day }),
                new ArrayColumn<TimeSpan>("Clock", "Time", new[] { clock }),
                new ArrayColumn<TimeSpan>("Fine", "Time64(3)", new[] { fine }),
                PrimitiveColumn<sbyte>.FromValues("Level", "Enum8('low' = -1, 'high' = 127)", new sbyte[] { 127 }),
            };

            await client.InsertAsync(
                $"INSERT INTO {table} (Stamp, Precise, Day, Clock, Fine, Level) VALUES",
                columns,
                new ClickHouseTcpInsertOptions { Settings = TimeSettings },
                None);

            List<CalendarRow> rows = await client.QueryAsync<CalendarRow>($"SELECT * FROM {table}", options, None).ToListAsync();

            Assert.That(rows, Has.Count.EqualTo(1));
            Assert.Multiple(() =>
            {
                Assert.That(rows[0].Stamp, Is.EqualTo(stamp));
                Assert.That(rows[0].Stamp.Kind, Is.EqualTo(DateTimeKind.Utc), "a zero offset presents as UTC");
                Assert.That(rows[0].Precise, Is.EqualTo(precise));
                Assert.That(rows[0].Day, Is.EqualTo(day));
                Assert.That(rows[0].Clock, Is.EqualTo(clock));
                Assert.That(rows[0].Fine, Is.EqualTo(fine));
                Assert.That(rows[0].Level, Is.EqualTo(Level.High));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task QueryAsync_TimezoneLessDateTimeColumn_PresentsTheSessionTimezoneWallClock()
    {
        // D7: a bare DateTime column is presented in the session timezone, so a DateTime property carries that wall
        // clock (as Unspecified, there being no offset to attach) while a DateTimeOffset property carries the offset.
        // This is a deliberate difference from the HTTP client, which has no session timezone to resolve.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["session_timezone"] = "Asia/Kolkata" },
        };

        List<InstantRow> rows = await client
            .QueryAsync<InstantRow>("SELECT toDateTime(1700000000) AS Stamp, toDateTime(1700000000) AS Offset", options, None)
            .ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Offset.ToUnixTimeSeconds(), Is.EqualTo(1_700_000_000));
            Assert.That(rows[0].Offset.Offset, Is.EqualTo(new TimeSpan(5, 30, 0)));
            Assert.That(rows[0].Stamp, Is.EqualTo(rows[0].Offset.DateTime));
            Assert.That(rows[0].Stamp.Kind, Is.EqualTo(DateTimeKind.Unspecified));
        });
    }

    [Test]
    public async Task QueryAsync_ResultSpanningSeveralBlocks_KeepsEveryRowValidAfterItsBlock()
    {
        // The rows are accumulated and asserted after the enumeration ends, which is the ownership contract: every
        // value a column surfaces is a copy, so a row outlives the borrowed block it came from. The row count is
        // well past one block, so the plan is also reused across blocks rather than rebuilt.
        const int rowCount = 200_000;
        await using var client = TcpServerFixture.CreateClient();

        List<Numbered> rows = await client
            .QueryAsync<Numbered>($"SELECT number AS Id, toString(number) AS Name FROM numbers({rowCount})", cancellationToken: None)
            .ToListAsync();

        Assert.That(rows, Has.Count.EqualTo(rowCount));
        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Id, Is.EqualTo(0ul));
            Assert.That(rows[0].Name, Is.EqualTo("0"));
            Assert.That(rows[rowCount - 1].Id, Is.EqualTo((ulong)(rowCount - 1)));
            Assert.That(rows[rowCount - 1].Name, Is.EqualTo((rowCount - 1).ToString()));
        });
    }

    [Test]
    public async Task QueryAsync_BlockLargerThanTheMaterializationWindow_YieldsEveryRowOnceInOrder()
    {
        // A block is materialized a window of rows at a time, not all at once, so a block that spans several
        // windows is the case where a window could drop, repeat or misorder rows. The multi-block test above cannot
        // reach it: there, every block boundary is also a window boundary.
        const int rowCount = 5_000;
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                // One block holding every row: the row count is under max_block_size, and the byte-based cap that
                // would otherwise split it first is disabled.
                ["max_block_size"] = "8192",
                ["preferred_block_size_bytes"] = "0",
                ["max_threads"] = "1",
            },
        };

        string sql = $"SELECT number AS Id, toString(number) AS Name FROM numbers({rowCount})";
        await using var client = TcpServerFixture.CreateClient();

        // The premise, asserted rather than assumed: if the server ever splits this differently, the row assertions
        // below would still pass while no longer testing a window boundary inside a block.
        int blocks = 0;
        await foreach (Block _ in client.StreamAsync(sql, options, None))
        {
            blocks++;
        }

        List<Numbered> rows = await client.QueryAsync<Numbered>(sql, options, None).ToListAsync();

        Assert.That(blocks, Is.EqualTo(1), "the query has to produce one block for this to test a window boundary");
        Assert.That(rows, Has.Count.EqualTo(rowCount));
        Assert.Multiple(() =>
        {
            for (int i = 0; i < rowCount; i++)
            {
                Assert.That(rows[i].Id, Is.EqualTo((ulong)i), $"Id at row {i}");
                Assert.That(rows[i].Name, Is.EqualTo(i.ToString(CultureInfo.InvariantCulture)), $"Name at row {i}");
            }
        });
    }

    [Test]
    public async Task QueryAsync_NullPastTheFirstWindow_NamesTheRowOfTheResult()
    {
        // The row a failure names is counted across the whole result. Reported from inside a window, so the offset
        // of the window within its block has to be carried into the message.
        const int nullAtRow = 1_500;
        await using var client = TcpServerFixture.CreateClient();

        InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(async () => await client
            .QueryAsync<Numbered>(
                $"SELECT if(number = {nullAtRow}, NULL, number) AS Id FROM numbers(3000)",
                cancellationToken: None)
            .ToListAsync());

        Assert.That(error.Message, Does.Contain($"row {nullAtRow}").And.Contain("Numbered.Id"));
    }

    [Test]
    public async Task QueryAsync_ColumnWithNoPropertyAndPropertyWithNoColumn_SkipsOneAndDefaultsTheOther()
    {
        await using var client = TcpServerFixture.CreateClient();

        List<Numbered> rows = await client
            .QueryAsync<Numbered>("SELECT toUInt64(7) AS Id, 'ignored' AS Untouched", cancellationToken: None)
            .ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Id, Is.EqualTo(7ul));
            Assert.That(rows[0].Name, Is.Null, "no column maps to Name");
        });
    }

    [Test]
    public async Task QueryAsync_RenamedAndNotMappedProperties_HonorTheAttributes()
    {
        await using var client = TcpServerFixture.CreateClient();

        List<AttributedRow> rows = await client
            .QueryAsync<AttributedRow>("SELECT toDateTime(1700000000, 'UTC') AS event_time, 'x' AS Ignored", cancellationToken: None)
            .ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].Timestamp, Is.EqualTo(DateTime.UnixEpoch.AddSeconds(1_700_000_000)));
            Assert.That(rows[0].Ignored, Is.Null, "[ClickHouseTcpNotMapped] keeps the column from reaching the property");
        });
    }

    [Test]
    public async Task QueryAsync_ResultWithNoRows_YieldsNothing()
    {
        // A result with no rows carries no block at all (the connection drops zero-row blocks), so there is no header
        // to compile a plan from: the sequence is simply empty, and nothing about T is validated.
        await using var client = TcpServerFixture.CreateClient();

        List<Numbered> rows = await client
            .QueryAsync<Numbered>("SELECT toUInt64(1) AS Id FROM numbers(1) WHERE 0", cancellationToken: None)
            .ToListAsync();

        Assert.That(rows, Is.Empty);
    }

    [Test]
    public async Task QueryAsync_EnumerationAbandonedEarly_LeavesTheClientUsable()
    {
        // Stopping mid-result has to release both the pooled row array and the connection; the second query is what
        // proves the release happened, since a leaked connection would leave the client unable to run another query.
        await using var client = TcpServerFixture.CreateClient();

        var seen = new List<ulong>();
        await foreach (Numbered row in client.QueryAsync<Numbered>("SELECT number AS Id FROM numbers(200000)", cancellationToken: None))
        {
            seen.Add(row.Id);
            if (seen.Count == 5)
            {
                break;
            }
        }

        List<Numbered> after = await client.QueryAsync<Numbered>("SELECT toUInt64(7) AS Id", cancellationToken: None).ToListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(new ulong[] { 0, 1, 2, 3, 4 }));
            Assert.That(after[0].Id, Is.EqualTo(7ul));
        });
    }

    [Test]
    public async Task QueryAsync_ColumnNameHoldingThePlanCacheSeparators_MapsToItsProperty()
    {
        // The premise of the plan cache's key: a column name is arbitrary text, tabs and newlines included, so a
        // quoted alias really can spell what a naively joined key uses as its separators.
        await using var client = TcpServerFixture.CreateClient();

        List<SeparatorRow> rows = await client
            .QueryAsync<SeparatorRow>("SELECT toInt32(42) AS `a\tb\nc`", cancellationToken: None)
            .ToListAsync();

        Assert.That(rows[0].Value, Is.EqualTo(42));
    }

    [Test]
    public async Task QueryAsync_PropertyTheColumnCannotBeReadAs_ThrowsBeforeTheFirstRow()
    {
        // The plan is compiled from the result's first block, so the failure arrives on the first MoveNext rather
        // than part-way through the rows.
        await using var client = TcpServerFixture.CreateClient();

        InvalidOperationException error = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await client.QueryAsync<Row<Guid>>("SELECT toInt32(1) AS value", cancellationToken: None).ToListAsync());

        Assert.That(error.Message, Does.Contain("Int32").And.Contain("System.Guid"));
    }

    [Test]
    public async Task Materialize_EveryScatterTier_ReadsTheSameRows()
    {
        // The tiers are compared here as well as in the unit tests because these values come off a real server: the
        // span tier's zero-copy read and the per-row tier have to agree on decoded storage, not just on columns a
        // test built. QueryAsync<T> leaves the tier to the runtime, so the plan is built directly to name one; each
        // tier reads its own block, since materializing one block twice would let the first tier fill the caches
        // the second reads.
        const string sql = "SELECT toUInt64(number) AS Id, toString(number) AS Name FROM numbers(3)";
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options());
        var byTier = new Dictionary<PocoScatterTier, List<Numbered>>();

        foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
        {
            var read = new List<Numbered>();
            await foreach (Block block in client.StreamAsync(sql, cancellationToken: None))
            {
                var rows = new Numbered[block.RowCount];
                PocoReadPlan<Numbered>.Build(PocoTypeDescriptor<Numbered>.Build(), block, tier)
                    .Materialize(block, rows, read.Count);
                read.AddRange(rows);
            }

            byTier[tier] = read;
        }

        Assert.Multiple(() =>
        {
            foreach ((PocoScatterTier tier, List<Numbered> rows) in byTier)
            {
                Assert.That(rows.ConvertAll(row => row.Id), Is.EqualTo(new ulong[] { 0, 1, 2 }), $"{tier}: Id");
                Assert.That(rows.ConvertAll(row => row.Name), Is.EqualTo(new[] { "0", "1", "2" }), $"{tier}: Name");
            }
        });
    }

    /// <summary>
    /// Reads a one-column result into <c>Row&lt;TValue&gt;</c> for a CLR type only known at runtime, which is what
    /// lets the corpus drive the POCO path: each case's expected column names the type its rows come back as.
    /// </summary>
    /// <param name="client">The client to query with.</param>
    /// <param name="sql">The one-column query.</param>
    /// <param name="options">The per-query options the case needs.</param>
    /// <param name="valueType">The CLR type of the column's values.</param>
    /// <returns>Each row's value, in row order.</returns>
    private static Task<object[]> ReadColumnAsRowsAsync(IClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options, Type valueType)
    {
        MethodInfo reader = typeof(PocoReadIntegrationTests)
            .GetMethod(nameof(ReadRowsAsync), BindingFlags.NonPublic | BindingFlags.Static)
            .MakeGenericMethod(valueType);

        return (Task<object[]>)reader.Invoke(null, new object[] { client, sql, options });
    }

    private static async Task<object[]> ReadRowsAsync<TValue>(IClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options)
    {
        var values = new List<object>();
        await foreach (Row<TValue> row in client.QueryAsync<Row<TValue>>(sql, options, None))
        {
            values.Add(row.Value);
        }

        return values.ToArray();
    }

    /// <summary>The <c>T</c> of the <see cref="IColumn{T}"/> a column surfaces.</summary>
    /// <param name="column">The column.</param>
    /// <returns>Its CLR element type.</returns>
    private static Type ElementTypeOf(IColumn column)
    {
        foreach (Type implemented in column.GetType().GetInterfaces())
        {
            if (implemented.IsGenericType && implemented.GetGenericTypeDefinition() == typeof(IColumn<>))
            {
                return implemented.GetGenericArguments()[0];
            }
        }

        throw new InvalidOperationException($"Column '{column.Name}' ({column.TypeName}) surfaces no IColumn<T>.");
    }

    private static KeyValuePair<TKey, TValue>[] Pairs<TKey, TValue>(params (TKey Key, TValue Value)[] pairs)
    {
        var result = new KeyValuePair<TKey, TValue>[pairs.Length];
        for (int i = 0; i < pairs.Length; i++)
        {
            result[i] = new KeyValuePair<TKey, TValue>(pairs[i].Key, pairs[i].Value);
        }

        return result;
    }

    private static string UniqueTableName() => $"tcp_poco_test_{Guid.NewGuid():N}";

    private enum Level : sbyte
    {
        Low = -1,
        High = 127,
    }

    private sealed class Numbered
    {
        public ulong Id { get; set; }

        public string Name { get; set; }
    }

    private sealed class CalendarRow
    {
        public DateTime Stamp { get; set; }

        public DateTimeOffset Precise { get; set; }

        public DateOnly Day { get; set; }

        public TimeSpan Clock { get; set; }

        public TimeSpan Fine { get; set; }

        public Level Level { get; set; }
    }

    private sealed class InstantRow
    {
        public DateTime Stamp { get; set; }

        public DateTimeOffset Offset { get; set; }
    }

    private sealed class CompositeStressRow
    {
        public byte Id { get; set; }

        public byte[][][][][] DeepBytes { get; set; }

        public uint?[][][] NullableDeep { get; set; }

        public (int?[], KeyValuePair<string, ushort[]>[], ValueTuple) MixedTuple { get; set; }

        public KeyValuePair<string, (int?, string[])[]>[] MapOfTupleArrays { get; set; }

        public KeyValuePair<string, ulong[]>[][] ArrayOfMaps { get; set; }

        public object[] VariantArray { get; set; }

        public (object, (object, string)[]) DynamicTuple { get; set; }

        public object[][] NestedRecords { get; set; }
    }

    private sealed class SeparatorRow
    {
        [ClickHouseTcpColumn(Name = "a\tb\nc")]
        public int Value { get; set; }
    }

    private sealed class AttributedRow
    {
        [ClickHouseTcpColumn(Name = "event_time")]
        public DateTime Timestamp { get; set; }

        [ClickHouseTcpNotMapped]
        public string Ignored { get; set; }
    }
}
