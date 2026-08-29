using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The three types whose value is not decided by the type string: <c>Variant(T1, ..., Tn)</c>, <c>Dynamic</c> and
/// <c>JSON</c>.
///
/// <para>
/// <c>Variant</c> and <c>Dynamic</c> are discriminated unions. Both read as <c>IColumn&lt;object&gt;</c>, which
/// loses the static type and boxes every row whose alternative is a value type, and both expose a columnar view
/// instead — a per-row discriminator plus one typed child column per alternative. They differ in where the
/// alternative list comes from: a <c>Variant</c> declares it in the type string, a <c>Dynamic</c> discovers it per
/// block and reports it as
/// <see cref="IDynamicColumn.TypeNames"/>. They also differ in how NULL is marked, which is the one detail that
/// will bite you.
/// </para>
///
/// <para>
/// <c>JSON</c> is a different problem. This client reads and writes it only in the String serialization
/// (version 1), so a value is its compact JSON text. That works in both directions, but the server <b>parses</b>
/// what you write into real paths and re-renders on the way out, so the text you get back is not the text you
/// sent. Section 5 shows exactly what changes.
/// </para>
/// </summary>
public static class TcpVariantDynamicJson
{
    private const string VariantTable = "example_tcp_variant";
    private const string DynamicTable = "example_tcp_dynamic";
    private const string JsonTable = "example_tcp_json";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await Seed(client);
            await Variants(client);
            await Dynamics(client);
            await TheTwoCompared(client);
            await JsonIsText(client);
            await JsonNormalization(client);
        }
        finally
        {
            foreach (string table in new[] { VariantTable, DynamicTable, JsonTable })
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }

            Console.WriteLine("\nDropped every table this example created.");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {VariantTable}");
        await client.ExecuteAsync($@"
            CREATE TABLE {VariantTable} (id UInt64, v Variant(String, UInt64, Array(Int32)))
            ENGINE = MergeTree() ORDER BY id");

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {DynamicTable}");
        await client.ExecuteAsync($@"
            CREATE TABLE {DynamicTable} (id UInt64, d Dynamic)
            ENGINE = MergeTree() ORDER BY id");

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {JsonTable}");
        await client.ExecuteAsync($@"
            CREATE TABLE {JsonTable} (id UInt64, doc JSON)
            ENGINE = MergeTree() ORDER BY id");

        // Both union types are written from an IColumn<object>: one row per value, of whichever CLR type the
        // chosen alternative takes, and null for a NULL row.
        await client.InsertAsync(
            $"INSERT INTO {VariantTable} (id, v) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2, 3, 4, 5 }),
                ClickHouseTcpColumn.Create("v", new object?[] { 42UL, "hi", null, new[] { 1, 2 }, 7UL }),
            });

        await client.InsertAsync(
            $"INSERT INTO {DynamicTable} (id, d) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2, 3, 4 }),
                ClickHouseTcpColumn.Create("d", new object?[] { 42UL, "hi", null, 1.5 }),
            });

        Console.WriteLine($"Seeded '{VariantTable}' (5 rows), '{DynamicTable}' (4 rows) and '{JsonTable}'.");
    }

    private static async Task Variants(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. Variant: the alternatives are declared, and the server sorts them\n");

        await foreach (Block block in client.StreamAsync($"SELECT v FROM {VariantTable} ORDER BY id"))
        {
            IColumn column = block["v"];
            Console.WriteLine($"   Declared as Variant(String, UInt64, Array(Int32))");
            Console.WriteLine($"   Header says {column.TypeName}");
            Console.WriteLine("   The server canonicalizes the alternatives into name-sorted order, and that order is");
            Console.WriteLine("   the discriminator order. So read it from TypeName, never from what you declared.\n");

            if (column is IVariantColumn variant)
            {
                Console.WriteLine($"     TypeCount                     {variant.TypeCount}");
                Console.WriteLine($"     Discriminators                [{string.Join(", ", variant.Discriminators.ToArray())}]   one byte per row");
                Console.WriteLine($"     LocalIndices                  [{string.Join(", ", variant.LocalIndices.ToArray())}]   -1 for a NULL row");
                Console.WriteLine($"     IVariantColumn.NullDiscriminator = {IVariantColumn.NullDiscriminator}   a fixed sentinel, not TypeCount");
                Console.WriteLine();
                Console.WriteLine("     One child column per alternative, holding only the rows that chose it:");

                for (int discriminator = 0; discriminator < variant.TypeCount; discriminator++)
                {
                    IColumn child = variant.GetTypeColumn(discriminator);
                    Console.WriteLine($"       {discriminator} {child.TypeName,-16} {child.RowCount} row(s)");
                }

                Console.WriteLine();
                Console.WriteLine("     Row i's value is GetTypeColumn(Discriminators[i])[LocalIndices[i]], so dispatch");
                Console.WriteLine("     once per alternative and read the child typed rather than boxed:");
                Console.WriteLine();

                // The typed children are bound once, outside the row loop. Nothing here boxes.
                var strings = (IColumn<string>)variant.GetTypeColumn(1);
                var numbers = (IColumn<ulong>)variant.GetTypeColumn(2);
                var lists = (IColumn<int[]>)variant.GetTypeColumn(0);
                ReadOnlySpan<byte> discriminators = variant.Discriminators;
                ReadOnlySpan<int> local = variant.LocalIndices;

                for (int row = 0; row < column.RowCount; row++)
                {
                    byte discriminator = discriminators[row];
                    string reading = discriminator == IVariantColumn.NullDiscriminator
                        ? "NULL"
                        : discriminator switch
                        {
                            0 => $"Array(Int32) [{string.Join(", ", lists[local[row]])}]",
                            1 => $"String '{strings[local[row]]}'",
                            2 => $"UInt64 {numbers[local[row]]}",
                            _ => "?",
                        };

                    Console.WriteLine($"       row {row}: discriminator {discriminator,3}, local {local[row],2} -> {reading}");
                }

                Console.WriteLine();
                Console.WriteLine("     Passing NullDiscriminator to GetTypeColumn throws — it selects no column — so");
                Console.WriteLine("     guard for it before the call, as the loop above does.");

                try
                {
                    _ = variant.GetTypeColumn(IVariantColumn.NullDiscriminator);
                }
                catch (IndexOutOfRangeException)
                {
                    Console.WriteLine($"       GetTypeColumn({IVariantColumn.NullDiscriminator}) -> IndexOutOfRangeException");
                }
            }

            Console.WriteLine();
            Console.WriteLine($"   The materialized surface is IColumn<object>: ElementType is {column.ElementType.Name}, so");
            Console.WriteLine("   the static type is gone and a value-type alternative is boxed. A String or an Array is");
            Console.WriteLine("   already a reference, so it costs nothing beyond the object[] the caller sees:");
            for (int row = 0; row < column.RowCount; row++)
            {
                object? value = column.GetValue(row);
                Console.WriteLine($"     GetValue({row}) -> {(value is null ? "null" : $"{Describe(value.GetType())} {Render(value)}")}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   The server can tell you the same thing in SQL, which is worth knowing for a query you");
        Console.WriteLine("   are debugging:");

        await foreach (object[] row in client.QueryAsync(
            $"SELECT id, variantType(v) FROM {VariantTable} ORDER BY id"))
        {
            Console.WriteLine($"     row {row[0]}: variantType(v) ordinal {row[1]}");
        }

        Console.WriteLine("     variantType returns an Enum8 whose type string spells the whole mapping — and which");
        Console.WriteLine("     this client reads as the bare ordinal, as Tcp_011 section 6 explains.");
    }

    private static async Task Dynamics(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. Dynamic: the alternatives are discovered, and they are named\n");

        await foreach (Block block in client.StreamAsync($"SELECT d FROM {DynamicTable} ORDER BY id"))
        {
            IColumn column = block["d"];
            Console.WriteLine($"   {column.TypeName} — the type string says nothing about what is in it.\n");

            if (column is IDynamicColumn dynamicColumn)
            {
                Console.WriteLine($"     TypeCount       {dynamicColumn.TypeCount}");
                Console.WriteLine($"     TypeNames       [{string.Join(", ", dynamicColumn.TypeNames)}]   read off the wire, in discriminator order");
                Console.WriteLine($"     Discriminators  [{string.Join(", ", dynamicColumn.Discriminators.ToArray())}]   ints here, not bytes");
                Console.WriteLine($"     LocalIndices    [{string.Join(", ", dynamicColumn.LocalIndices.ToArray())}]");
                Console.WriteLine();
                Console.WriteLine($"     NULL is marked with TypeCount ({dynamicColumn.TypeCount}), one past the last type — there is no");
                Console.WriteLine("     fixed sentinel, because the type list is per block rather than declared.");
                Console.WriteLine();

                ReadOnlySpan<int> discriminators = dynamicColumn.Discriminators;
                ReadOnlySpan<int> local = dynamicColumn.LocalIndices;

                for (int row = 0; row < column.RowCount; row++)
                {
                    int discriminator = discriminators[row];
                    if (discriminator == dynamicColumn.TypeCount)
                    {
                        Console.WriteLine($"       row {row}: discriminator {discriminator} -> NULL");
                        continue;
                    }

                    IColumn child = dynamicColumn.GetTypeColumn(discriminator);
                    Console.WriteLine(
                        $"       row {row}: discriminator {discriminator} -> {child.TypeName,-8} ({Describe(child.ElementType)}) value {Render(child.GetValue(local[row]))}");
                }

                Console.WriteLine();
                Console.WriteLine("     TypeNames is what makes typed reading possible: the name tells you what to cast a");
                Console.WriteLine("     child to, so a caller can bind IColumn<T> per alternative without inspecting a");
                Console.WriteLine("     single value.");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Because the type set is per block, the same value can land on a different discriminator");
        Console.WriteLine("   in the next block of the same result. Read TypeNames inside the loop, not once.");
        Console.WriteLine();
        Console.WriteLine("   The client infers the ClickHouse type of each written value from its CLR type, so what");
        Console.WriteLine("   went in as a ulong came back as UInt64 and a double as Float64. A value whose CLR type");
        Console.WriteLine("   has no ClickHouse counterpart cannot be written into a Dynamic at all.");
    }

    private static async Task TheTwoCompared(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. Variant against Dynamic, in one table\n");
        Console.WriteLine("                        Variant(...)                      Dynamic");
        Console.WriteLine("   ---------------      ----------------------------      ------------------------------");
        Console.WriteLine("   alternatives         declared in the type string       discovered per block");
        Console.WriteLine("   the list             parse it out of TypeName          IDynamicColumn.TypeNames");
        Console.WriteLine("   Discriminators       ReadOnlySpan<byte>                ReadOnlySpan<int>");
        Console.WriteLine($"   NULL is marked        {IVariantColumn.NullDiscriminator} (NullDiscriminator)           TypeCount");
        Console.WriteLine("   NULL LocalIndex      -1                                -1");
        Console.WriteLine("   a row not in the     rejected by the server            widens the type set");
        Console.WriteLine("     alternative list");
        Console.WriteLine();
        Console.WriteLine("   The asymmetry worth remembering: a Variant tells you nothing about its alternatives");
        Console.WriteLine("   through the interface, and a Geometry column (Tcp_013 section 9) does not even carry");
        Console.WriteLine("   them in its type string. So for a Variant, hard-code the order you declared and check it");
        Console.WriteLine("   against TypeName; for a Dynamic, read TypeNames.");

        Console.WriteLine();
        Console.WriteLine("   dynamicType() is the Dynamic counterpart of variantType(), and unlike it returns the");
        Console.WriteLine("   name rather than an ordinal:");

        await foreach (object[] row in client.QueryAsync(
            $"SELECT id, dynamicType(d) FROM {DynamicTable} ORDER BY id"))
        {
            Console.WriteLine($"     row {row[0]}: dynamicType(d) = '{row[1]}'");
        }

        Console.WriteLine("     'None' is the NULL row. It is not one of the TypeNames.");
    }

    private static async Task JsonIsText(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. JSON: one serialization, and it is text\n");
        Console.WriteLine("   ClickHouse can send a JSON column in several encodings. The per-path binary ones split");
        Console.WriteLine("   the column into one sub-column per JSON path; this client decodes none of them. It reads");
        Console.WriteLine("   and writes only the String serialization, version 1, where a value is its JSON text:\n");

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST('{""a"": 1}', 'JSON') AS plain,
                     CAST('{""a"": 1, ""z"": ""s""}', 'JSON(a UInt32)') AS typed_path,
                     CAST(NULL, 'Nullable(JSON)') AS maybe,
                     CAST(['{""a"":1}', '{""b"":2}'], 'Array(JSON)') AS several"))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine($"     {column.Name,-11} {column.TypeName,-16} reads as {Describe(column.ElementType),-9} {Render(column.GetValue(0))}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Every spelling of the type is the same String column, so JSON(a UInt32) and");
        Console.WriteLine("   JSON(max_dynamic_paths=8) need no special handling — the arguments ride in TypeName only.");
        Console.WriteLine("   Under a composite the version marker comes first, then the composite's own framing.");
        Console.WriteLine();
        Console.WriteLine("   Reading needs the query setting output_format_native_write_json_as_string = 1. The client");
        Console.WriteLine("   sets it on every operation, so this is only your problem if you override it:");

        try
        {
            var withoutTheSetting = new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["output_format_native_write_json_as_string"] = "0" },
            };

            // Drained rather than broken out of: the throw is what this demonstrates, and stopping early
            // would discard the connection on the way to it.
            await foreach (Block _ in client.StreamAsync(@"SELECT CAST('{""a"":1}', 'JSON') AS j", withoutTheSetting))
            {
            }

            Console.WriteLine("     accepted, which this example did not expect");
        }
        catch (ClickHouseTcpProtocolException ex)
        {
            Console.WriteLine($"     {Wrap(ex.Message)}");
        }

        Console.WriteLine();
        Console.WriteLine("   Writing needs no setting at all: the version marker the client writes tells the server");
        Console.WriteLine("   which encoding it is reading, so version 1 makes it parse the text server-side — into a");
        Console.WriteLine("   JSON(a UInt32) column's typed paths as readily as into an untyped one.");
    }

    private static async Task JsonNormalization(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. Text in is not text out\n");
        Console.WriteLine("   A JSON value is parsed into paths and re-rendered, never stored verbatim. So a round trip");
        Console.WriteLine("   through a JSON column is lossy in a way a String column would not be. Written and read");
        Console.WriteLine("   back, unchanged in between:\n");

        var documents = new (string Text, string What)[]
        {
            ("{\"b\": 1, \"a\": 2}", "keys are sorted, ordinally"),
            ("{ \"x\" :  1 , \"y\": 2 }", "whitespace is dropped"),
            ("{\"a\": 1.500, \"b\": 1e3, \"c\": -0.0}", "numbers are re-rendered canonically"),
            ("{\"n\": null, \"empty\": {}}", "a JSON null and an empty object contribute no path"),
            ("{\"when\": \"2026-06-01T12:00:00Z\"}", "a string the server reads as a DateTime is re-formatted"),
            ("{\"B\": 1, \"a\": 2}", "ordinal sorting puts every capital before every lower case"),
        };

        await client.InsertAsync(
            $"INSERT INTO {JsonTable} (id, doc) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", Enumerable.Range(0, documents.Length).Select(i => (ulong)i).ToArray()),
                ClickHouseTcpColumn.Create("doc", documents.Select(document => document.Text).ToArray()),
            });

        int index = 0;
        await foreach (object[] row in client.QueryAsync($"SELECT id, doc FROM {JsonTable} ORDER BY id"))
        {
            (string text, string what) = documents[index++];
            Console.WriteLine($"     {what}");
            Console.WriteLine($"       in  {text}");
            Console.WriteLine($"       out {row[1]}");
        }

        Console.WriteLine();
        Console.WriteLine("   The DateTime row is the one that catches people. \"2026-06-01T12:00:00Z\" was inferred to");
        Console.WriteLine("   be a DateTime path, and a DateTime renders in ClickHouse's own format, so the T and the Z");
        Console.WriteLine("   are gone. The value is not corrupted — but it is no longer the string you wrote, and a");
        Console.WriteLine("   consumer parsing it as ISO 8601 will fail.");
        Console.WriteLine();
        Console.WriteLine("   You can see what the server decided each path was:");

        await foreach (object[] row in client.QueryAsync(
            $"SELECT id, toString(JSONAllPathsWithTypes(doc)) FROM {JsonTable} ORDER BY id"))
        {
            Console.WriteLine($"     row {row[0]}: {row[1]}");
        }

        Console.WriteLine();
        Console.WriteLine("   What to do about it:");
        Console.WriteLine("     Do not compare the text you wrote with the text you read. Compare the paths, or the");
        Console.WriteLine("       values at a path, which is what the server can be asked for.");
        Console.WriteLine("     Store a timestamp as a real DateTime64 column, not inside a JSON string.");
        Console.WriteLine("     Use a String column when you need the bytes back exactly — a JSON column is a set of");
        Console.WriteLine("       typed paths that happens to be spelled as text on this transport.");
    }

    private static string Describe(Type type)
    {
        if (type.IsArray)
        {
            return Describe(type.GetElementType()!) + "[]";
        }

        return type switch
        {
            _ when type == typeof(int) => "int",
            _ when type == typeof(uint) => "uint",
            _ when type == typeof(long) => "long",
            _ when type == typeof(ulong) => "ulong",
            _ when type == typeof(double) => "double",
            _ when type == typeof(string) => "string",
            _ when type == typeof(object) => "object",
            _ => type.Name,
        };
    }

    private static string Render(object? value) => value switch
    {
        null => "NULL",
        string text => $"\"{text}\"",
        System.Collections.IEnumerable items => "[" + string.Join(", ", items.Cast<object?>().Select(Render)) + "]",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "NULL",
    };

    // Reflows a long driver message so the console output stays readable.
    private static string Wrap(string message)
    {
        var lines = new List<string>();
        var line = new System.Text.StringBuilder();
        foreach (string word in message.Split(' '))
        {
            if (line.Length + word.Length + 1 > 88)
            {
                lines.Add(line.ToString());
                line.Clear();
            }

            line.Append(line.Length == 0 ? word : " " + word);
        }

        lines.Add(line.ToString());
        return string.Join("\n     ", lines);
    }
}
