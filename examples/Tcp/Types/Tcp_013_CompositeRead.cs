using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Reading the composite types through their typed views: <see cref="IArrayColumn{TElement}"/>,
/// <see cref="IMapColumn{TKey, TValue}"/>, <see cref="ITupleColumn"/>, <see cref="INestedColumn"/>,
/// <see cref="INullableColumn{T}"/> and <see cref="ILowCardinalityColumn{T}"/> — how they nest, and what the geo
/// aliases resolve to.
///
/// <para>
/// Two things decide how you write the pattern match. First, the view's type argument is the <b>wire's</b>
/// element type, not the row's: a <c>Nullable(Int32)</c> reads as <c>int?</c> but its view is
/// <c>INullableColumn&lt;int&gt;</c>, and a <c>LowCardinality(Nullable(String))</c> is
/// <c>ILowCardinalityColumn&lt;string&gt;</c>. Second, a composite's child is a column in its own right, so
/// reaching into a nested composite is another pattern match rather than an index into a materialized value.
/// </para>
///
/// <para>
/// <c>Tcp_006</c> covers the block tier itself and <c>IArrayColumn</c> in particular; <c>Tcp_010</c> covers
/// writing these shapes. This example is about the types.
/// </para>
/// </summary>
public static class TcpCompositeRead
{
    private const string TableName = "example_tcp_composite_read";
    private const string NestedTable = "example_tcp_composite_read_nested";

    private const string Columns =
        "id, readings, attrs, point, named_point, score, city, nick, matrix, tagged, buckets";

    // Geometry, the Variant over the six geo aliases, is newer than the rest of this example.
    private static readonly Version GeometryFrom = new(25, 11);

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();

        try
        {
            await Seed(client);
            await WhichViewEachCompositeOffers(client);
            await MapsAndArrays(client);
            await Tuples(client);
            await Nulls(client);
            await LowCardinalities(client);
            await Nesting(client);
            await NestedColumns(client);
            await GeoAliases(client);
            await Geometry(client, server);
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {NestedTable}");
            Console.WriteLine("\nDropped every table this example created.");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                id UInt64,
                readings Array(Float64),
                attrs Map(String, Int64),
                point Tuple(Float64, Float64),
                named_point Tuple(x Int32, y String),
                score Nullable(Float64),
                city LowCardinality(String),
                nick LowCardinality(Nullable(String)),
                matrix Array(Array(Int32)),
                tagged Array(Tuple(Int32, String)),
                buckets Map(String, Array(Int32))
            )
            ENGINE = MergeTree()
            ORDER BY id");

        await client.InsertAsync(
            $"INSERT INTO {TableName} ({Columns}) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2, 3 }),
                ClickHouseTcpColumn.Create("readings", new[] { new[] { 0.5, 0.75 }, Array.Empty<double>(), new[] { 1.5 } }),
                ClickHouseTcpColumn.Create("attrs", new[]
                {
                    new[] { new KeyValuePair<string, long>("floor", 3), new KeyValuePair<string, long>("room", 12) },
                    Array.Empty<KeyValuePair<string, long>>(),

                    // The wire carries keys in order, duplicates and all, which is why a row is a pair array
                    // rather than a Dictionary.
                    new[] { new KeyValuePair<string, long>("floor", 1), new KeyValuePair<string, long>("floor", 2) },
                }),
                ClickHouseTcpColumn.Create("point", new[] { (1.0, 2.0), (3.0, 4.0), (5.0, 6.0) }),
                ClickHouseTcpColumn.Create("named_point", new[] { (10, "ten"), (20, "twenty"), (30, "thirty") }),
                ClickHouseTcpColumn.Create("score", new double?[] { 1.25, null, 3.5 }),
                ClickHouseTcpColumn.Create("city", new[] { "Amsterdam", "Amsterdam", "Reykjavik" }),
                ClickHouseTcpColumn.Create("nick", new string?[] { "ada", null, "ada" }),
                ClickHouseTcpColumn.Create("matrix", new[] { new[] { new[] { 1, 2 }, new[] { 3 } }, Array.Empty<int[]>(), new[] { new[] { 4 } } }),
                ClickHouseTcpColumn.Create("tagged", new[]
                {
                    new[] { (1, "a"), (2, "b") },
                    Array.Empty<(int, string)>(),
                    new[] { (3, "c") },
                }),
                ClickHouseTcpColumn.Create("buckets", new[]
                {
                    new[] { new KeyValuePair<string, int[]>("evens", new[] { 2, 4 }) },
                    Array.Empty<KeyValuePair<string, int[]>>(),
                    new[] { new KeyValuePair<string, int[]>("odds", new[] { 1, 3, 5 }) },
                }),
            });

        // Nested has to be created with flatten_nested = 0 to stay one column rather than becoming one
        // Array(T) per field, and the client cannot build one from CLR values, so this one is seeded in SQL.
        var oneColumnNested = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["flatten_nested"] = "0" },
        };

        await client.ExecuteAsync(
            $@"CREATE TABLE {NestedTable} (id UInt64, items Nested(sku String, qty UInt32))
               ENGINE = MergeTree() ORDER BY id",
            oneColumnNested);

        await client.ExecuteAsync($"INSERT INTO {NestedTable} VALUES (1, [('bolt', 2), ('nut', 3)]), (2, []), (3, [('washer', 7)])");

        Console.WriteLine($"Seeded '{TableName}' with 3 rows of every composite, and '{NestedTable}' with a Nested column.");
    }

    private static async Task WhichViewEachCompositeOffers(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. What each composite reads as, and which view it offers\n");
        Console.WriteLine("   ClickHouse type                   One row is                      Pattern-matches to");
        Console.WriteLine("   --------------------------------  ------------------------------  ------------------------------");

        await foreach (Block block in client.StreamAsync($"SELECT {Columns} FROM {TableName} ORDER BY id"))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine($"   {column.TypeName,-32}  {Describe(column.ElementType),-30}  {View(column)}");
            }
        }

        await foreach (Block block in client.StreamAsync($"SELECT items FROM {NestedTable} ORDER BY id"))
        {
            IColumn column = block["items"];
            Console.WriteLine($"   {column.TypeName,-32}  {Describe(column.ElementType),-30}  {View(column)}");
        }

        Console.WriteLine();
        Console.WriteLine("   The type argument of a view is the wire's element type, which is not always the row's:");
        Console.WriteLine("     Nullable(Float64)                reads double?, view INullableColumn<double>");
        Console.WriteLine("     LowCardinality(Nullable(String)) reads string,  view ILowCardinalityColumn<string>");
        Console.WriteLine("   ITupleColumn, INestedColumn, IVariantColumn, IDynamicColumn and IQBitColumn are not");
        Console.WriteLine("   generic at all, so those five need no type argument to match on.");
    }

    private static async Task MapsAndArrays(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. Map(K, V): two flat columns plus offsets\n");
        Console.WriteLine("   A Map is byte-identical to Array(Tuple(K, V)), so its view is an Array's shape with the");
        Console.WriteLine("   run split in two. Row i's entries are Offsets[i] to Offsets[i + 1] in both columns:\n");

        await foreach (Block block in client.StreamAsync($"SELECT id, attrs FROM {TableName} ORDER BY id"))
        {
            if (block["attrs"] is IMapColumn<string, long> attrs)
            {
                ReadOnlySpan<int> offsets = attrs.Offsets;
                IColumn<string> keys = attrs.KeyColumn;
                IColumn<long> values = attrs.ValueColumn;

                Console.WriteLine($"     Offsets     = [{string.Join(", ", offsets.ToArray())}]   ({offsets.Length} entries for {attrs.RowCount} rows)");
                Console.WriteLine($"     KeyColumn   = [{string.Join(", ", keys.Values.ToArray())}]   RowCount {keys.RowCount}, the total entry count");
                Console.WriteLine($"     ValueColumn = [{string.Join(", ", values.Values.ToArray())}]");
                Console.WriteLine();

                for (int row = 0; row < attrs.RowCount; row++)
                {
                    var pairs = new List<string>();
                    for (int entry = offsets[row]; entry < offsets[row + 1]; entry++)
                    {
                        pairs.Add($"{keys[entry]}={values[entry]}");
                    }

                    Console.WriteLine($"     row {row}: {(pairs.Count == 0 ? "(empty)" : string.Join(", ", pairs))}");
                }

                Console.WriteLine();
                Console.WriteLine("     Row 2 has 'floor' twice. The two columns keep it, entry order and all, which is");
                Console.WriteLine("     what a Dictionary could not do — and the reason the materialized row is a");
                Console.WriteLine($"     KeyValuePair<string, long>[]: attrs[2] = [{string.Join(", ", attrs[2].Select(p => $"{p.Key}={p.Value}"))}]");
                Console.WriteLine();
                Console.WriteLine("     Taking only the keys, or only the values, therefore costs nothing:");
                Console.WriteLine($"       distinct keys across every row = {string.Join(", ", keys.Values.ToArray().Distinct())}");
            }
        }
    }

    private static async Task Tuples(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. Tuple(...): one child column per element, and the names are metadata\n");

        await foreach (Block block in client.StreamAsync($"SELECT point, named_point FROM {TableName} ORDER BY id"))
        {
            foreach (IColumn column in block.Columns)
            {
                var tuple = (ITupleColumn)column;
                Console.WriteLine($"   {column.TypeName}");
                Console.WriteLine($"     Children     [{string.Join(", ", tuple.Children.Select(child => $"{child.TypeName} as {Describe(child.ElementType)}"))}]");
                Console.WriteLine($"     FieldNames   {(tuple.FieldNames is null ? "null — the tuple carries no names at all" : "[" + string.Join(", ", tuple.FieldNames.Select(name => name ?? "(unnamed)")) + "]")}");
                Console.WriteLine($"     row 0        {Render(column.GetValue(0))}");
            }

            Console.WriteLine();
            Console.WriteLine("   FieldNames is null for an unnamed tuple, so check it before enumerating; a partly");
            Console.WriteLine("   named tuple gives a list with a null entry per unnamed element.");
            Console.WriteLine();
            Console.WriteLine("   The names never reach the value. A named Tuple materializes as a plain ValueTuple, so");
            Console.WriteLine("   read one element without building the pair by going through Children:");

            var named = (ITupleColumn)block["named_point"];
            IColumn<int> xs = (IColumn<int>)named.Children[0];
            Console.WriteLine($"     Children[0].Values = [{string.Join(", ", xs.Values.ToArray())}]   (the x of every row, no ValueTuple built)");
        }
    }

    private static async Task Nulls(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. Nullable(T): a null map plus a full-height inner column\n");

        await foreach (Block block in client.StreamAsync($"SELECT score FROM {TableName} ORDER BY id"))
        {
            // The type argument is double, not double?: it is the inner column's element type.
            if (block["score"] is INullableColumn<double> score)
            {
                ReadOnlySpan<byte> nulls = score.NullMap;
                IColumn<double> inner = score.Inner;

                Console.WriteLine($"   {block["score"].TypeName}, read as {Describe(block["score"].ElementType)}, view INullableColumn<double>");
                Console.WriteLine($"     NullMap      = [{string.Join(", ", nulls.ToArray())}]   one byte per row, 1 means NULL");
                Console.WriteLine($"     Inner.Values = [{string.Join(", ", inner.Values.ToArray())}]   full height, with a placeholder where the row is NULL");
                Console.WriteLine();
                Console.WriteLine("     The two are indexed by the same row number, so a null-aware read is one branch:");

                for (int row = 0; row < score.RowCount; row++)
                {
                    Console.WriteLine($"       row {row}: {(nulls[row] != 0 ? "NULL" : inner[row].ToString(CultureInfo.InvariantCulture))}");
                }

                Console.WriteLine();
                Console.WriteLine("     Do not read Inner without the null map. The value at a NULL position is the inner");
                Console.WriteLine("     codec's placeholder, not data — here it is 0, which is a perfectly plausible score.");
            }
        }
    }

    private static async Task LowCardinalities(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. LowCardinality(T): a dictionary plus one key per row\n");
        Console.WriteLine("   This is the view that changes what an algorithm costs. The materialized surface resolves");
        Console.WriteLine("   every row to its entry, so a million rows over a five-entry dictionary materializes a");
        Console.WriteLine("   million values; grouping on the keys instead touches each distinct value once.\n");

        await foreach (Block block in client.StreamAsync($"SELECT city, nick FROM {TableName} ORDER BY id"))
        {
            foreach (string name in new[] { "city", "nick" })
            {
                if (block[name] is ILowCardinalityColumn<string> lc)
                {
                    // The reserved slots hold the inner codec's placeholder, which for a String is the empty
                    // string — indistinguishable from data unless they are labelled.
                    string[] slots = lc.Dictionary.Values.ToArray()
                        .Select((value, slot) => slot < lc.ReservedSlotCount
                            ? (slot == 0 && lc.ReservedSlotCount == 2 ? "<null marker>" : "<default>")
                            : $"'{value}'")
                        .ToArray();

                    Console.WriteLine($"   {block[name].TypeName}");
                    Console.WriteLine($"     Dictionary        [{string.Join(", ", slots)}]   RowCount {lc.Dictionary.RowCount}");
                    Console.WriteLine($"     Keys              [{string.Join(", ", lc.Keys.ToArray())}]   one per row, an index into it");
                    Console.WriteLine($"     ReservedSlotCount {lc.ReservedSlotCount}   (so data starts at slot {lc.ReservedSlotCount})");

                    for (int row = 0; row < lc.RowCount; row++)
                    {
                        bool isNull = lc.ReservedSlotCount == 2 && lc.Keys[row] == 0;
                        Console.WriteLine($"       row {row}: key {lc.Keys[row]} -> {(isNull ? "NULL" : $"'{lc.Dictionary[lc.Keys[row]]}'")}");
                    }
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine("   ReservedSlotCount is the whole reason to have that property: the leading dictionary slots");
        Console.WriteLine("   are not data. It is 1 for a non-nullable inner (slot 0 is the inner default) and 2 for a");
        Console.WriteLine("   nullable one (slot 0 is the NULL marker, slot 1 the default). So a key of 0 means NULL for");
        Console.WriteLine("   one shape and an ordinary default for the other, and reading the property is how you tell");
        Console.WriteLine("   them apart without parsing TypeName.");
        Console.WriteLine();
        Console.WriteLine("   The dictionary is per block, not per column or per table, so the same value can have a");
        Console.WriteLine("   different key in the next block of the same result.");
    }

    private static async Task Nesting(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. Nesting: a child is a column, so you match again\n");

        await foreach (Block block in client.StreamAsync($"SELECT matrix, tagged, buckets FROM {TableName} ORDER BY id"))
        {
            Console.WriteLine($"   {block["matrix"].TypeName}: an array whose Inner is another array");
            if (block["matrix"] is IArrayColumn<int[]> matrix && matrix.Inner is IArrayColumn<int> rows)
            {
                Console.WriteLine($"     outer Offsets      [{string.Join(", ", matrix.Offsets.ToArray())}]");
                Console.WriteLine($"     inner Offsets      [{string.Join(", ", rows.Offsets.ToArray())}]");
                Console.WriteLine($"     inner InnerValues  [{string.Join(", ", rows.InnerValues.ToArray())}]   every element of every sub-array, flat");
                Console.WriteLine("     Two offset levels over one flat run, so a sum over the whole column needs no");
                Console.WriteLine($"     array at all: total {Sum(rows.InnerValues)}");
            }

            Console.WriteLine();
            Console.WriteLine($"   {block["tagged"].TypeName}: an array whose Inner is a tuple");
            if (block["tagged"] is IArrayColumn<(int, string)> tagged && tagged.Inner is ITupleColumn pairs)
            {
                Console.WriteLine($"     Offsets            [{string.Join(", ", tagged.Offsets.ToArray())}]");
                Console.WriteLine($"     Inner is ITupleColumn with children [{string.Join(", ", pairs.Children.Select(c => c.TypeName))}]");
                Console.WriteLine($"     Inner.Children[1].Values = [{string.Join(", ", ((IColumn<string>)pairs.Children[1]).Values.ToArray())}]   every tag, no tuple built");
            }

            Console.WriteLine();
            Console.WriteLine($"   {block["buckets"].TypeName}: a map whose ValueColumn is an array");
            if (block["buckets"] is IMapColumn<string, int[]> buckets && buckets.ValueColumn is IArrayColumn<int> lists)
            {
                Console.WriteLine($"     Offsets                  [{string.Join(", ", buckets.Offsets.ToArray())}]");
                Console.WriteLine($"     KeyColumn.Values         [{string.Join(", ", buckets.KeyColumn.Values.ToArray())}]");
                Console.WriteLine($"     ValueColumn is IArrayColumn<int>, Offsets [{string.Join(", ", lists.Offsets.ToArray())}], InnerValues [{string.Join(", ", lists.InnerValues.ToArray())}]");
            }

            Console.WriteLine();
            Console.WriteLine("   Composites nest as deep as the server lets them, with no materialization at any level.");
            Console.WriteLine("   The one thing to know is that each match needs the child's element type spelled out,");
            Console.WriteLine("   which IColumn.ElementType on the parent tells you: Array(Array(Int32)) reports int[][],");
            Console.WriteLine("   so the outer view is IArrayColumn<int[]> and the inner one IArrayColumn<int>.");
        }
    }

    private static async Task NestedColumns(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n7. Nested(...): named fields over shared offsets\n");
        Console.WriteLine("   A Nested column is byte-identical to Array(Tuple(...)) and differs only in keeping the");
        Console.WriteLine("   field names. Its view is by name rather than by position, and is not generic:\n");

        await foreach (Block block in client.StreamAsync($"SELECT items FROM {NestedTable} ORDER BY id"))
        {
            if (block["items"] is INestedColumn items)
            {
                Console.WriteLine($"   {block["items"].TypeName}");
                Console.WriteLine($"     FieldCount  {items.FieldCount}");
                Console.WriteLine($"     FieldNames  [{string.Join(", ", items.FieldNames)}]");
                Console.WriteLine($"     Offsets     [{string.Join(", ", items.Offsets.ToArray())}]   shared by every field");

                var skus = (IColumn<string>)items.GetField("sku");
                var quantities = (IColumn<uint>)items.GetField("qty");
                Console.WriteLine($"     GetField(\"sku\").Values [{string.Join(", ", skus.Values.ToArray())}]");
                Console.WriteLine($"     GetField(\"qty\").Values [{string.Join(", ", quantities.Values.ToArray())}]");
                Console.WriteLine("     GetField(int) takes the same field by position.");
                Console.WriteLine();

                ReadOnlySpan<int> offsets = items.Offsets;
                for (int row = 0; row < items.RowCount; row++)
                {
                    var entries = new List<string>();
                    for (int entry = offsets[row]; entry < offsets[row + 1]; entry++)
                    {
                        entries.Add($"{skus[entry]} x{quantities[entry]}");
                    }

                    Console.WriteLine($"       row {row}: {(entries.Count == 0 ? "(empty)" : string.Join(", ", entries))}");
                }

                Console.WriteLine();
                Console.WriteLine($"     The materialized row is an object[][] — one object[] per entry, boxed, so the");
                Console.WriteLine($"     field columns are the way to read it: items.GetValue(0) = {Render(block["items"].GetValue(0))}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   A Nested column only exists as one column when the table was created with");
        Console.WriteLine("   flatten_nested = 0. On the default setting the server turns Nested(a T, b U) into an");
        Console.WriteLine("   Array(T) named a and an Array(U) named b, and this view never appears.");
    }

    private static async Task GeoAliases(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n8. The geo aliases resolve to structures you have already seen\n");
        Console.WriteLine("   Each is a name for a shape built out of Tuple and Array, and the wire header carries the");
        Console.WriteLine("   alias rather than the structure — so TypeName is the alias, and the view is the");
        Console.WriteLine("   structure's:\n");
        Console.WriteLine("   TypeName         One row is                          Pattern-matches to");
        Console.WriteLine("   ---------------  ----------------------------------  ----------------------------------");

        const string geoSql = @"
            SELECT CAST((1.0, 2.0), 'Point') AS p,
                   CAST([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)], 'Ring') AS r,
                   CAST([(0.0, 0.0), (1.0, 1.0)], 'LineString') AS ls,
                   CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]], 'Polygon') AS pg,
                   CAST([[(0.0, 0.0), (1.0, 1.0)]], 'MultiLineString') AS mls,
                   CAST([[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]]], 'MultiPolygon') AS mp";

        await foreach (Block block in client.StreamAsync(geoSql))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine($"   {column.TypeName,-15}  {Describe(column.ElementType),-34}  {View(column)}");
            }

            Console.WriteLine();
            Console.WriteLine("   Point is a Tuple(Float64, Float64) and the rest are arrays over it, so a Ring's");
            Console.WriteLine("   coordinates are reachable as two flat columns without any tuple being built:");

            if (block["r"] is IArrayColumn<(double, double)> ring && ring.Inner is ITupleColumn coordinates)
            {
                var longitudes = (IColumn<double>)coordinates.Children[0];
                var latitudes = (IColumn<double>)coordinates.Children[1];
                Console.WriteLine($"     Offsets     [{string.Join(", ", ring.Offsets.ToArray())}]");
                Console.WriteLine($"     Children[0] [{string.Join(", ", longitudes.Values.ToArray())}]");
                Console.WriteLine($"     Children[1] [{string.Join(", ", latitudes.Values.ToArray())}]");
                Console.WriteLine($"     row 0       {Render(block["r"].GetValue(0))}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   The point to carry over from the HTTP driver: a coordinate pair here is a ValueTuple,");
        Console.WriteLine("   where that one builds a System.Tuple. So (double, double) and not Tuple<double, double>,");
        Console.WriteLine("   and .Item1 / .Item2 on a struct rather than on a class.");
        Console.WriteLine();
        Console.WriteLine("   Ring and LineString are distinct types to the server and the same structure to this");
        Console.WriteLine("   client, as are Polygon and MultiLineString. Only the name tells them apart.");
    }

    private static async Task Geometry(ClickHouseTcpClient client, ClickHouseTcpServerInfo server)
    {
        Console.WriteLine("\n9. Geometry is the one alias that is not a nested array\n");

        if (server.Version < GeometryFrom)
        {
            Console.WriteLine($"   Skipped: needs ClickHouse {GeometryFrom} or newer, this server is {server.Version}.");
            return;
        }

        Console.WriteLine("   It names a Variant over the six above, so one column holds rows of different shapes. The");
        Console.WriteLine("   header carries only 'Geometry', so the client expands the alternatives itself, in the");
        Console.WriteLine("   server's own name-sorted discriminator order:\n");

        const string sql = @"
            SELECT g FROM (SELECT arrayJoin([
                CAST(CAST((1.0, 2.0), 'Point'), 'Geometry'),
                CAST(CAST([(1.0, 2.0), (3.0, 4.0)], 'LineString'), 'Geometry'),
                CAST(CAST([[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]]], 'MultiPolygon'), 'Geometry')]) AS g)";

        await foreach (Block block in client.StreamAsync(sql))
        {
            if (block["g"] is IVariantColumn geometry)
            {
                Console.WriteLine($"   {block["g"].TypeName}, read as {Describe(block["g"].ElementType)}, view IVariantColumn");
                Console.WriteLine($"     TypeCount       {geometry.TypeCount}");
                Console.WriteLine($"     Discriminators  [{string.Join(", ", geometry.Discriminators.ToArray())}]");
                Console.WriteLine($"     LocalIndices    [{string.Join(", ", geometry.LocalIndices.ToArray())}]");
                Console.WriteLine();
                Console.WriteLine("     Alternative order: 0 LineString, 1 MultiLineString, 2 MultiPolygon, 3 Point,");
                Console.WriteLine("     4 Polygon, 5 Ring. GetTypeColumn names the shape of each row:");

                for (int row = 0; row < geometry.RowCount; row++)
                {
                    IColumn child = geometry.GetTypeColumn(geometry.Discriminators[row]);
                    Console.WriteLine($"       row {row}: discriminator {geometry.Discriminators[row]} -> {child.TypeName,-14} value {Render(block["g"].GetValue(row))}");
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Tcp_014 covers IVariantColumn properly, including the NULL discriminator and how to");
        Console.WriteLine("   dispatch on it without boxing.");
    }

    private static double Sum(ReadOnlySpan<int> values)
    {
        double total = 0;
        foreach (int value in values)
        {
            total += value;
        }

        return total;
    }

    // Which of the block tier's typed views a column offers, found by pattern-matching rather than by reading
    // TypeName. The generic ones each need their element type spelled out, which is what makes this list long.
    private static string View(IColumn column) => column switch
    {
        IVariantColumn => "IVariantColumn",
        INestedColumn => "INestedColumn",
        ITupleColumn => "ITupleColumn",
        IMapColumn<string, long> => "IMapColumn<string, long>",
        IMapColumn<string, int[]> => "IMapColumn<string, int[]>",
        INullableColumn<double> => "INullableColumn<double>",
        ILowCardinalityColumn<string> => "ILowCardinalityColumn<string>",
        IArrayColumn<double> => "IArrayColumn<double>",
        IArrayColumn<int[]> => "IArrayColumn<int[]>",
        IArrayColumn<(int, string)> => "IArrayColumn<(int, string)>",
        IArrayColumn<(double, double)> => "IArrayColumn<(double, double)>",
        IArrayColumn<(double, double)[]> => "IArrayColumn<(double, double)[]>",
        IArrayColumn<(double, double)[][]> => "IArrayColumn<(double, double)[][]>",
        _ => "- (no composite view)",
    };

    private static string Describe(Type type)
    {
        if (type.IsArray)
        {
            return Describe(type.GetElementType()!) + "[]";
        }

        if (type.IsGenericType)
        {
            Type definition = type.GetGenericTypeDefinition();
            string[] arguments = type.GetGenericArguments().Select(Describe).ToArray();
            if (definition == typeof(Nullable<>))
            {
                return arguments[0] + "?";
            }

            if (definition.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true)
            {
                return "(" + string.Join(", ", arguments) + ")";
            }

            string name = definition.Name[..definition.Name.IndexOf('`')];
            return $"{name}<{string.Join(", ", arguments)}>";
        }

        return type switch
        {
            _ when type == typeof(byte) => "byte",
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
        string text => $"'{text}'",
        System.Collections.IEnumerable items => "[" + string.Join(", ", items.Cast<object?>().Select(Render)) + "]",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "NULL",
    };
}
