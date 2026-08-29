using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// <c>QBit(T, N)</c> and <see cref="IQBitColumn"/>: the one type whose whole point is its storage layout.
///
/// <para>
/// A <c>QBit</c> row is an <c>N</c>-element vector, but the elements of a row are not stored together. The column
/// holds one <b>bit plane</b> per bit position of the element type, and a plane carries that one bit of every
/// element of every row. So the most significant bits of a whole column sit contiguously, which is what lets a
/// distance be computed at reduced precision by reading only the top few planes — the server's
/// <c>L2DistanceTransposed(vector, query, precision)</c> does exactly that, and
/// <see cref="IQBitColumn.GetPlane(int)"/> is the same access from the client.
/// </para>
///
/// <para>
/// The default <c>IColumn&lt;T&gt;</c> view undoes the transposition and hands back a <c>float[]</c> (or
/// <c>double[]</c>) per row, which is convenient and throws away the only reason to use the type. This example is
/// about the planes. <c>examples/Http/DataTypes/Vector_001_QBitSimilaritySearch.cs</c> covers the server-side
/// search, which the HTTP transport can do just as well.
/// </para>
/// </summary>
public static class TcpQBitVectorSearch
{
    private const string TableName = "example_tcp_qbit";
    private const string WideTable = "example_tcp_qbit_wide";

    // Int8 elements and the strided QBit(T, N, stride) form both need a newer server.
    private static readonly Version StridedAndInt8From = new(26, 7);

    private static readonly (string Word, float[] Vector)[] Corpus =
    {
        ("apple", new[] { 0.9f, 0.1f, 0.8f, 0.2f, 0.7f }),
        ("banana", new[] { 0.85f, 0.15f, 0.75f, 0.25f, 0.65f }),
        ("orange", new[] { 0.88f, 0.12f, 0.78f, 0.22f, 0.68f }),
        ("dog", new[] { 0.1f, 0.9f, 0.2f, 0.8f, 0.3f }),
        ("horse", new[] { 0.15f, 0.85f, 0.25f, 0.75f, 0.35f }),
        ("cat", new[] { 0.12f, 0.88f, 0.22f, 0.78f, 0.32f }),
    };

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();

        try
        {
            await Seed(client);
            await TheGeometry(client);
            await ReadingAPlane(client);
            await ByteOrderWithinABitmap(client);
            await ReducedPrecision(client);
            await ElementTypes(client, server);
            await Strided(client, server);
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {WideTable}");
            Console.WriteLine("\nDropped every table this example created.");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName} (word String, vec QBit(Float32, 5))
            ENGINE = MergeTree() ORDER BY word");

        // A QBit column is written from one float[] per row. The client transposes it into planes.
        await client.InsertAsync(
            $"INSERT INTO {TableName} (word, vec) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("word", Corpus.Select(entry => entry.Word).ToArray()),
                ClickHouseTcpColumn.Create("vec", Corpus.Select(entry => entry.Vector).ToArray()),
            });

        Console.WriteLine($"Seeded '{TableName}' with {Corpus.Length} words as QBit(Float32, 5) embeddings.");
    }

    private static async Task TheGeometry(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. What the column reports about its layout\n");

        await foreach (Block block in client.StreamAsync($"SELECT vec FROM {TableName} ORDER BY word"))
        {
            IColumn column = block["vec"];
            Console.WriteLine($"   {column.TypeName}, reads as {Describe(column.ElementType)}, {column.RowCount} rows\n");

            if (column is IQBitColumn qbit)
            {
                Console.WriteLine($"     Dimension    {qbit.Dimension}   the N of QBit(T, N) — elements per vector");
                Console.WriteLine($"     BitWidth     {qbit.BitWidth}  the stored element's bit width, so the number of planes");
                Console.WriteLine($"     Stride       {qbit.Stride}   elements one group of planes covers");
                Console.WriteLine($"     GroupCount   {qbit.GroupCount}   Dimension / Stride");
                Console.WriteLine($"     BytesPerRow  {qbit.BytesPerRow}   ceil(Stride / 8) — one row's bitmap within one plane");
                Console.WriteLine();
                Console.WriteLine("     The body is plane-major and every row is the same width, so its size is exact:");

                int body = qbit.BitWidth * qbit.RowCount * qbit.BytesPerRow;
                int flat = qbit.Dimension * (qbit.BitWidth / 8) * qbit.RowCount;
                Console.WriteLine($"       BitWidth * RowCount * BytesPerRow = {qbit.BitWidth} * {qbit.RowCount} * {qbit.BytesPerRow} = {body} bytes");
                Console.WriteLine($"       the same values as {qbit.Dimension} Float32 per row       = {flat} bytes");
                Console.WriteLine();
                Console.WriteLine($"     The extra is padding. A plane's row is a whole number of bytes, so {qbit.BytesPerRow * 8} bit slots");
                Console.WriteLine($"     carry {qbit.Stride} elements and {(qbit.BytesPerRow * 8) - qbit.Stride} slots go unused in every one of the {qbit.BitWidth} planes. A Stride");
                Console.WriteLine($"     that is a multiple of 8 wastes nothing; {qbit.Stride} is not, so this column is the wider one.");
                Console.WriteLine();
                Console.WriteLine("     BitWidth is the width of the STORED element, not of the CLR one: a");
                Console.WriteLine("     QBit(BFloat16, N) has 16 planes and still reads as float[]. Section 5.");
            }

            break;
        }
    }

    private static async Task ReadingAPlane(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. A plane is one bit of every element of every row\n");

        await foreach (Block block in client.StreamAsync($"SELECT word, vec FROM {TableName} ORDER BY word"))
        {
            if (block["vec"] is IQBitColumn qbit)
            {
                var words = (IColumn<string>)block["word"];
                Console.WriteLine($"     GetPlane(bit) returns RowCount * BytesPerRow = {qbit.RowCount} * {qbit.BytesPerRow} = {qbit.RowCount * qbit.BytesPerRow} bytes.");
                Console.WriteLine($"     Row r's bitmap is the slice [r * BytesPerRow, (r + 1) * BytesPerRow).");
                Console.WriteLine();
                Console.WriteLine($"     bit is the SIGNIFICANCE within the stored element, so {qbit.BitWidth - 1} is the sign bit and 0 the");
                Console.WriteLine("     least significant mantissa bit. (The wire stores the planes the other way round,");
                Console.WriteLine("     most significant first; the accessor hides that.)");
                Console.WriteLine();
                Console.WriteLine($"     The top {qbit.BitWidth - 24} planes of this corpus, most significant first:");
                Console.WriteLine();
                Console.WriteLine("       plane   bitmaps, one byte per row");
                Console.WriteLine("       ------  -------------------------");

                for (int bit = qbit.BitWidth - 1; bit >= 24; bit--)
                {
                    ReadOnlySpan<byte> plane = qbit.GetPlane(bit);
                    Console.WriteLine($"       bit {bit,2}  {string.Join(" ", plane.ToArray().Select(value => value.ToString("X2", CultureInfo.InvariantCulture)))}");
                }

                // The highest plane that is not identical across every row. Found from the data rather than
                // asserted, because it depends entirely on what the vectors are.
                int firstDifference = qbit.BitWidth - 1;
                while (firstDifference >= 0 && Uniform(qbit.GetPlane(firstDifference)))
                {
                    firstDifference--;
                }

                Console.WriteLine();
                Console.WriteLine($"     The top {qbit.BitWidth - 1 - firstDifference} planes are identical in every row: bit {qbit.BitWidth - 1} is the sign and every vector");
                Console.WriteLine($"     here is positive, and the exponent's high bits agree because every element is in");
                Console.WriteLine($"     [0.1, 0.9]. The first plane that separates the corpus is bit {firstDifference}. That is the type's");
                Console.WriteLine("     bargain: precision costs planes, and how many you can drop depends on the data.");

                Console.WriteLine();
                Console.WriteLine($"     Within a row's bitmap, element i is bit i % 8 of byte BytesPerRow - 1 - i / 8. With");
                Console.WriteLine($"     BytesPerRow = {qbit.BytesPerRow} there is one byte per row, so element i is simply bit i:");
                Console.WriteLine();
                Console.WriteLine($"       word     bit {firstDifference} bitmap  elements with bit {firstDifference} set");
                Console.WriteLine("       -------  -------------  ------------------------");

                ReadOnlySpan<byte> interesting = qbit.GetPlane(firstDifference);
                for (int row = 0; row < qbit.RowCount; row++)
                {
                    byte bitmap = interesting[(row * qbit.BytesPerRow) + qbit.BytesPerRow - 1];
                    var set = new List<int>();
                    for (int element = 0; element < qbit.Dimension; element++)
                    {
                        if ((bitmap & (1 << element)) != 0)
                        {
                            set.Add(element);
                        }
                    }

                    Console.WriteLine($"       {words[row],-7}  {Convert.ToString(bitmap, 2).PadLeft(8, '0')}       {(set.Count == 0 ? "none" : string.Join(", ", set))}");
                }

                Console.WriteLine();
                Console.WriteLine("     A bit index outside the planes, or a group outside the groups, is refused:");
                foreach (Action attempt in new Action[] { () => qbit.GetPlane(qbit.BitWidth), () => qbit.GetPlane(0, 1) })
                {
                    try
                    {
                        attempt();
                    }
                    catch (ArgumentOutOfRangeException ex)
                    {
                        Console.WriteLine($"       {ex.Message.Split(" (Parameter")[0]}");
                    }
                }
            }

            break;
        }
    }

    private static async Task ByteOrderWithinABitmap(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. Past 8 elements the bytes run backwards\n");
        Console.WriteLine("   The bits within a byte run least significant first, but the bytes run in the reverse of");
        Console.WriteLine("   the element order — element 0 is in the LAST byte. Equivalently, a row's bitmap is the");
        Console.WriteLine("   big-endian encoding of a BytesPerRow-byte integer whose bit i is element i. That is");
        Console.WriteLine("   invisible at 5 elements and not at 12:\n");

        await client.ExecuteAsync($@"
            CREATE TABLE {WideTable} (v QBit(Float32, 12))
            ENGINE = MergeTree() ORDER BY tuple()");

        // Elements 0 and 8 negative, the rest positive, so the sign plane says exactly where they sit.
        float[] signs = Enumerable.Range(0, 12).Select(i => i is 0 or 8 ? -1.0f : 1.0f).ToArray();
        await client.InsertAsync(
            $"INSERT INTO {WideTable} (v) VALUES",
            new[] { ClickHouseTcpColumn.Create("v", new[] { signs }) });

        Console.WriteLine($"   One row of QBit(Float32, 12): [{string.Join(", ", signs.Select(value => value.ToString(CultureInfo.InvariantCulture)))}]");
        Console.WriteLine("   Only elements 0 and 8 are negative.\n");

        await foreach (Block block in client.StreamAsync($"SELECT v FROM {WideTable}"))
        {
            if (block["v"] is IQBitColumn wide)
            {
                ReadOnlySpan<byte> sign = wide.GetPlane(wide.BitWidth - 1);
                Console.WriteLine($"     BytesPerRow  {wide.BytesPerRow}   (ceil(12 / 8))");
                Console.WriteLine($"     sign plane   {string.Join("  ", sign.ToArray().Select(value => Convert.ToString(value, 2).PadLeft(8, '0')))}");
                Console.WriteLine("                  ^ byte 0  ^ byte 1");
                Console.WriteLine("     Byte 1 holds elements 0-7 and byte 0 elements 8-11, so the bit set in byte 1 is");
                Console.WriteLine("     element 0 and the bit set in byte 0 is element 8.");
                Console.WriteLine();
                Console.WriteLine("     The formula covers both: element i is bit i % 8 of byte BytesPerRow - 1 - i / 8.");
                Console.WriteLine($"       element 0 -> bit 0 of byte {wide.BytesPerRow - 1 - (0 / 8)}");
                Console.WriteLine($"       element 8 -> bit 0 of byte {wide.BytesPerRow - 1 - (8 / 8)}");
                Console.WriteLine();
                Console.WriteLine($"     With Stride not a multiple of 8, the {(wide.BytesPerRow * 8) - wide.Dimension} unused bits are the high bits of byte 0.");
            }

            break;
        }
    }

    private static async Task ReducedPrecision(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. Why the planes exist: a distance at reduced precision\n");
        Console.WriteLine("   Reading only the top K planes and treating the rest of each element's bits as zero gives");
        Console.WriteLine("   a truncated float — a quarter of the bytes at K = 8. Rebuilt from its planes, the vector");
        Console.WriteLine("   of 'apple':\n");
        Console.WriteLine("   Planes read  Bytes per row  Reconstructed vector");
        Console.WriteLine("   -----------  -------------  ----------------------------------------------");

        await foreach (Block block in client.StreamAsync($"SELECT word, vec FROM {TableName} ORDER BY word"))
        {
            if (block["vec"] is IQBitColumn qbit)
            {
                var words = (IColumn<string>)block["word"];
                int apple = 0;
                for (int row = 0; row < words.RowCount; row++)
                {
                    if (words[row] == "apple")
                    {
                        apple = row;
                    }
                }

                foreach (int keep in new[] { 32, 16, 12, 8 })
                {
                    float[] rebuilt = Reconstruct(qbit, apple, keep);
                    Console.WriteLine(
                        $"   top {keep,2}       {keep * qbit.BytesPerRow,3}            [{string.Join(", ", rebuilt.Select(value => value.ToString("0.####", CultureInfo.InvariantCulture)))}]");
                }

                // The materialized view, for comparison: it reads every plane, which is what "top 32" did.
                float[] materialized = ((IColumn<float[]>)block["vec"])[apple];
                Console.WriteLine();
                Console.WriteLine($"   The top row is exact, and equals what the IColumn<float[]> view hands back:");
                Console.WriteLine($"     [{string.Join(", ", materialized.Select(value => value.ToString("0.####", CultureInfo.InvariantCulture)))}]");
            }

            break;
        }

        Console.WriteLine();
        Console.WriteLine("   The server does the same arithmetic in L2DistanceTransposed's third argument, which is a");
        Console.WriteLine("   count of planes. Ranking 'apple' against the corpus at three precisions:\n");
        Console.WriteLine("   word     precision 32  precision 12  precision 8");
        Console.WriteLine("   -------  ------------  ------------  -----------");

        const string query = "[0.9, 0.1, 0.8, 0.2, 0.7]";
        await foreach (object[] row in client.QueryAsync($@"
            SELECT word,
                   L2DistanceTransposed(vec, {query}, 32) AS d32,
                   L2DistanceTransposed(vec, {query}, 12) AS d12,
                   L2DistanceTransposed(vec, {query}, 8) AS d8
            FROM {TableName}
            ORDER BY d32"))
        {
            Console.WriteLine($"   {row[0],-7}  {Number(row[1]),-12}  {Number(row[2]),-12}  {Number(row[3])}");
        }

        Console.WriteLine();
        Console.WriteLine("   Read the columns, not just the numbers. At precision 12 the ranking is unchanged and the");
        Console.WriteLine("   distances are already wrong in the second digit. At precision 8 apple and orange tie and");
        Console.WriteLine("   banana comes out ahead of both — the ranking has broken, while the two clusters are still");
        Console.WriteLine("   cleanly separated. That is the trade the type is for: shortlist cheaply at low precision,");
        Console.WriteLine("   then re-rank the shortlist exactly. How low you can go is a property of your vectors, so");
        Console.WriteLine("   measure it rather than picking a number.");
        Console.WriteLine();
        Console.WriteLine("   Where the client's plane access earns its keep is the work the server has no function");
        Console.WriteLine("   for: a custom metric, a quantizer, or an index built over the top planes only. Reading");
        Console.WriteLine("   GetPlane(bit) for the few bits you want touches only those bytes, whereas the");
        Console.WriteLine("   IColumn<float[]> view materializes every element of every row.");
    }

    private static async Task ElementTypes(ClickHouseTcpClient client, ClickHouseTcpServerInfo server)
    {
        Console.WriteLine("\n5. The element types, and the CLR type each reads as\n");
        Console.WriteLine("   Type                BitWidth  Reads as   Note");
        Console.WriteLine("   ------------------  --------  ---------  --------------------------------------");

        foreach (string element in new[] { "BFloat16", "Float32", "Float64", "Int8" })
        {
            if (element == "Int8" && server.Version < StridedAndInt8From)
            {
                Console.WriteLine($"   QBit(Int8, 5)       -         -          skipped: needs ClickHouse {StridedAndInt8From} or newer,");
                Console.WriteLine($"                                            this server is {server.Version}");
                continue;
            }

            string table = $"{TableName}_{element}";
            try
            {
                await client.ExecuteAsync($"CREATE TABLE {table} (v QBit({element}, 5)) ENGINE = MergeTree() ORDER BY tuple()");
                await client.ExecuteAsync($"INSERT INTO {table} VALUES ([1.0, 2.0, 0.5, -1.0, 0.25])");

                await foreach (Block block in client.StreamAsync($"SELECT v FROM {table}"))
                {
                    var qbit = (IQBitColumn)block["v"];
                    string note = element switch
                    {
                        "BFloat16" => "16 planes, widened to float on the way out",
                        "Float64" => "the only one that reads as double[]",
                        "Int8" => "since ClickHouse 26.7",
                        _ => "the common case",
                    };
                    Console.WriteLine($"   {block["v"].TypeName,-18}  {qbit.BitWidth,-8}  {Describe(block["v"].ElementType),-9}  {note}");
                    Console.WriteLine($"                                            row 0 = [{string.Join(", ", ((System.Collections.IEnumerable)block["v"].GetValue(0)!).Cast<object>().Select(Number))}]");
                    break;
                }
            }
            catch (ClickHouseTcpServerException ex)
            {
                Console.WriteLine($"   QBit({element}, 5)  refused by this server:");
                Console.WriteLine($"     {FirstLine(ex.Message)}");
            }
            finally
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   A BFloat16 element's plane positions are those of the 16-bit brain-float, not of the");
        Console.WriteLine("   float you read: bit 15 is its sign, and there are only 16 planes to ask for.");
    }

    private static async Task Strided(ClickHouseTcpClient client, ClickHouseTcpServerInfo server)
    {
        Console.WriteLine("\n6. The strided form, and why Stride and GroupCount exist\n");
        Console.WriteLine("   ClickHouse 26.7 added an optional third argument, QBit(T, N, stride), which splits a row");
        Console.WriteLine("   into N / stride independent groups, each carrying its own full set of planes. A plane is");
        Console.WriteLine("   then GroupCount disjoint runs, so GetPlane(bit) cannot name it and GetPlane(bit, group)");
        Console.WriteLine("   is the accessor.\n");

        if (server.Version < StridedAndInt8From)
        {
            Console.WriteLine($"   Skipped: needs ClickHouse {StridedAndInt8From} or newer, this server is {server.Version}.");
            Console.WriteLine("   Confirming the server's own answer rather than assuming it:");

            try
            {
                await client.ExecuteAsync($"CREATE TABLE {WideTable}_strided (v QBit(Float32, 8, 4)) ENGINE = MergeTree() ORDER BY tuple()");
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {WideTable}_strided");
                Console.WriteLine("     accepted, which this example did not expect");
            }
            catch (ClickHouseTcpServerException ex)
            {
                Console.WriteLine($"     {FirstLine(ex.Message)}");
            }
        }
        else
        {
            Console.WriteLine("   This server is new enough to declare one. Note that this client does not decode the");
            Console.WriteLine("   strided body yet, so reading such a column reports a NotSupportedException:");

            try
            {
                await client.ExecuteAsync($"CREATE TABLE {WideTable}_strided (v QBit(Float32, 8, 4)) ENGINE = MergeTree() ORDER BY tuple()");
                await client.ExecuteAsync($"INSERT INTO {WideTable}_strided VALUES ([1, 2, 3, 4, 5, 6, 7, 8])");

                await foreach (Block _ in client.StreamAsync($"SELECT v FROM {WideTable}_strided"))
                {
                    break;
                }

                Console.WriteLine("     read, which this example did not expect");
            }
            catch (Exception ex) when (ex is NotSupportedException or ClickHouseTcpServerException)
            {
                Console.WriteLine($"     {ex.GetType().Name}: {FirstLine(ex.Message)}");
            }
            finally
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {WideTable}_strided");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   So on any server this client reads, GroupCount is 1 and Stride equals Dimension. Both");
        Console.WriteLine("   properties are there so plane-reading code is written against the general layout —");
        Console.WriteLine("   GetPlane(bit, group), BytesPerRow from Stride — and needs no change when it is not.");
    }

    // True when every byte of a plane is the same, so the plane separates no row from any other.
    private static bool Uniform(ReadOnlySpan<byte> plane)
    {
        for (int i = 1; i < plane.Length; i++)
        {
            if (plane[i] != plane[0])
            {
                return false;
            }
        }

        return true;
    }

    // Rebuilds one row's vector from its top `keep` planes, leaving the rest of each element's bits zero. This is
    // the client-side equivalent of L2DistanceTransposed's precision argument.
    private static float[] Reconstruct(IQBitColumn column, int row, int keep)
    {
        var rebuilt = new float[column.Dimension];
        for (int element = 0; element < column.Dimension; element++)
        {
            uint bits = 0;
            for (int bit = column.BitWidth - 1; bit >= column.BitWidth - keep; bit--)
            {
                ReadOnlySpan<byte> plane = column.GetPlane(bit);
                byte bitmap = plane[(row * column.BytesPerRow) + column.BytesPerRow - 1 - (element / 8)];
                if ((bitmap & (1 << (element % 8))) != 0)
                {
                    bits |= 1u << bit;
                }
            }

            rebuilt[element] = BitConverter.UInt32BitsToSingle(bits);
        }

        return rebuilt;
    }

    private static string Describe(Type type) => type switch
    {
        _ when type == typeof(float[]) => "float[]",
        _ when type == typeof(double[]) => "double[]",
        _ when type == typeof(sbyte[]) => "sbyte[]",
        _ => type.Name,
    };

    private static string Number(object? value)
        => value is IFormattable formattable ? formattable.ToString("0.######", CultureInfo.InvariantCulture) : "-";

    private static string FirstLine(string message)
    {
        int newline = message.IndexOf('\n');
        string line = newline < 0 ? message : message[..newline];
        if (line.StartsWith("DB::Exception: ", StringComparison.Ordinal))
        {
            line = line["DB::Exception: ".Length..];
        }

        int scope = line.IndexOf(": In scope", StringComparison.Ordinal);
        return scope < 0 ? line : line[..scope];
    }
}
