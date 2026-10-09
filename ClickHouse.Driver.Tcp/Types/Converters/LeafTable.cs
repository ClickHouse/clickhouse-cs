using System;
using System.Collections.Generic;
using System.Net;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The leaf table: for each leaf ClickHouse type, the CLR types that it reads as and writes from, and the converter
/// for each pair. It is the only place that knows a (leaf type, CLR type) pair. A composite only recurses to its
/// leaves. The pairs are the readings and the writes that the leaf codecs offer.
/// </summary>
internal static class LeafTable
{
    private static readonly Dictionary<string, Leaf> Leaves = Build();

    /// <summary>Every leaf, for diagnostics and tests.</summary>
    public static IReadOnlyCollection<Leaf> All => Leaves.Values;

    /// <summary>Finds the leaf for a type name.</summary>
    /// <param name="canonicalName">The name as the codec registry spells it (aliases and case already resolved).</param>
    /// <param name="leaf">The leaf, or null when the name is not a leaf.</param>
    /// <returns>Whether the name is a leaf.</returns>
    public static bool TryGet(string canonicalName, out Leaf leaf) => Leaves.TryGetValue(canonicalName, out leaf);

    private static Dictionary<string, Leaf> Build()
    {
        var leaves = new Dictionary<string, Leaf>(StringComparer.Ordinal);
        Leaf Add(string name, string writeRefusal = null)
        {
            var leaf = new Leaf(name, writeRefusal);
            leaves.Add(name, leaf);
            return leaf;
        }

        // The integers, Bool and the intervals: the stored value, the CLR value and the wire value are the same.
        Same<byte>(Add("UInt8"));
        Same<sbyte>(Add("Int8"));
        Same<ushort>(Add("UInt16"));
        Same<short>(Add("Int16"));
        Same<uint>(Add("UInt32"));
        Same<int>(Add("Int32"));
        Same<ulong>(Add("UInt64"));
        Same<long>(Add("Int64"));
        Same<UInt128>(Add("UInt128"));
        Same<Int128>(Add("Int128"));
        Same<UInt256>(Add("UInt256"));
        Same<Int256>(Add("Int256"));
        Same<bool>(Add("Bool"));
        foreach (string unit in new[] { "Nanosecond", "Microsecond", "Millisecond", "Second", "Minute", "Hour", "Day", "Week", "Month", "Quarter", "Year" })
        {
            Same<long>(Add("Interval" + unit));
        }

        // The floats are interned on their bit patterns. BFloat16 keeps the 16 high bits of a Float32.
        Add("Float32")
            .Read<float>(static _ => Identity<float>())
            .Write<float>(static _ => Float32Writer);
        Add("Float64")
            .Read<double>(static _ => Identity<double>())
            .Write<double>(static _ => Float64Writer);
        Add("BFloat16")
            .Read<float>(static _ => Identity<float>())
            .Write<float>(static _ => BFloat16Writer);

        // The byte-run leaves.
        Add("String")
            .Read<string>(static _ => StringLeafReader<string, Utf8Text>.Instance)
            .Read<byte[]>(static _ => StringLeafReader<byte[], CopiedBytes>.Instance, conversion: true)
            .Write<string>(static _ => TextStringWriter.String)
            .Write<byte[]>(static _ => BytesStringWriter.Instance, conversion: true);
        Add("FixedString")
            .Read<byte[]>(static _ => FixedStringLeafReader<byte[], CopiedBytes>.Instance)
            .Read<string>(static _ => FixedStringLeafReader<string, Utf8Text>.Instance, conversion: true)
            .Write<byte[]>(static codec => new FixedStringBytesWriter(((FixedStringColumnCodec)codec).Size, codec.TypeName));
        Add("JSON")
            .Read<string>(static _ => StringLeafReader<string, Utf8Text>.Instance)
            .Write<string>(static _ => TextStringWriter.Json);

        // The leaves whose decoded column stores the CLR value and whose wire value differs from it.
        Add("Date")
            .Read<DateOnly>(static _ => Identity<DateOnly>())
            .Write<DateOnly>(static _ => DateWriter);
        Add("Date32")
            .Read<DateOnly>(static _ => Identity<DateOnly>())
            .Write<DateOnly>(static _ => Date32Writer);
        Add("UUID")
            .Read<Guid>(static _ => Identity<Guid>())
            .Write<Guid>(static _ => UuidWriter);
        Add("IPv4")
            .Read<IPAddress>(static _ => Identity<IPAddress>())
            .Write<IPAddress>(static _ => IPv4Writer);
        Add("IPv6")
            .Read<IPAddress>(static _ => Identity<IPAddress>())
            .Write<IPAddress>(static _ => IPv6Writer);
        Add("Nothing", writeRefusal: "Values cannot be written to a ClickHouse Nothing column.")
            .Read<object>(static _ => Identity<object>());

        // Times and instants: the decoded column stores the wire count, and the calendar types convert.
        Add("Time")
            .Read<int>(static _ => Identity<int>())
            .Read<TimeSpan>(static _ => new ValueLeafReader<int, TimeSpan, TimeAsTimeSpan>(default), conversion: true)
            .Read<TimeOnly>(static _ => new ValueLeafReader<int, TimeOnly, TimeAsTimeOnly>(default), conversion: true)
            .Write<int>(static _ => IdentityWriter<int>.Instance)
            .Write<TimeSpan>(static _ => new FixedLeafWriter<TimeSpan, int, TimeFromTimeSpan>(default, 0), conversion: true)
            .Write<TimeOnly>(static _ => new FixedLeafWriter<TimeOnly, int, TimeFromTimeOnly>(default, 0), conversion: true);
        Add("Time64")
            .Read<long>(static _ => Identity<long>())
            .Read<TimeSpan>(static codec => new ValueLeafReader<long, TimeSpan, Time64AsTimeSpan>(new(Time64Scale(codec))), conversion: true)
            .Read<TimeOnly>(static codec => new ValueLeafReader<long, TimeOnly, Time64AsTimeOnly>(new(Time64Scale(codec))), conversion: true)
            .Write<long>(static _ => IdentityWriter<long>.Instance)
            .Write<TimeSpan>(static codec => new FixedLeafWriter<TimeSpan, long, Time64FromTimeSpan>(new(Time64Scale(codec)), 0L), conversion: true)
            .Write<TimeOnly>(static codec => new FixedLeafWriter<TimeOnly, long, Time64FromTimeOnly>(new(Time64Scale(codec)), 0L), conversion: true);
        Add("DateTime")
            .Read<uint>(static _ => Identity<uint>())
            .Read<DateTimeOffset>(static codec => new ValueLeafReader<uint, DateTimeOffset, DateTimeAsOffset>(new(DateTimeZone(codec))), conversion: true)
            .Read<DateTime>(static codec => new ValueLeafReader<uint, DateTime, DateTimeAsDateTime>(new(DateTimeZone(codec))), conversion: true)
            .Write<uint>(static _ => IdentityWriter<uint>.Instance)
            .Write<DateTimeOffset>(static _ => new FixedLeafWriter<DateTimeOffset, uint, DateTimeFromOffset>(default, 0u), conversion: true)
            .Write<DateTime>(static codec => new FixedLeafWriter<DateTime, uint, DateTimeFromDateTime>(new(DateTimeZone(codec)), 0u), conversion: true);
        Add("DateTime64")
            .Read<long>(static _ => Identity<long>())
            .Read<DateTimeOffset>(
                static codec => new ValueLeafReader<long, DateTimeOffset, DateTime64AsOffset>(new(DateTime64(codec).Scale, DateTime64(codec).TimeZone)),
                conversion: true)
            .Read<DateTime>(
                static codec => new ValueLeafReader<long, DateTime, DateTime64AsDateTime>(new(DateTime64(codec).Scale, DateTime64(codec).TimeZone)),
                conversion: true)
            .Write<long>(static _ => IdentityWriter<long>.Instance)
            .Write<DateTimeOffset>(
                static codec => new FixedLeafWriter<DateTimeOffset, long, DateTime64FromOffset>(new(DateTime64(codec).Scale, codec.TypeName), 0L),
                conversion: true)
            .Write<DateTime>(
                static codec => new FixedLeafWriter<DateTime, long, DateTime64FromDateTime>(
                    new(DateTime64(codec).Scale, DateTime64(codec).TimeZone, codec.TypeName),
                    0L),
                conversion: true);

        // The enums: the ordinal, or the label. The placeholder is the first declared member, because the server can
        // refuse an ordinal that the type does not declare, even under a NULL.
        EnumOf<sbyte>(Add("Enum8"));
        EnumOf<short>(Add("Enum16"));
        Leaf bareEnum = Add("Enum");
        EnumOf<sbyte>(bareEnum, static codec => codec is EnumColumnCodec<sbyte>);
        EnumOf<short>(bareEnum, static codec => codec is EnumColumnCodec<short>);

        // The decimals: the decoded column stores the CLR value, and the wire value is the mantissa. The precision
        // decides the CLR type of Decimal(P, S).
        DecimalOf<decimal>(Add("Decimal32"));
        DecimalOf<decimal>(Add("Decimal64"));
        DecimalOf<ClickHouseTcpDecimal>(Add("Decimal128"));
        DecimalOf<ClickHouseTcpDecimal>(Add("Decimal256"));
        Leaf decimalLeaf = Add("Decimal");
        DecimalOf<decimal>(decimalLeaf, static codec => codec.ElementType == typeof(decimal));
        DecimalOf<ClickHouseTcpDecimal>(decimalLeaf, static codec => codec.ElementType == typeof(ClickHouseTcpDecimal));

        return leaves;
    }

    private static readonly FixedLeafWriter<float, uint, Float32Bits> Float32Writer = new(default, 0u);
    private static readonly FixedLeafWriter<double, ulong, Float64Bits> Float64Writer = new(default, 0ul);
    private static readonly FixedLeafWriter<float, ushort, BFloat16Bits> BFloat16Writer = new(default, (ushort)0);
    private static readonly FixedLeafWriter<DateOnly, ushort, DateDays> DateWriter = new(default, (ushort)0);
    private static readonly FixedLeafWriter<DateOnly, int, Date32Days> Date32Writer = new(default, 0);
    private static readonly FixedLeafWriter<Guid, UInt128, UuidBytes> UuidWriter = new(default, UInt128.Zero);
    private static readonly FixedLeafWriter<IPAddress, uint, IPv4Number> IPv4Writer = new(default, 0u);
    private static readonly FixedLeafWriter<IPAddress, UInt128, IPv6Bytes> IPv6Writer = new(default, UInt128.Zero);

    private static void Same<T>(Leaf leaf)
        where T : unmanaged, IEquatable<T>
        => leaf.Read<T>(static _ => Identity<T>()).Write<T>(static _ => IdentityWriter<T>.Instance);

    private static void EnumOf<T>(Leaf leaf, Func<IColumnCodec, bool> applies = null)
        where T : unmanaged, IEquatable<T>, System.Numerics.IBinaryInteger<T>
        => leaf
            .Read<T>(static _ => Identity<T>(), applies: applies)
            .Read<string>(static codec => new ValueLeafReader<T, string, EnumAsLabel<T>>(new(((EnumColumnCodec<T>)codec).Members)), conversion: true, applies: applies)
            .Write<T>(static codec => new FixedLeafWriter<T, T, IdentityWrite<T>>(default, (T)codec.NullPlaceholder), applies: applies)
            .Write<string>(
                static codec => new FixedLeafWriter<string, T, EnumFromLabel<T>>(new((EnumColumnCodec<T>)codec), (T)codec.NullPlaceholder),
                conversion: true,
                applies: applies);

    private static void DecimalOf<TValue>(Leaf leaf, Func<IColumnCodec, bool> applies = null)
        => leaf
            .Read<TValue>(static _ => Identity<TValue>(), applies: applies)
            .Write<TValue>(static codec => (ColumnWriter<TValue>)DecimalWriter(codec), applies: applies);

    private static ColumnWriter DecimalWriter(IColumnCodec codec) => codec switch
    {
        DecimalColumnCodec<int, decimal> d => new FixedLeafWriter<decimal, int, DecimalMantissa<decimal, int>>(new(d), 0),
        DecimalColumnCodec<long, decimal> d => new FixedLeafWriter<decimal, long, DecimalMantissa<decimal, long>>(new(d), 0L),
        DecimalColumnCodec<Int128, ClickHouseTcpDecimal> d =>
            new FixedLeafWriter<ClickHouseTcpDecimal, Int128, DecimalMantissa<ClickHouseTcpDecimal, Int128>>(new(d), Int128.Zero),
        DecimalColumnCodec<Int256, ClickHouseTcpDecimal> d =>
            new FixedLeafWriter<ClickHouseTcpDecimal, Int256, DecimalMantissa<ClickHouseTcpDecimal, Int256>>(new(d), Int256.Zero),
        _ => throw new InvalidOperationException($"The codec for '{codec.TypeName}' is {codec.GetType()}, which is not a decimal codec."),
    };

    private static ColumnReader<T> Identity<T>() => IdentityReader<T>.Instance;

    private static int Time64Scale(IColumnCodec codec) => ((Time64ColumnCodec)codec).Scale;

    private static ResolvedTimeZone DateTimeZone(IColumnCodec codec) => ((DateTimeColumnCodec)codec).TimeZone;

    private static DateTime64ColumnCodec DateTime64(IColumnCodec codec) => (DateTime64ColumnCodec)codec;

    // One identity reader for each stored type. It has no state, so every derivation shares it.
    private static class IdentityReader<T>
    {
        public static readonly ValueLeafReader<T, T, Identity<T>> Instance = new(default);
    }

    // One identity writer for each wire type whose placeholder is the default value.
    private static class IdentityWriter<T>
        where T : unmanaged, IEquatable<T>
    {
        public static readonly FixedLeafWriter<T, T, IdentityWrite<T>> Instance = new(default, default);
    }
}

/// <summary>One leaf ClickHouse type and its (CLR type, converter) pairs in each direction.</summary>
internal sealed class Leaf
{
    private readonly List<LeafPair> reads = new();
    private readonly List<LeafPair> writes = new();
    private readonly string writeRefusal;

    /// <summary>Initializes a leaf with no pairs.</summary>
    /// <param name="name">The canonical ClickHouse type name.</param>
    /// <param name="writeRefusal">The refusal for every write, for a leaf that has no write; else null.</param>
    public Leaf(string name, string writeRefusal)
    {
        Name = name;
        this.writeRefusal = writeRefusal;
    }

    /// <summary>The canonical ClickHouse type name.</summary>
    public string Name { get; }

    /// <summary>The read pairs.</summary>
    public IReadOnlyList<LeafPair> Reads => reads;

    /// <summary>The write pairs.</summary>
    public IReadOnlyList<LeafPair> Writes => writes;

    /// <summary>The CLR types that one instance of the leaf reads as, in table order.</summary>
    /// <param name="codec">The codec of the instance (it carries the parameters, such as the width of a decimal).</param>
    /// <returns>The CLR types.</returns>
    public IEnumerable<Type> ReadTypes(IColumnCodec codec) => TypesFor(reads, codec);

    /// <summary>The CLR types that one instance of the leaf writes from, in table order.</summary>
    /// <param name="codec">The codec of the instance.</param>
    /// <returns>The CLR types.</returns>
    public IEnumerable<Type> WriteTypes(IColumnCodec codec) => TypesFor(writes, codec);

    /// <summary>The reader for one instance of the leaf and one CLR type, or null when the leaf does not read as it.</summary>
    /// <param name="codec">The codec of the instance.</param>
    /// <param name="clrType">The CLR type to read as.</param>
    /// <returns>A <see cref="ColumnReader{T}"/> of <paramref name="clrType"/>, or null.</returns>
    public ColumnReader CreateReader(IColumnCodec codec, Type clrType) => (ColumnReader)Find(reads, codec, clrType)?.Create(codec);

    /// <summary>The writer for one instance of the leaf and one CLR type, or null when the leaf does not write from it.</summary>
    /// <param name="codec">The codec of the instance.</param>
    /// <param name="clrType">The CLR type to write from.</param>
    /// <returns>A <see cref="ColumnWriter{T}"/> of <paramref name="clrType"/>, or null.</returns>
    public ColumnWriter CreateWriter(IColumnCodec codec, Type clrType) => (ColumnWriter)Find(writes, codec, clrType)?.Create(codec);

    /// <summary>Why the leaf does not read as <paramref name="clrType"/>, in the style of the codec messages.</summary>
    /// <param name="node">The leaf type as written.</param>
    /// <param name="codec">The codec of the instance.</param>
    /// <param name="clrType">The CLR type that was asked for.</param>
    /// <returns>The reason.</returns>
    public string ReadRefusal(TypeNode node, IColumnCodec codec, Type clrType)
        => $"'{node}' cannot be read as {clrType}. It reads as: {string.Join(", ", ReadTypes(codec))}.";

    /// <summary>Why the leaf does not write from <paramref name="clrType"/>, in the style of the codec messages.</summary>
    /// <param name="node">The leaf type as written.</param>
    /// <param name="codec">The codec of the instance.</param>
    /// <param name="clrType">The CLR type that was asked for.</param>
    /// <returns>The reason.</returns>
    public string WriteRefusal(TypeNode node, IColumnCodec codec, Type clrType)
        => writeRefusal ?? $"'{node}' cannot be written from {clrType}. It is written from: {string.Join(", ", WriteTypes(codec))}.";

    /// <summary>Adds a read pair.</summary>
    /// <typeparam name="T">The CLR type.</typeparam>
    /// <param name="create">Builds the reader for one instance of the leaf.</param>
    /// <param name="conversion">Whether the pair converts, rather than give the value that the column stores.</param>
    /// <param name="applies">Whether the pair applies to one instance, or null when it applies to every instance.</param>
    /// <returns>This leaf.</returns>
    public Leaf Read<T>(Func<IColumnCodec, ColumnReader<T>> create, bool conversion = false, Func<IColumnCodec, bool> applies = null)
    {
        reads.Add(new LeafPair(typeof(T), conversion, applies, create));
        return this;
    }

    /// <summary>Adds a write pair.</summary>
    /// <typeparam name="T">The CLR type.</typeparam>
    /// <param name="create">Builds the writer for one instance of the leaf.</param>
    /// <param name="conversion">Whether the pair converts, rather than write the value that the column stores.</param>
    /// <param name="applies">Whether the pair applies to one instance, or null when it applies to every instance.</param>
    /// <returns>This leaf.</returns>
    public Leaf Write<T>(Func<IColumnCodec, ColumnWriter<T>> create, bool conversion = false, Func<IColumnCodec, bool> applies = null)
    {
        writes.Add(new LeafPair(typeof(T), conversion, applies, create));
        return this;
    }

    private static IEnumerable<Type> TypesFor(List<LeafPair> pairs, IColumnCodec codec)
    {
        foreach (LeafPair pair in pairs)
        {
            if (pair.AppliesTo(codec))
            {
                yield return pair.ClrType;
            }
        }
    }

    private static LeafPair Find(List<LeafPair> pairs, IColumnCodec codec, Type clrType)
    {
        foreach (LeafPair pair in pairs)
        {
            if (pair.ClrType == clrType && pair.AppliesTo(codec))
            {
                return pair;
            }
        }

        return null;
    }
}

/// <summary>One (leaf type, CLR type) pair in one direction.</summary>
internal sealed class LeafPair
{
    private readonly Func<IColumnCodec, bool> applies;
    private readonly Func<IColumnCodec, object> create;

    /// <summary>Initializes a pair.</summary>
    /// <param name="clrType">The CLR type.</param>
    /// <param name="isConversion">Whether the pair converts.</param>
    /// <param name="applies">Whether the pair applies to one instance, or null for every instance.</param>
    /// <param name="create">Builds the converter for one instance.</param>
    public LeafPair(Type clrType, bool isConversion, Func<IColumnCodec, bool> applies, Func<IColumnCodec, object> create)
    {
        ClrType = clrType;
        IsConversion = isConversion;
        this.applies = applies;
        this.create = create;
    }

    /// <summary>The CLR type.</summary>
    public Type ClrType { get; }

    /// <summary>Whether the pair converts, rather than give or take the value that the decoded column stores.</summary>
    public bool IsConversion { get; }

    /// <summary>Whether the pair applies to the instance that <paramref name="codec"/> describes.</summary>
    /// <param name="codec">The codec of the instance.</param>
    /// <returns>Whether it applies.</returns>
    public bool AppliesTo(IColumnCodec codec) => applies is null || applies(codec);

    /// <summary>Builds the converter for one instance.</summary>
    /// <param name="codec">The codec of the instance.</param>
    /// <returns>The converter.</returns>
    public object Create(IColumnCodec codec) => create(codec);
}
