using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Builds the codec for a ClickHouse type string, given the resolution context. A factory consumes the parsed
/// <see cref="TypeNode"/> arguments (for parameterized types such as <c>Decimal(P,S)</c> or <c>Enum8(...)</c>)
/// and the <see cref="ResolveContext"/> (for timezone-bearing types that fall back to the server timezone).
/// </summary>
/// <param name="node">The parsed type, whose <see cref="TypeNode.Arguments"/> carry any type parameters.</param>
/// <param name="context">The resolution context (e.g. the server timezone).</param>
/// <param name="registry">The registry itself, so a composite type can resolve its child type arguments.</param>
/// <returns>The codec for the type.</returns>
internal delegate IColumnCodec CodecFactory(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry);

/// <summary>
/// Resolves a ClickHouse type string to the codec that reads/writes it. The type string is parsed into a
/// <see cref="TypeNode"/> and dispatched on its base name to a <see cref="CodecFactory"/>; simple types register
/// a factory that returns a shared singleton, while parameterized and timezone-bearing types build a codec
/// instance from the parsed arguments and the resolution context.
/// </summary>
internal sealed class ColumnCodecRegistry
{
    /// <summary>The default registry with the built-in codecs.</summary>
    public static readonly ColumnCodecRegistry Default = CreateDefault();

    private readonly Dictionary<string, CodecFactory> byName;

    private ColumnCodecRegistry(Dictionary<string, CodecFactory> byName) => this.byName = byName;

    /// <summary>Resolves the codec for a ClickHouse type string.</summary>
    /// <param name="typeString">The type string from a column header (e.g. <c>UInt64</c>, <c>DateTime('UTC')</c>).</param>
    /// <param name="context">The resolution context (server timezone, etc.); use <see cref="ResolveContext.ForWrite"/> on the write path.</param>
    /// <returns>The codec for that type.</returns>
    /// <exception cref="FormatException"><paramref name="typeString"/> is malformed.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed but not yet supported by this client.</exception>
    public IColumnCodec Resolve(string typeString, in ResolveContext context)
    {
        TypeNode node = TypeParser.Parse(typeString);
        return ResolveNode(node, in context);
    }

    /// <summary>
    /// Resolves the codec for an already-parsed type node. Composite codecs (e.g. <c>Nullable(T)</c>) call this
    /// to build the codecs for their child type arguments without re-serializing and re-parsing a type string.
    /// </summary>
    /// <param name="node">The parsed type node.</param>
    /// <param name="context">The resolution context (server timezone, etc.).</param>
    /// <returns>The codec for that type.</returns>
    /// <exception cref="NotSupportedException">The type is well-formed but not yet supported by this client.</exception>
    public IColumnCodec ResolveNode(TypeNode node, in ResolveContext context)
    {
        if (byName.TryGetValue(node.Name, out CodecFactory factory))
        {
            return factory(node, in context, this);
        }

        throw new NotSupportedException($"ClickHouse type '{node}' is not supported by this client yet.");
    }

    private static ColumnCodecRegistry CreateDefault()
    {
        var byName = new Dictionary<string, CodecFactory>(StringComparer.Ordinal);

        // A type whose codec ignores both the parsed arguments and the resolution context registers a single
        // shared instance, wrapped in a factory that returns it unconditionally.
        void AddConstant(IColumnCodec codec) => byName[codec.TypeName] = (TypeNode _, in ResolveContext _, ColumnCodecRegistry _) => codec;

        void AddFactory(string name, CodecFactory factory) => byName[name] = factory;

        AddConstant(new FixedWidthColumnCodec<byte>("UInt8"));
        AddConstant(new FixedWidthColumnCodec<sbyte>("Int8"));
        AddConstant(new FixedWidthColumnCodec<ushort>("UInt16"));
        AddConstant(new FixedWidthColumnCodec<short>("Int16"));
        AddConstant(new FixedWidthColumnCodec<uint>("UInt32"));
        AddConstant(new FixedWidthColumnCodec<int>("Int32"));
        AddConstant(new FixedWidthColumnCodec<ulong>("UInt64"));
        AddConstant(new FixedWidthColumnCodec<long>("Int64"));
        AddConstant(new FixedWidthColumnCodec<UInt128>("UInt128"));
        AddConstant(new FixedWidthColumnCodec<Int128>("Int128"));
        AddConstant(new FixedWidthColumnCodec<UInt256>("UInt256"));
        AddConstant(new FixedWidthColumnCodec<Int256>("Int256"));

        // IEEE-754 floats and the widened brain-float.
        AddConstant(new FixedWidthColumnCodec<float>("Float32"));
        AddConstant(new FixedWidthColumnCodec<double>("Float64"));
        AddConstant(BFloat16ColumnCodec.Instance);

        AddConstant(new FixedWidthColumnCodec<bool>("Bool"));
        AddConstant(StringColumnCodec.Instance);

        // FixedString(N): N contiguous bytes per row, the length parsed from the type argument.
        AddFactory("FixedString", static (TypeNode node, in ResolveContext _, ColumnCodecRegistry _) => FixedStringColumnCodec.Create(node));

        // Dates and times.
        AddConstant(DateColumnCodec.Instance);
        AddConstant(Date32ColumnCodec.Instance);
        AddConstant(TimeColumnCodec.Instance);
        AddConstant(UuidColumnCodec.Instance);
        AddConstant(IPv4ColumnCodec.Instance);
        AddConstant(IPv6ColumnCodec.Instance);
        AddConstant(NothingColumnCodec.Instance);

        // Interval<Unit>: the underlying Int64 count, one registration per unit; the unit is kept in the name.
        foreach (string unit in new[] { "Nanosecond", "Microsecond", "Millisecond", "Second", "Minute", "Hour", "Day", "Week", "Month", "Quarter", "Year" })
        {
            AddConstant(new FixedWidthColumnCodec<long>("Interval" + unit));
        }

        // Timezone-bearing types resolve their offset from the type string or, failing that, the session timezone.
        AddFactory("DateTime", static (TypeNode node, in ResolveContext context, ColumnCodecRegistry _) => DateTimeColumnCodec.Create(node, context.ServerTimezone));
        AddFactory("DateTime64", static (TypeNode node, in ResolveContext context, ColumnCodecRegistry _) => DateTime64ColumnCodec.Create(node, context.ServerTimezone));
        AddFactory("Time64", static (TypeNode node, in ResolveContext _, ColumnCodecRegistry _) => Time64ColumnCodec.Create(node));

        // Enum aliases: raw underlying Int8/Int16 ordinal; the label map is parsed and retained by the codec.
        AddFactory("Enum8", static (TypeNode node, in ResolveContext _, ColumnCodecRegistry _) => Enum8ColumnCodec.Create(node));
        AddFactory("Enum16", static (TypeNode node, in ResolveContext _, ColumnCodecRegistry _) => Enum16ColumnCodec.Create(node));

        // Decimal(P, S) and the fixed-width aliases share the width-by-precision codec factory.
        foreach (string name in new[] { "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256" })
        {
            AddFactory(name, static (TypeNode node, in ResolveContext _, ColumnCodecRegistry _) => DecimalColumnCodec.Create(node));
        }

        // Nullable(T) wraps a child codec, resolved recursively through the registry.
        AddFactory("Nullable", static (TypeNode node, in ResolveContext context, ColumnCodecRegistry registry) => NullableColumnCodec.Create(node, context, registry));

        // Array(T) wraps a child codec (offsets + flattened element values), resolved recursively.
        AddFactory("Array", static (TypeNode node, in ResolveContext context, ColumnCodecRegistry registry) => ArrayColumnCodec.Create(node, context, registry));

        // Tuple(T1, ..., Tn) serializes its elements as N independent child columns (all prefixes then all
        // bodies, in order); each element codec is resolved recursively and element names, if any, are kept.
        AddFactory("Tuple", static (TypeNode node, in ResolveContext context, ColumnCodecRegistry registry) => TupleColumnCodec.Create(node, context, registry));

        // Map(K, V) is byte-identical to Array(Tuple(K, V)): offsets + a keys stream + a values stream. The key
        // and value codecs are resolved recursively; each row surfaces as a KeyValuePair<K, V>[].
        AddFactory("Map", static (TypeNode node, in ResolveContext context, ColumnCodecRegistry registry) => MapColumnCodec.Create(node, context, registry));

        // Nested(...) as a single wire column (flatten_nested = 0) is byte-identical to Array(Tuple(...)): the same
        // offsets stream plus each field's flattened stream, differing only in the type string, which keeps the
        // field names. It has a dedicated arity-agnostic codec (not Array(Tuple) reuse) surfacing a columnar
        // NestedColumn, so it is not bound by the tuple's element-count cap.
        AddFactory("Nested", static (TypeNode node, in ResolveContext context, ColumnCodecRegistry registry) => NestedColumnCodec.Create(node, context, registry));

        return new ColumnCodecRegistry(byName);
    }
}
