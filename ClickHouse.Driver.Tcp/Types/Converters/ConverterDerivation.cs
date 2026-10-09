using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>The direction of a converter: read from the wire, or write to it.</summary>
internal enum ConversionDirection
{
    /// <summary>A <see cref="ColumnReader{T}"/>: from a decoded column to CLR values.</summary>
    Read,

    /// <summary>A <see cref="ColumnWriter{T}"/>: from CLR values to the wire.</summary>
    Write,
}

/// <summary>The result of a derivation: a converter tree, or the reason that there is none.</summary>
internal sealed class Derivation
{
    private Derivation(object converter, string refusal)
    {
        Converter = converter;
        Refusal = refusal;
    }

    /// <summary>
    /// The converter tree: a <see cref="ColumnReader{T}"/> or a <see cref="ColumnWriter{T}"/> of the CLR type that was
    /// asked for. Null when the derivation refused.
    /// </summary>
    public object Converter { get; }

    /// <summary>Why there is no converter, in the style of the codec messages. Null when the derivation succeeded.</summary>
    public string Refusal { get; }

    /// <summary>Whether the derivation gave a converter.</summary>
    public bool Succeeded => Converter is not null;

    /// <summary>A derivation that gave <paramref name="converter"/>.</summary>
    /// <param name="converter">The converter tree.</param>
    /// <returns>The derivation.</returns>
    public static Derivation Of(object converter) => new(converter ?? throw new ArgumentNullException(nameof(converter)), null);

    /// <summary>A derivation that refused.</summary>
    /// <param name="reason">Why there is no converter.</param>
    /// <returns>The derivation.</returns>
    public static Derivation Refused(string reason) => new(null, reason ?? throw new ArgumentNullException(nameof(reason)));
}

/// <summary>
/// <c>Derive(type, context, T, direction)</c>: builds the converter tree that reads a ClickHouse type as a CLR type,
/// or writes it from one, or the reason that there is none. The result is cached by (type, timezone, CLR type,
/// direction), so later calls with the same key get the same tree, on every thread.
/// </summary>
/// <remarks>
/// <para>
/// The leaf table (<see cref="LeafTable"/>) is the only place that knows a (leaf type, CLR type) pair. A composite
/// type recurses into its arguments and wraps the trees of its children in a combinator.
/// </para>
/// <para>
/// A derivation can need dynamic code: a combinator over a CLR type that is known only at run time is a generic type
/// that the derivation closes with <see cref="Type.MakeGenericType"/>, and <see cref="ColumnReader.Emit"/> builds an
/// expression tree. The bulk <see cref="BoundReader{T}.Fill"/> path does not compile code.
/// </para>
/// </remarks>
internal sealed class ConverterDerivation
{
    // The cache holds trees for each session timezone that a caller uses, so it has a limit. The count check does not
    // lock, so the cache can pass the limit by a few entries when callers race. A tree that is not cached is correct.
    private const int MaxCachedDerivations = 1024;

    private readonly ColumnCodecRegistry registry;

    private readonly ConcurrentDictionary<(string Type, string Timezone, Type ClrType, ConversionDirection Direction), Derivation> cache = new();

    /// <summary>Initializes a derivation over the codecs of one registry.</summary>
    /// <param name="registry">The registry that validates the type strings and resolves the leaf codecs.</param>
    public ConverterDerivation(ColumnCodecRegistry registry) => this.registry = registry ?? throw new ArgumentNullException(nameof(registry));

    /// <summary>The derivation over <see cref="ColumnCodecRegistry.Default"/>.</summary>
    public static ConverterDerivation Default { get; } = new(ColumnCodecRegistry.Default);

    /// <summary>Derives the converter tree for one ClickHouse type, one CLR type and one direction.</summary>
    /// <param name="type">The ClickHouse type string, for example <c>Nullable(DateTime('UTC'))</c>.</param>
    /// <param name="context">The resolution context. Its session timezone applies to a <c>DateTime</c> with no timezone of its own.</param>
    /// <param name="clrType">The CLR type of one value: the type to read as, or the type to write from.</param>
    /// <param name="direction">Read or write.</param>
    /// <returns>The tree, or the reason that there is none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="type"/> or <paramref name="clrType"/> is null.</exception>
    /// <exception cref="FormatException"><paramref name="type"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed, but this client does not support it.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public Derivation Derive(string type, in ResolveContext context, Type clrType, ConversionDirection direction)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(clrType);

        var key = (type, context.ServerTimezone ?? string.Empty, clrType, direction);
        if (cache.TryGetValue(key, out Derivation cached))
        {
            return cached;
        }

        Derivation derived = DeriveUncached(type, in context, clrType, direction);

        // GetOrAdd, so callers that race on one key all get the tree that the cache keeps.
        return cache.Count < MaxCachedDerivations ? cache.GetOrAdd(key, derived) : derived;
    }

    /// <summary>The reader for one ClickHouse type and <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">The CLR type to read as.</typeparam>
    /// <param name="type">The ClickHouse type string.</param>
    /// <param name="context">The resolution context.</param>
    /// <returns>The reader.</returns>
    /// <exception cref="InvalidCastException">The type cannot be read as <typeparamref name="T"/>. The message gives the reason.</exception>
    /// <exception cref="FormatException"><paramref name="type"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed, but this client does not support it.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public ColumnReader<T> Reader<T>(string type, in ResolveContext context)
    {
        Derivation derivation = Derive(type, in context, typeof(T), ConversionDirection.Read);
        return derivation.Succeeded ? (ColumnReader<T>)derivation.Converter : throw new InvalidCastException(derivation.Refusal);
    }

    /// <summary>The writer for one ClickHouse type and <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">The CLR type to write from.</typeparam>
    /// <param name="type">The ClickHouse type string.</param>
    /// <param name="context">The resolution context.</param>
    /// <returns>The writer.</returns>
    /// <exception cref="InvalidCastException">The type cannot be written from <typeparamref name="T"/>. The message gives the reason.</exception>
    /// <exception cref="FormatException"><paramref name="type"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed, but this client does not support it.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public ColumnWriter<T> Writer<T>(string type, in ResolveContext context)
    {
        Derivation derivation = Derive(type, in context, typeof(T), ConversionDirection.Write);
        return derivation.Succeeded ? (ColumnWriter<T>)derivation.Converter : throw new InvalidCastException(derivation.Refusal);
    }

    /// <summary>
    /// Derives the tree for one node of a parsed type. A combinator can call this for each of its children, with
    /// the root of the column type, so that a refusal inside a composite names the column type.
    /// </summary>
    /// <param name="node">The node to derive.</param>
    /// <param name="root">The root of the column type.</param>
    /// <param name="context">The resolution context.</param>
    /// <param name="clrType">The CLR type of one value at this node.</param>
    /// <param name="direction">Read or write.</param>
    /// <returns>The tree, or the reason that there is none.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    internal Derivation DeriveNode(TypeNode node, TypeNode root, in ResolveContext context, Type clrType, ConversionDirection direction)
    {
        string name = registry.TryCanonicalName(node.Name, out string canonical) ? canonical : node.Name;
        switch (name)
        {
            // The function name is only in the type string: the column is its value type.
            case "SimpleAggregateFunction":
                return DeriveNode(node.Arguments[1], root, in context, clrType, direction);

            default:
                if (LeafTable.TryGet(name, out Leaf leaf))
                {
                    return DeriveLeaf(leaf, node, root, in context, clrType, direction);
                }

                return Refuse(node, root, $"'{node}' has no converter for {clrType}.");
        }
    }

    // Validates the whole type first, so a malformed or an unsupported type throws the same exception as a codec
    // resolution, with the same message.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveUncached(string type, in ResolveContext context, Type clrType, ConversionDirection direction)
    {
        registry.Resolve(type, in context);
        TypeNode root = TypeParser.Parse(type);
        return DeriveNode(root, root, in context, clrType, direction);
    }

    private Derivation DeriveLeaf(Leaf leaf, TypeNode node, TypeNode root, in ResolveContext context, Type clrType, ConversionDirection direction)
    {
        IColumnCodec codec = registry.ResolveNode(node, in context);
        if (direction == ConversionDirection.Read)
        {
            ColumnReader reader = leaf.CreateReader(codec, clrType);
            return reader is not null ? Derivation.Of(reader) : Refuse(node, root, leaf.ReadRefusal(node, codec, clrType));
        }

        ColumnWriter writer = leaf.CreateWriter(codec, clrType);
        return writer is not null ? Derivation.Of(writer) : Refuse(node, root, leaf.WriteRefusal(node, codec, clrType));
    }

    // A refusal inside a composite also names the column type, as a codec resolution does for an unsupported child.
    private static Derivation Refuse(TypeNode node, TypeNode root, string reason)
        => Derivation.Refused(ReferenceEquals(node, root) ? reason : $"{reason} It is inside the column type '{root}'.");
}
