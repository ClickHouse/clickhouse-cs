using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>The write combinators of the derivation.</summary>
internal sealed partial class ConverterDerivation
{
    private static readonly Type[] TupleWriterDefinitions =
    {
        null,
        typeof(TupleWriter<>),
        typeof(TupleWriter<,>),
        typeof(TupleWriter<,,>),
        typeof(TupleWriter<,,,>),
        typeof(TupleWriter<,,,,>),
        typeof(TupleWriter<,,,,,>),
        typeof(TupleWriter<,,,,,,>),
    };

    /// <summary>
    /// Whether a column of the type holds NULL: <c>Nullable(X)</c>, <c>LowCardinality(Nullable(X))</c>, <c>Variant</c>,
    /// <c>Dynamic</c> and <c>Geometry</c>, also as the value type of a <c>SimpleAggregateFunction</c>.
    /// </summary>
    /// <param name="type">The ClickHouse type string. It must be well formed.</param>
    /// <returns>Whether the type holds NULL.</returns>
    public bool HoldsNull(string type) => HoldsNull(TypeParser.Parse(type));

    private bool HoldsNull(TypeNode node)
    {
        string name = registry.TryCanonicalName(node.Name, out string canonical) ? canonical : node.Name;
        return name switch
        {
            "Nullable" or "Variant" or "Dynamic" or "Geometry" => true,
            "LowCardinality" => node.Arguments.Count == 1 && HoldsNull(node.Arguments[0]),
            "SimpleAggregateFunction" => node.Arguments.Count == 2 && HoldsNull(node.Arguments[1]),
            _ => false,
        };
    }

    // A composite node, or a node that is written only from its canonical type.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveCompositeWrite(string name, TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        switch (name)
        {
            case "Nullable":
                return DeriveNullableWrite(node, root, in context, clrType);

            case "LowCardinality":
                return DeriveLowCardinalityWrite(node, root, in context, clrType);

            case "Array":
                return DeriveArrayWrite(node, root, in context, clrType);

            case "Map":
                return DeriveMapWrite(node, root, in context, clrType);

            case "Tuple" when node.Arguments.Count > 0:
                return DeriveTupleWrite(node, root, in context, clrType);

            case "Tuple":
                return RefuseOtherThanCanonical(node, root, in context, clrType) ?? Derivation.Of(EmptyTupleWriter.Instance);

            case "Variant":
                return DeriveVariantWrite(node, root, in context, clrType, node.ToString());

            case "Dynamic":
                return DeriveDynamicWrite(node, root, in context, clrType);

            case "QBit":
                return DeriveQBitWrite(node, root, in context, clrType);

            // A Nested column is written from the column that a query of the same type read, and from nothing else.
            case "Nested":
                return Refuse(node, root, $"'{node}' cannot be written from {clrType}. No column built from a CLR element type can fill it; insert a column of the same type that a query read.");

            default:
                if (GeoColumnCodecs.TryGetStructure(name, out TypeNode structure))
                {
                    return DeriveGeoWrite(name, node, structure, root, in context, clrType);
                }

                return Refuse(node, root, $"'{node}' cannot be written from {clrType}.");
        }
    }

    // Nullable(X): T? lifts a value type, and a reference type holds the NULL itself.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveNullableWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        TypeNode innerNode = node.Arguments[0];
        Type value = Nullable.GetUnderlyingType(clrType);
        if (value is not null)
        {
            Derivation inner = DeriveNode(innerNode, root, in context, value, ConversionDirection.Write);
            return inner.Succeeded ? Wrap(typeof(NullableValueWriter<>), value, needsColumn: false, inner.Converter) : inner;
        }

        if (clrType.IsValueType)
        {
            return Refuse(node, root, $"'{node}' cannot be written from {clrType}, which cannot hold NULL. It is written from a nullable type.");
        }

        Derivation reference = DeriveNode(innerNode, root, in context, clrType, ConversionDirection.Write);
        return reference.Succeeded ? Wrap(typeof(NullableReferenceWriter<>), clrType, needsColumn: false, reference.Converter) : reference;
    }

    // LowCardinality(X) over the leaf X: the dictionary interns the canonical values of the leaf. The dictionary of
    // LowCardinality(Nullable(X)) is a column of the bare X, with the NULL slot, so the derivation writes X.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveLowCardinalityWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        TypeNode innerNode = node.Arguments[0];
        bool nullable = innerNode.Name == "Nullable";
        Type leafType = clrType;
        Type unwrap;
        if (!nullable)
        {
            unwrap = typeof(PlainDictionaryValue<>).MakeGenericType(clrType);
        }
        else
        {
            innerNode = innerNode.Arguments[0];
            Type value = Nullable.GetUnderlyingType(clrType);
            if (value is not null)
            {
                leafType = value;
                unwrap = typeof(LiftedDictionaryValue<>).MakeGenericType(value);
            }
            else if (clrType.IsValueType)
            {
                return Refuse(node, root, $"'{node}' cannot be written from {clrType}, which cannot hold NULL. It is written from a nullable type.");
            }
            else
            {
                unwrap = typeof(ReferenceDictionaryValue<>).MakeGenericType(clrType);
            }
        }

        // The JSON leaf has no dictionary: its serialization is not the one of a dictionary entry.
        if (registry.TryCanonicalName(innerNode.Name, out string innerName) && innerName == "JSON")
        {
            return Refuse(node, root, $"'{node}' cannot be written from {clrType}: a JSON value cannot be a LowCardinality dictionary entry.");
        }

        Derivation leaf = DeriveNode(innerNode, root, in context, leafType, ConversionDirection.Write);
        if (!leaf.Succeeded)
        {
            return leaf;
        }

        if (TryGetBase(leaf.Converter.GetType(), typeof(FixedLeafWriter<,>), out Type fixedLeaf))
        {
            Type canonical = fixedLeaf.GetGenericArguments()[1];
            return Derivation.Of(Activator.CreateInstance(
                typeof(FixedDictionaryWriter<,,,>).MakeGenericType(clrType, leafType, canonical, unwrap),
                leaf.Converter,
                nullable));
        }

        if (TryGetBase(leaf.Converter.GetType(), typeof(BytesLeafWriter<>), out _))
        {
            return Derivation.Of(Activator.CreateInstance(
                typeof(BytesDictionaryWriter<,,>).MakeGenericType(clrType, leafType, unwrap),
                leaf.Converter,
                nullable));
        }

        return Refuse(node, root, $"'{node}' cannot be written from {clrType}: '{innerNode}' is not a type whose values a LowCardinality dictionary holds.");
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveArrayWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        if (!clrType.IsSZArray)
        {
            return Refuse(node, root, $"'{node}' cannot be written from {clrType}. It is written from an array.");
        }

        Type element = clrType.GetElementType();
        Derivation inner = DeriveNode(node.Arguments[0], root, in context, element, ConversionDirection.Write);
        return inner.Succeeded ? Wrap(typeof(ArrayWriter<>), element, needsColumn: false, inner.Converter) : inner;
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveMapWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        Type pair = clrType.IsSZArray ? clrType.GetElementType() : null;
        if (pair is null || !pair.IsGenericType || pair.GetGenericTypeDefinition() != typeof(KeyValuePair<,>))
        {
            return Refuse(node, root, $"'{node}' cannot be written from {clrType}. It is written from an array of KeyValuePair<TKey, TValue>.");
        }

        Type[] arguments = pair.GetGenericArguments();
        Derivation key = DeriveNode(node.Arguments[0], root, in context, arguments[0], ConversionDirection.Write);
        if (!key.Succeeded)
        {
            return key;
        }

        Derivation value = DeriveNode(node.Arguments[1], root, in context, arguments[1], ConversionDirection.Write);
        if (!value.Succeeded)
        {
            return value;
        }

        return Derivation.Of(Activator.CreateInstance(typeof(MapWriter<,>).MakeGenericType(arguments), key.Converter, value.Converter));
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveTupleWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        (string Name, TypeNode Type)[] elements = NamedElementParser.Split(node);
        int arity = elements.Length;
        if (!clrType.IsGenericType || clrType.GetGenericTypeDefinition() != ValueTupleDefinitions[arity])
        {
            return Refuse(node, root, $"'{node}' cannot be written from {clrType}. It is written from a ValueTuple of {arity} element(s).");
        }

        Type[] arguments = clrType.GetGenericArguments();
        var fields = new object[arity];
        for (int i = 0; i < arity; i++)
        {
            Derivation field = DeriveNode(elements[i].Type, root, in context, arguments[i], ConversionDirection.Write);
            if (!field.Succeeded)
            {
                return field;
            }

            fields[i] = field.Converter;
        }

        return Derivation.Of(Activator.CreateInstance(TupleWriterDefinitions[arity].MakeGenericType(arguments), fields));
    }

    // Variant(...) from object. Each alternative must be written from its canonical type, as a Variant whose
    // alternative cannot be written (Nothing) is refused before a write starts. The writer derives the alternatives for
    // the other CLR types of the values when a write meets them. typeName: the type that the messages name.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveVariantWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType, string typeName)
    {
        if (clrType != typeof(object))
        {
            return Refuse(node, root, $"'{node}' cannot be written from {clrType}. It is written from: {typeof(object)}.");
        }

        int count = node.Arguments.Count;
        var codecs = new IColumnCodec[count];
        var canonical = new VariantChild[count];
        var nodes = new TypeNode[count];
        for (int i = 0; i < count; i++)
        {
            nodes[i] = node.Arguments[i];
            codecs[i] = registry.ResolveNode(nodes[i], in context);
            Derivation alternative = DeriveNode(nodes[i], root, in context, codecs[i].ElementType, ConversionDirection.Write);
            if (!alternative.Succeeded)
            {
                return alternative;
            }

            canonical[i] = Child(codecs[i].ElementType, alternative.Converter);
        }

        ResolveContext captured = context;
        VariantChild DeriveAlternative(int alternative, Type type)
        {
            Derivation derived = DeriveNode(nodes[alternative], root, in captured, type, ConversionDirection.Write);
            return derived.Succeeded ? Child(type, derived.Converter) : null;
        }

        return Derivation.Of(new VariantWriter(typeName, codecs, canonical, DeriveAlternative));
    }

    // Dynamic from object. The writer infers the type of each value, and derives the tree of each type that it meets
    // from the canonical CLR type of the type.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveDynamicWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        Derivation refused = RefuseOtherThanCanonical(node, root, in context, clrType);
        if (refused is not null)
        {
            return refused;
        }

        ResolveContext captured = context;
        VariantChild DeriveType(string type)
        {
            Type canonical = registry.Resolve(type, in captured).ElementType;
            Derivation derived = Derive(type, in captured, canonical, ConversionDirection.Write);
            return derived.Succeeded ? Child(canonical, derived.Converter) : throw new NotSupportedException(derived.Refusal);
        }

        return Derivation.Of(new DynamicWriter(DeriveType));
    }

    // QBit(X, N) from the vectors of its element type.
    private Derivation DeriveQBitWrite(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        Derivation refused = RefuseOtherThanCanonical(node, root, in context, clrType);
        if (refused is not null)
        {
            return refused;
        }

        ColumnWriter writer = registry.ResolveNode(node, in context) switch
        {
            QBitSByteColumnCodec bytes => new QBitSByteWriter(bytes.TypeName, bytes.Dimension),
            QBitFloatColumnCodec floats => new QBitFloatWriter(floats.TypeName, floats.Dimension, floats.BitWidth),
            QBitDoubleColumnCodec doubles => new QBitDoubleWriter(doubles.TypeName, doubles.Dimension),
            _ => null,
        };

        return writer is not null ? Derivation.Of(writer) : Refuse(node, root, $"'{node}' cannot be written from {clrType}.");
    }

    // A geo type from its canonical CLR type only, through the writers of its structure: a Tuple, an Array or a Variant.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveGeoWrite(string name, TypeNode node, TypeNode structure, TypeNode root, in ResolveContext context, Type clrType)
    {
        Derivation refused = RefuseOtherThanCanonical(node, root, in context, clrType);
        if (refused is not null)
        {
            return refused;
        }

        return name == "Geometry"
            ? DeriveVariantWrite(structure, root, in context, clrType, name)
            : DeriveNode(structure, root, in context, clrType, ConversionDirection.Write);
    }

    // The refusal of a CLR type other than the canonical CLR type of the node, or null for the canonical type.
    private Derivation RefuseOtherThanCanonical(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        IColumnCodec codec = registry.ResolveNode(node, in context);
        return clrType == codec.ElementType
            ? null
            : Refuse(node, root, $"'{node}' cannot be written from {clrType}. It is written from: {codec.ElementType}.");
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static VariantChild Child(Type clrType, object writer)
        => (VariantChild)Activator.CreateInstance(typeof(VariantChild<>).MakeGenericType(clrType), writer);

    // Finds the closed base type of a writer whose definition is the given open generic type.
    private static bool TryGetBase(Type type, Type definition, out Type closed)
    {
        for (Type current = type; current is not null; current = current.BaseType)
        {
            if (current.IsGenericType && current.GetGenericTypeDefinition() == definition)
            {
                closed = current;
                return true;
            }
        }

        closed = null;
        return false;
    }
}
