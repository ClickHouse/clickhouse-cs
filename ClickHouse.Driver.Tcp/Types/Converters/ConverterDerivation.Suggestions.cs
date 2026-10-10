using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>The CLR types that an error message suggests for a column type.</summary>
internal sealed partial class ConverterDerivation
{
    /// <summary>
    /// A short list of CLR types that a column type reads as, or is written from, for an error message. A leaf gives the
    /// CLR types of its pairs in the leaf table; <c>Nullable(X)</c> gives the types of X, a value type made nullable;
    /// <c>LowCardinality(X)</c> and <c>SimpleAggregateFunction(f, X)</c> give the types of X; every other type gives its
    /// canonical CLR type. The list keeps only the types that the derivation accepts for the whole column type
    /// (<c>LowCardinality(JSON)</c> is written from no CLR type). A derivation cannot list what it accepts, so the list
    /// is not complete: the read and write rules of D6 and the other CLR types under a composite are not in it.
    /// </summary>
    /// <param name="type">The ClickHouse type string.</param>
    /// <param name="context">The resolution context.</param>
    /// <param name="direction">Read or write.</param>
    /// <returns>The CLR types, in the order of the leaf table. Empty when the type is written from no CLR type.</returns>
    /// <exception cref="FormatException"><paramref name="type"/> is not a well-formed ClickHouse type.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed, but this client does not support it.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public IReadOnlyList<Type> SuggestedTypes(string type, in ResolveContext context, ConversionDirection direction)
    {
        ArgumentNullException.ThrowIfNull(type);
        registry.Resolve(type, in context);
        var candidates = new List<Type>();
        AddSuggestedTypes(TypeParser.Parse(type), in context, direction, lift: false, candidates);
        var types = new List<Type>(candidates.Count);
        foreach (Type candidate in candidates)
        {
            if (Derive(type, in context, candidate, direction).Succeeded)
            {
                types.Add(candidate);
            }
        }

        return types;
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private void AddSuggestedTypes(TypeNode node, in ResolveContext context, ConversionDirection direction, bool lift, List<Type> types)
    {
        string name = registry.TryCanonicalName(node.Name, out string canonical) ? canonical : node.Name;
        switch (name)
        {
            case "SimpleAggregateFunction":
                AddSuggestedTypes(node.Arguments[1], in context, direction, lift, types);
                return;

            case "Nullable":
                AddSuggestedTypes(node.Arguments[0], in context, direction, lift: true, types);
                return;

            case "LowCardinality":
                AddSuggestedTypes(node.Arguments[0], in context, direction, lift, types);
                return;
        }

        IColumnCodec codec = registry.ResolveNode(node, in context);
        if (!LeafTable.TryGet(name, out Leaf leaf))
        {
            // The type of the decoded column. Nested is written from no CLR type, so the derivation removes it there.
            types.Add(lift ? Lifted(codec.ElementType) : codec.ElementType);
            return;
        }

        foreach (Type clrType in direction == ConversionDirection.Read ? leaf.ReadTypes(codec) : leaf.WriteTypes(codec))
        {
            types.Add(lift ? Lifted(clrType) : clrType);
        }
    }

    // The type that holds a NULL: a value type made nullable, or a reference type as it is.
    private static Type Lifted(Type type) => type.IsValueType ? typeof(Nullable<>).MakeGenericType(type) : type;
}
