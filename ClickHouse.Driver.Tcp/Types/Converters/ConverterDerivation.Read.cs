using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>The read combinators of the derivation and the read rules of D6.</summary>
internal sealed partial class ConverterDerivation
{
    private static readonly Type[] TupleReaderDefinitions =
    {
        null,
        typeof(TupleReader<>),
        typeof(TupleReader<,>),
        typeof(TupleReader<,,>),
        typeof(TupleReader<,,,>),
        typeof(TupleReader<,,,,>),
        typeof(TupleReader<,,,,,>),
        typeof(TupleReader<,,,,,,>),
    };

    private static readonly Type[] ValueTupleDefinitions =
    {
        null,
        typeof(ValueTuple<>),
        typeof(ValueTuple<,>),
        typeof(ValueTuple<,,>),
        typeof(ValueTuple<,,,>),
        typeof(ValueTuple<,,,,>),
        typeof(ValueTuple<,,,,,>),
        typeof(ValueTuple<,,,,,,>),
    };

    /// <summary>
    /// Whether a CLR type can be the type of a read value: not a pointer, a by-reference type, a ref struct, an open
    /// generic type or <see cref="void"/>, which no generic converter can take as a type argument.
    /// </summary>
    /// <param name="clrType">The CLR type.</param>
    /// <returns>Whether a reader can give values of the type.</returns>
    private static bool IsValueTypeArgument(Type clrType)
        => !(clrType.IsPointer || clrType.IsByRef || clrType.IsByRefLike || clrType.ContainsGenericParameters || clrType == typeof(void));

    // A composite node, or a node that reads only as its canonical type.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveCompositeRead(string name, TypeNode node, TypeNode root, in ResolveContext context, Type clrType, DictionaryOrder order)
    {
        switch (name)
        {
            case "Nullable":
                return DeriveNullable(node, root, in context, clrType, order);

            case "LowCardinality":
                return DeriveLowCardinality(node, root, in context, clrType, order);

            case "Array":
                return DeriveArray(node, root, in context, clrType, order);

            case "Map":
                return DeriveMap(node, root, in context, clrType, order);

            case "Tuple" when node.Arguments.Count > 0:
                return DeriveTuple(node, root, in context, clrType, order);

            default:
                return DeriveCanonicalOnly(node, root, in context, clrType);
        }
    }

    // Nullable(X): T? lifts a value type, a reference type holds the NULL itself, and a bare value type cannot hold it.
    // A value-type lift accepts only a child that does not need the column.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveNullable(TypeNode node, TypeNode root, in ResolveContext context, Type clrType, DictionaryOrder order)
    {
        TypeNode innerNode = node.Arguments[0];
        Type value = Nullable.GetUnderlyingType(clrType);
        if (value is not null)
        {
            Derivation inner = DeriveNode(innerNode, root, in context, value, ConversionDirection.Read, order);
            if (!inner.Succeeded)
            {
                return inner;
            }

            return inner.NeedsColumn
                ? Refuse(node, root, $"'{node}' cannot be read as {clrType}: '{innerNode}' reads as {value} only from its column, and a nullable value type converts each value alone.")
                : Wrap(typeof(NullableValueReader<>), value, needsColumn: false, inner.Converter);
        }

        if (clrType.IsValueType)
        {
            return Refuse(node, root, $"'{node}' cannot be read as {clrType}, which cannot hold NULL.");
        }

        Derivation reference = DeriveNode(innerNode, root, in context, clrType, ConversionDirection.Read, order);
        return reference.Succeeded ? Wrap(typeof(NullableReferenceReader<>), clrType, reference.NeedsColumn, reference.Converter) : reference;
    }

    // LowCardinality(X) reads as X reads. The dictionary of LowCardinality(Nullable(X)) is a column of the bare X, so
    // the derivation reads X and the dictionary reader gives the NULL slot.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveLowCardinality(TypeNode node, TypeNode root, in ResolveContext context, Type clrType, DictionaryOrder order)
    {
        TypeNode innerNode = node.Arguments[0];
        bool nullable = innerNode.Name == "Nullable";
        if (!nullable)
        {
            Derivation plain = DeriveNode(innerNode, root, in context, clrType, ConversionDirection.Read, order);
            return plain.Succeeded ? Wrap(typeof(DictionaryReader<>), clrType, plain.NeedsColumn, plain.Converter, order) : plain;
        }

        innerNode = innerNode.Arguments[0];
        Type value = Nullable.GetUnderlyingType(clrType);
        if (value is not null)
        {
            Derivation lifted = DeriveNode(innerNode, root, in context, value, ConversionDirection.Read, order);
            return lifted.Succeeded ? Wrap(typeof(LiftingDictionaryReader<>), value, lifted.NeedsColumn, lifted.Converter, order) : lifted;
        }

        if (clrType.IsValueType)
        {
            return Refuse(node, root, $"'{node}' cannot be read as {clrType}, which cannot hold NULL.");
        }

        Derivation reference = DeriveNode(innerNode, root, in context, clrType, ConversionDirection.Read, order);
        return reference.Succeeded ? Wrap(typeof(DictionaryReader<>), clrType, reference.NeedsColumn, reference.Converter, order) : reference;
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveArray(TypeNode node, TypeNode root, in ResolveContext context, Type clrType, DictionaryOrder order)
    {
        if (!clrType.IsSZArray)
        {
            return Refuse(node, root, $"'{node}' cannot be read as {clrType}. It reads as an array.");
        }

        Type element = clrType.GetElementType();
        Derivation inner = DeriveNode(node.Arguments[0], root, in context, element, ConversionDirection.Read, order);
        return inner.Succeeded ? Wrap(typeof(ArrayReader<>), element, inner.NeedsColumn, inner.Converter) : inner;
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveMap(TypeNode node, TypeNode root, in ResolveContext context, Type clrType, DictionaryOrder order)
    {
        Type pair = clrType.IsSZArray ? clrType.GetElementType() : null;
        if (pair is null || !pair.IsGenericType || pair.GetGenericTypeDefinition() != typeof(KeyValuePair<,>))
        {
            return Refuse(node, root, $"'{node}' cannot be read as {clrType}. It reads as an array of KeyValuePair<TKey, TValue>.");
        }

        Type[] arguments = pair.GetGenericArguments();
        Derivation key = DeriveNode(node.Arguments[0], root, in context, arguments[0], ConversionDirection.Read, order);
        if (!key.Succeeded)
        {
            return key;
        }

        Derivation value = DeriveNode(node.Arguments[1], root, in context, arguments[1], ConversionDirection.Read, order);
        if (!value.Succeeded)
        {
            return value;
        }

        return Derivation.Of(
            Activator.CreateInstance(typeof(MapReader<,>).MakeGenericType(arguments), key.Converter, value.Converter),
            key.NeedsColumn || value.NeedsColumn);
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveTuple(TypeNode node, TypeNode root, in ResolveContext context, Type clrType, DictionaryOrder order)
    {
        (string Name, TypeNode Type)[] elements = NamedElementParser.Split(node);
        int arity = elements.Length;
        if (!clrType.IsGenericType || clrType.GetGenericTypeDefinition() != ValueTupleDefinitions[arity])
        {
            return Refuse(node, root, $"'{node}' cannot be read as {clrType}. It reads as a ValueTuple of {arity} element(s).");
        }

        Type[] arguments = clrType.GetGenericArguments();
        var fields = new object[arity];
        bool needsColumn = false;
        for (int i = 0; i < arity; i++)
        {
            Derivation field = DeriveNode(elements[i].Type, root, in context, arguments[i], ConversionDirection.Read, order);
            if (!field.Succeeded)
            {
                return field;
            }

            fields[i] = field.Converter;
            needsColumn |= field.NeedsColumn;
        }

        return Derivation.Of(Activator.CreateInstance(TupleReaderDefinitions[arity].MakeGenericType(arguments), fields), needsColumn);
    }

    // Variant, Dynamic, Nested, QBit, the geo types and Tuple() read only as their canonical type, through the indexer of
    // the decoded column (IndexedReader).
    private Derivation DeriveCanonicalOnly(TypeNode node, TypeNode root, in ResolveContext context, Type clrType)
    {
        Type canonical = registry.ResolveNode(node, in context).ElementType;
        if (clrType != canonical)
        {
            return Refuse(node, root, $"'{node}' cannot be read as {clrType}. It reads as: {canonical}.");
        }

        return Derivation.Of(typeof(IndexedReader<>).MakeGenericType(canonical).GetField(nameof(IndexedReader<object>.Instance)).GetValue(null));
    }

    /// <summary>
    /// The read rules of D6 (<see cref="ReadRules"/>), for a root whose own readings refused <paramref name="clrType"/>.
    /// They follow the order of POCO mapping: a value-type nullable column read as a bare value type, then a nullable
    /// target, then a cast of the canonical value.
    /// </summary>
    /// <param name="root">The column type.</param>
    /// <param name="canonical">The canonical CLR type of the column (its codec's element type).</param>
    /// <param name="context">The resolution context.</param>
    /// <param name="clrType">The CLR type to read as.</param>
    /// <param name="refused">The refusal of the column type's own readings, which this gives when no rule applies.</param>
    /// <returns>The reader of a rule, or <paramref name="refused"/>.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Derivation DeriveByReadRules(TypeNode root, Type canonical, in ResolveContext context, Type clrType, Derivation refused)
    {
        if (!IsValueTypeArgument(clrType))
        {
            return refused;
        }

        Type sourceValue = Nullable.GetUnderlyingType(canonical);
        Type targetValue = Nullable.GetUnderlyingType(clrType);

        // A column of T? read as a bare value type: the column's reading as clrType?, or a rule over its value type.
        // Either throws at the first NULL.
        if (sourceValue is not null && targetValue is null && clrType.IsValueType)
        {
            Derivation lifted = DeriveNode(root, root, in context, typeof(Nullable<>).MakeGenericType(clrType), ConversionDirection.Read, DictionaryOrder.Row);
            if (lifted.Succeeded && !lifted.NeedsColumn)
            {
                return Wrap(typeof(NonNullReader<>), clrType, needsColumn: false, lifted.Converter);
            }

            if (ReadRules.IsEnumOrdinal(sourceValue, clrType))
            {
                object ordinals = CanonicalReader(root, canonical, in context);
                object enums = Activator.CreateInstance(typeof(NullableEnumReader<,>).MakeGenericType(sourceValue, clrType), ordinals);
                return Wrap(typeof(NonNullReader<>), clrType, needsColumn: false, enums);
            }

            return refused;
        }

        if (targetValue is not null)
        {
            // A column of a type that is not T? read as clrType: the reading of the value type, then lifted.
            if (sourceValue is null)
            {
                Derivation present = DeriveNode(root, root, in context, targetValue, ConversionDirection.Read, DictionaryOrder.Row);
                if (present.Succeeded && !present.NeedsColumn)
                {
                    return Wrap(typeof(AsNullableReader<>), targetValue, needsColumn: false, present.Converter);
                }

                if (ReadRules.CanConvert(canonical, targetValue))
                {
                    object converted = Cast(CanonicalReader(root, canonical, in context), canonical, targetValue);
                    return Wrap(typeof(AsNullableReader<>), targetValue, needsColumn: false, converted);
                }

                return refused;
            }

            // A column of T? read as U?: null stays null.
            if (ReadRules.IsEnumOrdinal(sourceValue, targetValue))
            {
                return Derivation.Of(Activator.CreateInstance(
                    typeof(NullableEnumReader<,>).MakeGenericType(sourceValue, targetValue),
                    CanonicalReader(root, canonical, in context)));
            }

            return refused;
        }

        return ReadRules.CanConvert(canonical, clrType)
            ? Derivation.Of(Cast(CanonicalReader(root, canonical, in context), canonical, clrType))
            : refused;
    }

    // The reader of the canonical type of the column, which every supported type has.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private object CanonicalReader(TypeNode root, Type canonical, in ResolveContext context)
    {
        Derivation derived = DeriveNode(root, root, in context, canonical, ConversionDirection.Read);
        return derived.Succeeded
            ? derived.Converter
            : throw new InvalidOperationException($"The converter derivation cannot read '{root}' as its canonical type {canonical}: {derived.Refusal}");
    }

    // A rule of ReadRules.CanConvert as a reader: the enum ordinal, or the cast.
    private static object Cast(object reader, Type from, Type to)
        => ReadRules.IsEnumOrdinal(from, to)
            ? Activator.CreateInstance(typeof(EnumReader<,>).MakeGenericType(from, to), reader)
            : Activator.CreateInstance(typeof(AssignReader<,>).MakeGenericType(from, to), reader);

    private static Derivation Wrap(Type combinator, Type argument, bool needsColumn, params object[] arguments)
        => Derivation.Of(Activator.CreateInstance(combinator.MakeGenericType(argument), arguments), needsColumn);
}
