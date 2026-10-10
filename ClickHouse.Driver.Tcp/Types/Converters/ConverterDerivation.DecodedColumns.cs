using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The write of a column that a query read as one type into a column of another type, where the canonical values of the
/// two types mean other values: the count of a <c>DateTime64</c> or a <c>Time64</c> is a count at the scale of its type,
/// and the ordinal of an <c>Enum</c> means the label that its type declares for it. Such a column is converted through
/// the meaning of its values: a time as <see cref="DateTimeOffset"/>, a duration as <see cref="TimeSpan"/>, a label as
/// <see cref="string"/>.
/// </summary>
internal sealed partial class ConverterDerivation
{
    /// <summary>
    /// The conversion of a column that a query read as <paramref name="source"/> into a column of
    /// <paramref name="target"/>: a reader of the source type and a writer of the target type, over the CLR type that holds
    /// the meaning of the values.
    /// </summary>
    /// <param name="column">The column to write.</param>
    /// <param name="source">The type that the column says it holds.</param>
    /// <param name="target">The type of the target column.</param>
    /// <param name="context">The resolution context of the target.</param>
    /// <param name="reader">The reader of the source type, when the conversion is possible.</param>
    /// <param name="writer">The writer of the target type, over the CLR type of the reader.</param>
    /// <param name="refusal">Why the column cannot be converted, when the conversion applies and is not possible.</param>
    /// <returns>
    /// Whether the conversion applies: the column is the column that a query of <paramref name="source"/> reads, and its
    /// canonical values mean other values in <paramref name="target"/>. When it applies, either the reader and the writer
    /// or the refusal is set.
    /// </returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    internal bool TryDeriveDecodedConversion(
        IColumn column,
        string source,
        string target,
        in ResolveContext context,
        out ColumnReader reader,
        out ColumnWriter writer,
        out string refusal)
    {
        reader = null;
        writer = null;
        refusal = null;
        if (!TryParse(source, out TypeNode sourceNode) || !TryParse(target, out TypeNode targetNode))
        {
            return false;
        }

        // A column that a caller built can carry any type name; one that names no type that the registry resolves is not a
        // column that a query read.
        IColumnCodec sourceCodec;
        try
        {
            sourceCodec = registry.ResolveNode(sourceNode, in context);
        }
        catch (Exception ex) when (ex is FormatException or NotSupportedException or ArgumentException or OverflowException)
        {
            return false;
        }

        if (!sourceCodec.CanWrite(column) || !ChangesMeaning(sourceNode, targetNode))
        {
            return false;
        }

        Type meaning = MeaningType(sourceNode, in context, out string reason);
        if (meaning is null)
        {
            refusal = reason;
            return true;
        }

        Derivation read = Derive(source, in context, meaning, ConversionDirection.Read);
        Derivation write = read.Succeeded ? Derive(target, in context, meaning, ConversionDirection.Write) : read;
        if (!write.Succeeded)
        {
            refusal = write.Refusal;
            return true;
        }

        reader = (ColumnReader)read.Converter;
        writer = (ColumnWriter)write.Converter;
        return true;
    }

    /// <summary>
    /// Whether the canonical values of <paramref name="source"/> mean other values in <paramref name="target"/>: at the same
    /// place of the two types, a <c>DateTime64</c> or a <c>Time64</c> of another scale, or an <c>Enum</c> of another width
    /// or with other members. <c>Nullable</c>, <c>LowCardinality</c> and <c>SimpleAggregateFunction</c> on either side do
    /// not change the place, as the write rules lift a value through them.
    /// </summary>
    /// <param name="source">The type that the values have.</param>
    /// <param name="target">The type that they are written as.</param>
    /// <returns>Whether a value of the source type means another value in the target type.</returns>
    internal bool ChangesMeaning(string source, string target)
        => TryParse(source, out TypeNode sourceNode) && TryParse(target, out TypeNode targetNode) && ChangesMeaning(sourceNode, targetNode);

    private static bool TryParse(string type, out TypeNode node)
    {
        node = null;
        if (string.IsNullOrEmpty(type))
        {
            return false;
        }

        try
        {
            node = TypeParser.Parse(type);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private bool ChangesMeaning(TypeNode source, TypeNode target)
    {
        source = Unwrapped(source);
        target = Unwrapped(target);
        string sourceName = CanonicalName(source);
        string targetName = CanonicalName(target);
        string family = MeaningFamily(sourceName);
        if (family is not null)
        {
            return family == MeaningFamily(targetName) && !SameMeaningParameters(family, source, target);
        }

        if (sourceName != targetName)
        {
            return false;
        }

        switch (sourceName)
        {
            case "Array" when source.Arguments.Count == 1 && target.Arguments.Count == 1:
                return ChangesMeaning(source.Arguments[0], target.Arguments[0]);

            case "Map" when source.Arguments.Count == 2 && target.Arguments.Count == 2:
                return ChangesMeaning(source.Arguments[0], target.Arguments[0]) || ChangesMeaning(source.Arguments[1], target.Arguments[1]);

            case "Tuple" or "Nested":
            {
                (string Name, TypeNode Type)[] sourceElements = NamedElementParser.Split(source);
                (string Name, TypeNode Type)[] targetElements = NamedElementParser.Split(target);
                if (sourceElements.Length != targetElements.Length)
                {
                    return false;
                }

                for (int i = 0; i < sourceElements.Length; i++)
                {
                    if (ChangesMeaning(sourceElements[i].Type, targetElements[i].Type))
                    {
                        return true;
                    }
                }

                return false;
            }

            case "Variant":
                return VariantChangesMeaning(source, target);

            default:
                return false;
        }
    }

    // A Variant places a value by its CLR type, so an alternative of the source pairs with each alternative of the target
    // of the same name or family, whatever their order: the meaning changes when every such alternative gives the value
    // another meaning. An alternative with no such alternative in the target pairs with none.
    private bool VariantChangesMeaning(TypeNode source, TypeNode target)
    {
        foreach (TypeNode alternative in source.Arguments)
        {
            TypeNode leaf = Unwrapped(alternative);
            string name = CanonicalName(leaf);
            string family = MeaningFamily(name);
            bool paired = false;
            bool kept = false;
            foreach (TypeNode candidate in target.Arguments)
            {
                TypeNode candidateLeaf = Unwrapped(candidate);
                string candidateName = CanonicalName(candidateLeaf);
                if (candidateName == name || (family is not null && MeaningFamily(candidateName) == family))
                {
                    paired = true;
                    kept |= !ChangesMeaning(leaf, candidateLeaf);
                }
            }

            if (paired && !kept)
            {
                return true;
            }
        }

        return false;
    }

    // The CLR type that holds the meaning of the values of a type: a time, a duration, a label, and the canonical type of
    // every other leaf, through Nullable, LowCardinality, SimpleAggregateFunction, Array, Map and Tuple. Null, with the
    // reason, when no CLR type holds the values without a loss.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Type MeaningType(TypeNode node, in ResolveContext context, out string reason)
    {
        reason = null;
        string name = CanonicalName(node);
        switch (name)
        {
            case "DateTime64":
            case "Time64":
            {
                int scale = Scale(node);
                if (scale > 7)
                {
                    reason = string.Create(
                        CultureInfo.InvariantCulture,
                        $"A value of {node} is a count at scale {scale}, finer than the 100 ns ticks of the {(name == "DateTime64" ? "System.DateTimeOffset" : "System.TimeSpan")} that converts it, so a column read as {node} is written only into a column of the same scale.");
                    return null;
                }

                return name == "DateTime64" ? typeof(DateTimeOffset) : typeof(TimeSpan);
            }

            case "Enum8" or "Enum16" or "Enum":
                return typeof(string);

            case "Nullable" when node.Arguments.Count == 1:
            {
                Type inner = MeaningType(node.Arguments[0], in context, out reason);
                return inner is null ? null : inner.IsValueType ? typeof(Nullable<>).MakeGenericType(inner) : inner;
            }

            case "LowCardinality" when node.Arguments.Count == 1:
                return MeaningType(node.Arguments[0], in context, out reason);

            case "SimpleAggregateFunction" when node.Arguments.Count == 2:
                return MeaningType(node.Arguments[1], in context, out reason);

            case "Array" when node.Arguments.Count == 1:
                return MeaningType(node.Arguments[0], in context, out reason)?.MakeArrayType();

            case "Map" when node.Arguments.Count == 2:
            {
                Type key = MeaningType(node.Arguments[0], in context, out reason);
                Type value = key is null ? null : MeaningType(node.Arguments[1], in context, out reason);
                return value is null ? null : typeof(KeyValuePair<,>).MakeGenericType(key, value).MakeArrayType();
            }

            case "Tuple":
            {
                (string Name, TypeNode Type)[] elements = NamedElementParser.Split(node);
                if (elements.Length == 0)
                {
                    return typeof(ValueTuple);
                }

                if (elements.Length >= ValueTupleDefinitions.Length)
                {
                    reason = $"{node} has more elements than a ValueTuple holds.";
                    return null;
                }

                var types = new Type[elements.Length];
                for (int i = 0; i < elements.Length; i++)
                {
                    types[i] = MeaningType(elements[i].Type, in context, out reason);
                    if (types[i] is null)
                    {
                        return null;
                    }
                }

                return ValueTupleDefinitions[elements.Length].MakeGenericType(types);
            }

            default:
                if (HasMeaningLeaf(node))
                {
                    reason = $"The values of {node} are converted to another type only through Nullable, LowCardinality, Array, Map and Tuple, so a column read as {node} is written only into a column whose DateTime64, Time64 and Enum types are the same.";
                    return null;
                }

                return registry.ResolveNode(node, in context).ElementType;
        }
    }

    // Whether a DateTime64, a Time64 or an Enum is in the type.
    private bool HasMeaningLeaf(TypeNode node)
    {
        string name = CanonicalName(node);
        if (MeaningFamily(name) is not null)
        {
            return true;
        }

        if (name is "Tuple" or "Nested")
        {
            foreach ((string _, TypeNode element) in NamedElementParser.Split(node))
            {
                if (HasMeaningLeaf(element))
                {
                    return true;
                }
            }

            return false;
        }

        foreach (TypeNode argument in node.Arguments)
        {
            // An argument that is not a type (a scale, a label, a function name) has no registered name.
            if (registry.TryCanonicalName(argument.Name, out _) && HasMeaningLeaf(argument))
            {
                return true;
            }
        }

        return false;
    }

    private TypeNode Unwrapped(TypeNode node)
    {
        while (true)
        {
            string name = CanonicalName(node);
            if (name is "Nullable" or "LowCardinality" && node.Arguments.Count == 1)
            {
                node = node.Arguments[0];
            }
            else if (name == "SimpleAggregateFunction" && node.Arguments.Count == 2)
            {
                node = node.Arguments[1];
            }
            else
            {
                return node;
            }
        }
    }

    private string CanonicalName(TypeNode node) => registry.TryCanonicalName(node.Name, out string canonical) ? canonical : node.Name;

    // The leaves whose canonical value depends on a parameter of the type.
    private static string MeaningFamily(string name) => name switch
    {
        "DateTime64" => "DateTime64",
        "Time64" => "Time64",
        "Enum8" or "Enum16" or "Enum" => "Enum",
        _ => null,
    };

    // The scale of a DateTime64 or a Time64; the timezone of a DateTime64 does not change a count, which is an instant.
    // An Enum: the same width and the same members.
    private bool SameMeaningParameters(string family, TypeNode source, TypeNode target)
    {
        if (family != "Enum")
        {
            return Scale(source) == Scale(target);
        }

        IColumnCodec sourceCodec = registry.ResolveNode(source, ResolveContext.ForWrite);
        IColumnCodec targetCodec = registry.ResolveNode(target, ResolveContext.ForWrite);
        return (sourceCodec, targetCodec) switch
        {
            (EnumColumnCodec<sbyte> a, EnumColumnCodec<sbyte> b) => a.Members.HasTheSameMembers(b.Members),
            (EnumColumnCodec<short> a, EnumColumnCodec<short> b) => a.Members.HasTheSameMembers(b.Members),
            _ => false,
        };
    }

    // The scale argument; a malformed one has failed the resolution of the type before.
    private static int Scale(TypeNode node)
        => node.Arguments.Count > 0 && int.TryParse(node.Arguments[0].Name, NumberStyles.Integer, CultureInfo.InvariantCulture, out int scale) ? scale : -1;
}
