using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>The kind of one part of a column that a query read as another type, as the insert writes it.</summary>
internal enum DecodedPart
{
    /// <summary>
    /// The values keep their meaning: the codec writes the part from its storage, else the converter tree of its CLR type.
    /// Under rows that an enclosing <c>Nullable</c> hides, a part of another type goes through the converter tree with
    /// those rows marked (<see cref="DecodedPartPlan.SameType"/>).
    /// </summary>
    Kept,

    /// <summary>
    /// A <c>DateTime64</c>, <c>Time64</c> or <c>Enum</c> whose values mean other values, also in <c>Nullable</c>,
    /// <c>LowCardinality</c> and <c>SimpleAggregateFunction</c>: converted through the CLR type of their meaning. Also a
    /// <c>String</c> or <c>FixedString</c> written into another type of a string (another width, <c>Nullable</c> or
    /// <c>LowCardinality</c>): converted through the bytes of the values, so no byte is read as text.
    /// </summary>
    Converted,

    /// <summary>A <c>Tuple</c>: each element is a part.</summary>
    Tuple,

    /// <summary>An <c>Array</c>: the stored offsets, then the elements as a part.</summary>
    Array,

    /// <summary>A <c>Map</c>: the stored offsets, then the keys and the values as parts.</summary>
    Map,

    /// <summary>A <c>Nullable</c> of a composite on either side: the null map, then the composite as a part.</summary>
    Nullable,

    /// <summary>A type that no part of the write converts (a <c>Variant</c>, a <c>Nested</c>); the reason says why.</summary>
    Refused,
}

/// <summary>How the insert writes one part of a column that a query read as another type.</summary>
internal readonly struct DecodedPartPlan
{
    /// <summary>Initializes a plan.</summary>
    /// <param name="kind">The kind of the part.</param>
    /// <param name="sourceParts">The source types of the parts in it.</param>
    /// <param name="targetParts">The target types of the parts in it.</param>
    /// <param name="sourceHoldsNull">For <see cref="DecodedPart.Nullable"/>: whether the source is <c>Nullable</c>.</param>
    /// <param name="targetHoldsNull">For <see cref="DecodedPart.Nullable"/>: whether the target is <c>Nullable</c>.</param>
    /// <param name="refusal">For <see cref="DecodedPart.Refused"/>: why no part converts the values.</param>
    /// <param name="sameType">Whether the source and the target of the part are the same type.</param>
    public DecodedPartPlan(DecodedPart kind, string[] sourceParts = null, string[] targetParts = null, bool sourceHoldsNull = false, bool targetHoldsNull = false, string refusal = null, bool sameType = false)
    {
        Kind = kind;
        SameType = sameType;
        SourceParts = sourceParts ?? System.Array.Empty<string>();
        TargetParts = targetParts ?? System.Array.Empty<string>();
        SourceHoldsNull = sourceHoldsNull;
        TargetHoldsNull = targetHoldsNull;
        Refusal = refusal;
    }

    /// <summary>The kind of the part.</summary>
    public DecodedPart Kind { get; }

    /// <summary>
    /// The source types of the parts in it: the elements of a <c>Tuple</c>, the element of an <c>Array</c>, the key and the
    /// value of a <c>Map</c>, the type in a <c>Nullable</c> (or the type itself when the source is not <c>Nullable</c>).
    /// </summary>
    public string[] SourceParts { get; }

    /// <summary>The target types of the parts in it, in the order of <see cref="SourceParts"/>.</summary>
    public string[] TargetParts { get; }

    /// <summary>For <see cref="DecodedPart.Nullable"/>: whether the source is <c>Nullable</c>.</summary>
    public bool SourceHoldsNull { get; }

    /// <summary>For <see cref="DecodedPart.Nullable"/>: whether the target is <c>Nullable</c>.</summary>
    public bool TargetHoldsNull { get; }

    /// <summary>For <see cref="DecodedPart.Refused"/>: why no part converts the values.</summary>
    public string Refusal { get; }

    /// <summary>
    /// Whether the source and the target of the part are the same type, so that a write from storage keeps every byte,
    /// also the bytes of a value that an enclosing <c>Nullable</c> hides.
    /// </summary>
    public bool SameType { get; }
}

/// <summary>How the insert writes a column that a query read as another type.</summary>
internal enum ReadColumnRoute
{
    /// <summary>As any other column: the codec when it writes the column from its storage, else the converter tree.</summary>
    Column,

    /// <summary>Part by part, because a write through its CLR type would change its values.</summary>
    Parts,

    /// <summary>
    /// Part by part when every part can be written so, because a value that a <c>Nullable</c> hides must not be converted
    /// into the target type; else as any other column.
    /// </summary>
    PartsUnderNull,
}

/// <summary>
/// The write of a column that a query read as one type into a column of another type, where a write through the CLR type
/// of the column would change its values. The canonical values of the two types can mean other values: the count of a
/// <c>DateTime64</c> or a <c>Time64</c> is a count at the scale of its type, and the ordinal of an <c>Enum</c> means the
/// label that its type declares for it. Or a <c>String</c> goes into another type of a string, and its CLR type
/// <see cref="string"/> reads bytes that are not UTF-8 as U+FFFD. The insert converts only those parts: through the
/// meaning of their values (a time as <see cref="DateTimeOffset"/>, a duration as <see cref="TimeSpan"/>, a label as
/// <see cref="string"/>), or through their bytes (<c>byte[]</c>). It writes every other part from its storage,
/// so their bytes do not change.
/// </summary>
internal sealed partial class ConverterDerivation
{
    /// <summary>
    /// Whether <paramref name="column"/> is the column that a query of <paramref name="source"/> reads, and a write through
    /// its CLR type would change its values in <paramref name="target"/>: a part whose canonical values mean other values
    /// (<see cref="ChangesMeaning(string, string)"/>), or a <c>String</c> or <c>FixedString</c> part written into another
    /// type of a string.
    /// </summary>
    /// <param name="column">The column to write.</param>
    /// <param name="source">The type that the column says it holds.</param>
    /// <param name="target">The type of the target column.</param>
    /// <param name="context">The resolution context of the target.</param>
    /// <returns>
    /// How the insert writes the column: part by part (<see cref="PlanDecodedPart"/>) when a part changes; also when the
    /// source holds values under a <c>Nullable</c> and the target is another type, so that no hidden value is converted.
    /// </returns>
    internal ReadColumnRoute RouteOfReadColumn(IColumn column, string source, string target, in ResolveContext context)
    {
        if (!TryParse(source, out TypeNode sourceNode) || !TryParse(target, out TypeNode targetNode))
        {
            return ReadColumnRoute.Column;
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
            return ReadColumnRoute.Column;
        }

        if (!sourceCodec.CanWrite(column))
        {
            return ReadColumnRoute.Column;
        }

        if (ChangesMeaning(sourceNode, targetNode) || ChangesStringShape(sourceNode, targetNode))
        {
            return ReadColumnRoute.Parts;
        }

        return HidesValues(sourceNode) && !SameType(sourceNode, targetNode) ? ReadColumnRoute.PartsUnderNull : ReadColumnRoute.Column;
    }

    /// <summary>How the insert writes one part of a column that a query read as <paramref name="source"/>.</summary>
    /// <param name="source">The source type of the part.</param>
    /// <param name="target">The target type of the part.</param>
    /// <param name="hidden">
    /// Whether an enclosing <c>Nullable</c> hides rows of the part. Then a composite of another type is taken apart too,
    /// so that each of its parts marks those rows, and a <c>Nullable</c> of another type is taken apart to mark its own.
    /// </param>
    /// <returns>The plan of the part.</returns>
    internal DecodedPartPlan PlanDecodedPart(string source, string target, bool hidden = false)
    {
        TypeNode sourceNode = WithoutAggregateFunction(TypeParser.Parse(source));
        TypeNode targetNode = WithoutAggregateFunction(TypeParser.Parse(target));
        bool changes = ChangesMeaning(sourceNode, targetNode) || ChangesStringShape(sourceNode, targetNode);
        bool sameType = SameType(sourceNode, targetNode);
        if (sameType || (!changes && !hidden && !HidesValues(sourceNode)))
        {
            return new DecodedPartPlan(DecodedPart.Kept, sameType: sameType);
        }

        string leaf = CanonicalName(Unwrapped(sourceNode));
        if (changes && (MeaningFamily(leaf) is not null || (IsStringLeaf(leaf) && IsStringLeaf(CanonicalName(Unwrapped(targetNode))))))
        {
            return new DecodedPartPlan(DecodedPart.Converted);
        }

        // A Nullable on either side, when neither side is a LowCardinality: the NULL of a LowCardinality is a key of its
        // dictionary, so it hides no value, and its converter tree writes its NULL.
        bool sourceNullable = CanonicalName(sourceNode) == "Nullable" && sourceNode.Arguments.Count == 1;
        bool targetNullable = CanonicalName(targetNode) == "Nullable" && targetNode.Arguments.Count == 1;
        bool dictionary = CanonicalName(sourceNode) == "LowCardinality" || CanonicalName(targetNode) == "LowCardinality";
        if ((sourceNullable || targetNullable) && !dictionary)
        {
            return new DecodedPartPlan(
                DecodedPart.Nullable,
                new[] { (sourceNullable ? sourceNode.Arguments[0] : sourceNode).ToString() },
                new[] { (targetNullable ? targetNode.Arguments[0] : targetNode).ToString() },
                sourceNullable,
                targetNullable);
        }

        // A change in the composite means that both types have its name; a composite of another type that changes nothing
        // and has another shape is written as any other column.
        string name = CanonicalName(sourceNode);
        int arguments = sourceNode.Arguments.Count;
        bool sameShape = name == CanonicalName(targetNode) && arguments == targetNode.Arguments.Count;
        switch (name)
        {
            case "Array" when sameShape && arguments == 1:
                return new DecodedPartPlan(DecodedPart.Array, new[] { sourceNode.Arguments[0].ToString() }, new[] { targetNode.Arguments[0].ToString() });

            case "Map" when sameShape && arguments == 2:
                return new DecodedPartPlan(
                    DecodedPart.Map,
                    new[] { sourceNode.Arguments[0].ToString(), sourceNode.Arguments[1].ToString() },
                    new[] { targetNode.Arguments[0].ToString(), targetNode.Arguments[1].ToString() });

            case "Tuple" when sameShape:
                return new DecodedPartPlan(
                    DecodedPart.Tuple,
                    System.Array.ConvertAll(NamedElementParser.Split(sourceNode), element => element.Type.ToString()),
                    System.Array.ConvertAll(NamedElementParser.Split(targetNode), element => element.Type.ToString()));

            default:
                return changes
                    ? new DecodedPartPlan(
                        DecodedPart.Refused,
                        refusal: $"The values of {sourceNode} are converted to another type only through Nullable, LowCardinality, Array, Map and Tuple, so a column read as {sourceNode} is written only into a column whose DateTime64, Time64 and Enum types are the same.")
                    : new DecodedPartPlan(DecodedPart.Kept);
        }
    }

    // Whether a column of the type can hold a value that a Nullable hides: a Nullable that is not the dictionary of a
    // LowCardinality (whose NULL is a key, with no value), also inside Tuple, Array and Map.
    private bool HidesValues(TypeNode node)
    {
        node = WithoutAggregateFunction(node);
        switch (CanonicalName(node))
        {
            case "Nullable":
                return true;

            case "Array" or "Map":
                foreach (TypeNode argument in node.Arguments)
                {
                    if (HidesValues(argument))
                    {
                        return true;
                    }
                }

                return false;

            case "Tuple":
                foreach ((string _, TypeNode element) in NamedElementParser.Split(node))
                {
                    if (HidesValues(element))
                    {
                        return true;
                    }
                }

                return false;

            default:
                return false;
        }
    }

    /// <summary>
    /// The conversion of a part of kind <see cref="DecodedPart.Converted"/>: a reader of the source type and a writer of the
    /// target type, over the CLR type that holds the meaning of the values, or their bytes for a string.
    /// </summary>
    /// <param name="source">The source type of the part.</param>
    /// <param name="target">The target type of the part.</param>
    /// <param name="context">The resolution context of the target.</param>
    /// <param name="reader">The reader of the source type, when the conversion is possible.</param>
    /// <param name="writer">The writer of the target type, over the CLR type of the reader.</param>
    /// <param name="refusal">Why the part cannot be converted, when it cannot.</param>
    /// <returns>Whether the conversion is possible.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    internal bool TryDeriveMeaningConversion(string source, string target, in ResolveContext context, out ColumnReader reader, out ColumnWriter writer, out string refusal)
    {
        reader = null;
        writer = null;
        Type meaning = MeaningType(TypeParser.Parse(source), out refusal);
        if (meaning is null)
        {
            return false;
        }

        Derivation read = Derive(source, in context, meaning, ConversionDirection.Read);
        Derivation write = read.Succeeded ? Derive(target, in context, meaning, ConversionDirection.Write) : read;
        if (!write.Succeeded)
        {
            refusal = write.Refusal;
            return false;
        }

        reader = (ColumnReader)read.Converter;
        writer = (ColumnWriter)write.Converter;
        return true;
    }

    /// <summary>The codec of the target type of a part.</summary>
    /// <param name="type">The type of the part.</param>
    /// <param name="context">The resolution context of the target.</param>
    /// <returns>The codec.</returns>
    internal IColumnCodec ResolvePart(string type, in ResolveContext context) => registry.Resolve(type, in context);

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

    // Whether a String or a FixedString of the source goes, at the same place, into another type of a string: another
    // width, or other Nullable and LowCardinality wrappers. The CLR type of a String column is string, whose text does not
    // keep bytes that are not UTF-8, so the write reads the bytes of such a part. A Variant and a Nested are written only
    // from their own layout, so they have no such place.
    private bool ChangesStringShape(TypeNode source, TypeNode target)
    {
        TypeNode sourceLeaf = Unwrapped(source);
        TypeNode targetLeaf = Unwrapped(target);
        string sourceName = CanonicalName(sourceLeaf);
        string targetName = CanonicalName(targetLeaf);
        if (IsStringLeaf(sourceName) && IsStringLeaf(targetName))
        {
            return !SameType(source, target);
        }

        if (sourceName != targetName)
        {
            return false;
        }

        switch (sourceName)
        {
            case "Array" when sourceLeaf.Arguments.Count == 1 && targetLeaf.Arguments.Count == 1:
                return ChangesStringShape(sourceLeaf.Arguments[0], targetLeaf.Arguments[0]);

            case "Map" when sourceLeaf.Arguments.Count == 2 && targetLeaf.Arguments.Count == 2:
                return ChangesStringShape(sourceLeaf.Arguments[0], targetLeaf.Arguments[0]) || ChangesStringShape(sourceLeaf.Arguments[1], targetLeaf.Arguments[1]);

            case "Tuple":
            {
                (string Name, TypeNode Type)[] sourceElements = NamedElementParser.Split(sourceLeaf);
                (string Name, TypeNode Type)[] targetElements = NamedElementParser.Split(targetLeaf);
                if (sourceElements.Length != targetElements.Length)
                {
                    return false;
                }

                for (int i = 0; i < sourceElements.Length; i++)
                {
                    if (ChangesStringShape(sourceElements[i].Type, targetElements[i].Type))
                    {
                        return true;
                    }
                }

                return false;
            }

            default:
                return false;
        }
    }

    private static bool IsStringLeaf(string name) => name is "String" or "FixedString";

    // Whether two types are the same type: the same names (under any alias), arguments and wrappers. A
    // SimpleAggregateFunction is the type of its values.
    private bool SameType(TypeNode left, TypeNode right)
    {
        left = WithoutAggregateFunction(left);
        right = WithoutAggregateFunction(right);
        if (CanonicalName(left) != CanonicalName(right) || left.Arguments.Count != right.Arguments.Count)
        {
            return false;
        }

        for (int i = 0; i < left.Arguments.Count; i++)
        {
            if (!SameType(left.Arguments[i], right.Arguments[i]))
            {
                return false;
            }
        }

        return true;
    }

    // The CLR type that holds the meaning of the values of a part of kind Converted: a time, a duration, a label or the
    // bytes of a string, through Nullable, LowCardinality and SimpleAggregateFunction. Null, with the reason, when no CLR type holds the values
    // without a loss.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private Type MeaningType(TypeNode node, out string reason)
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

            // The bytes of a string, which the text of its CLR type string would not keep.
            case "String" or "FixedString":
                return typeof(byte[]);

            case "Nullable" when node.Arguments.Count == 1:
            {
                Type inner = MeaningType(node.Arguments[0], out reason);
                return inner is null ? null : inner.IsValueType ? typeof(Nullable<>).MakeGenericType(inner) : inner;
            }

            case "LowCardinality" when node.Arguments.Count == 1:
                return MeaningType(node.Arguments[0], out reason);

            case "SimpleAggregateFunction" when node.Arguments.Count == 2:
                return MeaningType(node.Arguments[1], out reason);

            default:
                throw new InvalidOperationException($"{node} is not a DateTime64, a Time64, an Enum, a String or a FixedString in Nullable, LowCardinality or SimpleAggregateFunction.");
        }
    }

    // A SimpleAggregateFunction(f, T) column is a T column.
    private TypeNode WithoutAggregateFunction(TypeNode node)
    {
        while (CanonicalName(node) == "SimpleAggregateFunction" && node.Arguments.Count == 2)
        {
            node = node.Arguments[1];
        }

        return node;
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
