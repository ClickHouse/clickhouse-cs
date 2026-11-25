using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>The kind of one part of a column that a query read as another type, as the insert writes it.</summary>
internal enum DecodedPart
{
    /// <summary>
    /// The same type (<see cref="DecodedPartPlan.SameType"/>), or a part that has no structure in common with its target:
    /// the codec writes the part from its storage, else the converter tree of its CLR type. Under rows that an enclosing
    /// <c>Nullable</c> hides, a part of another type goes through the converter tree with those rows marked.
    /// </summary>
    Kept,

    /// <summary>
    /// A <c>DateTime64</c>, <c>Time64</c> or <c>Enum</c> whose values mean other values, also in <c>Nullable</c>,
    /// <c>LowCardinality</c> and <c>SimpleAggregateFunction</c>: converted through the CLR type of their meaning. Also a
    /// <c>String</c> or <c>FixedString</c> written into another type of a string (another width, <c>Nullable</c> or
    /// <c>LowCardinality</c>): converted through the bytes of the values, so no byte is read as text.
    /// </summary>
    Converted,

    /// <summary>A <c>Tuple</c> on both sides, with the same number of elements: each element is a part.</summary>
    Tuple,

    /// <summary>An <c>Array</c> on both sides: the stored offsets, then the elements as a part.</summary>
    Array,

    /// <summary>A <c>Map</c> on both sides: the stored offsets, then the keys and the values as parts.</summary>
    Map,

    /// <summary>
    /// A <c>Nullable</c> on either side, when neither side is a <c>LowCardinality</c>: the null map (or a null map of zeros
    /// when the source is not <c>Nullable</c>), then the type in it as a part.
    /// </summary>
    Nullable,

    /// <summary>
    /// A <c>Variant</c> on both sides, where each alternative of the source is an alternative of the target: the
    /// discriminators through the map of the alternatives (<see cref="DecodedPartPlan.Alternatives"/>), then each
    /// alternative of the source as a part. An alternative that only the target has holds no row.
    /// </summary>
    Variant,

    /// <summary>
    /// A type that no part of the write converts (a <c>Nested</c>, a <c>Variant</c> with an alternative that the target
    /// does not have), or a <c>DateTime64</c> into a <c>Time64</c> or the reverse; the reason says why.
    /// </summary>
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
    /// <param name="keepsStorage">
    /// For <see cref="DecodedPart.Tuple"/>, <see cref="DecodedPart.Array"/> and <see cref="DecodedPart.Map"/>: whether no
    /// part in it changes meaning or string shape and no <c>Nullable</c> in the source hides a value.
    /// </param>
    /// <param name="alternatives">For <see cref="DecodedPart.Variant"/>: the target alternative of each source alternative.</param>
    public DecodedPartPlan(
        DecodedPart kind,
        string[] sourceParts = null,
        string[] targetParts = null,
        bool sourceHoldsNull = false,
        bool targetHoldsNull = false,
        string refusal = null,
        bool sameType = false,
        bool keepsStorage = false,
        int[] alternatives = null)
    {
        Kind = kind;
        SameType = sameType;
        SourceParts = sourceParts ?? System.Array.Empty<string>();
        TargetParts = targetParts ?? System.Array.Empty<string>();
        SourceHoldsNull = sourceHoldsNull;
        TargetHoldsNull = targetHoldsNull;
        Refusal = refusal;
        KeepsStorage = keepsStorage;
        Alternatives = alternatives ?? System.Array.Empty<int>();
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

    /// <summary>
    /// For <see cref="DecodedPart.Tuple"/>, <see cref="DecodedPart.Array"/> and <see cref="DecodedPart.Map"/>: whether no
    /// part in it changes meaning or string shape, and no <c>Nullable</c> in the source hides a value. When no enclosing
    /// <c>Nullable</c> hides a row of the part either, a codec of the target that writes the part from its storage writes the
    /// bytes that the write of each part gives.
    /// </summary>
    public bool KeepsStorage { get; }

    /// <summary>
    /// For <see cref="DecodedPart.Variant"/>: for each alternative of the source, in its discriminator order, the
    /// discriminator of the alternative of the target of the same type. <see cref="SourceParts"/> and
    /// <see cref="TargetParts"/> are the alternatives of the two types, in discriminator order.
    /// </summary>
    public int[] Alternatives { get; }
}

/// <summary>
/// The write of a column that a query read as one type into a column of another type. The insert takes the column apart
/// along the two types, and decides at each part from the types of the part and from the column at that place. The
/// canonical values of the two types can mean other values: the count of a <c>DateTime64</c> or a <c>Time64</c> is a
/// count at the scale of its type, and the ordinal of an <c>Enum</c> means the label that its type declares for it. Or a
/// <c>String</c> goes into another type of a string, and its CLR type <see cref="string"/> reads bytes that are not UTF-8
/// as U+FFFD. The insert converts only those parts: through the meaning of their values (a time as
/// <see cref="DateTimeOffset"/>, a duration as <see cref="TimeSpan"/>, a label as <see cref="string"/>), or through their
/// bytes (<c>byte[]</c>). It writes the offsets of an <c>Array</c> or a <c>Map</c>, the null map of a <c>Nullable</c> and
/// every part of the same type from their storage, so their bytes do not change.
/// </summary>
internal sealed partial class ConverterDerivation
{
    /// <summary>
    /// Whether <paramref name="column"/> is the column that a query of <paramref name="source"/> reads: the type parses,
    /// the registry resolves it, and its codec writes the column from its storage.
    /// </summary>
    /// <param name="column">The column to write.</param>
    /// <param name="source">The type that the column says it holds.</param>
    /// <param name="target">The type of the target column, which must parse too.</param>
    /// <param name="context">The resolution context of the target.</param>
    /// <returns>Whether the insert writes the column part by part (<see cref="PlanDecodedPart"/>) into another type.</returns>
    internal bool IsReadColumn(IColumn column, string source, string target, in ResolveContext context)
    {
        if (!TryParse(source, out TypeNode sourceNode) || !TryParse(target, out _))
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

        return sourceCodec.WritesFromStorage(column);
    }

    /// <summary>How the insert writes one part of a column that a query read as <paramref name="source"/>.</summary>
    /// <param name="source">The source type of the part.</param>
    /// <param name="target">The target type of the part.</param>
    /// <param name="context">The resolution context of the target.</param>
    /// <returns>The plan of the part.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    internal DecodedPartPlan PlanDecodedPart(string source, string target, in ResolveContext context)
    {
        TypeNode sourceNode = WithoutAggregateFunction(TypeParser.Parse(source));
        TypeNode targetNode = WithoutAggregateFunction(TypeParser.Parse(target));
        if (SameType(sourceNode, targetNode))
        {
            return new DecodedPartPlan(DecodedPart.Kept, sameType: true);
        }

        // The count of a DateTime64 is an instant and the count of a Time64 is a duration, so neither is written into the
        // other.
        TypeNode sourceLeaf = Unwrapped(sourceNode);
        TypeNode targetLeaf = Unwrapped(targetNode);
        if (TimeKind(CanonicalName(sourceLeaf)) is string sourceKind && TimeKind(CanonicalName(targetLeaf)) is string targetKind && sourceKind != targetKind)
        {
            return new DecodedPartPlan(
                DecodedPart.Refused,
                refusal: $"A value of {sourceLeaf} is {sourceKind} and a value of {targetLeaf} is {targetKind}, so a column read as {sourceLeaf} is not written into {targetLeaf}. Convert the values in the query, or insert the counts as a column of System.Int64 values.");
        }

        bool changes = ChangesMeaning(sourceNode, targetNode) || ChangesStringShape(sourceNode, targetNode);
        string leaf = CanonicalName(Unwrapped(sourceNode));
        if (changes && (MeaningFamily(leaf) is not null || (IsStringLeaf(leaf) && IsStringLeaf(CanonicalName(Unwrapped(targetNode))))))
        {
            return new DecodedPartPlan(DecodedPart.Converted);
        }

        // A String into a Variant whose String alternative takes both the text and the bytes of a value: through the bytes,
        // so a value that is not UTF-8 keeps them. Also a String under Nullable, whose NULL is the NULL of the Variant.
        if (leaf == "String" && PlacesStringBytesAsString(targetNode, in context))
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

        // A composite of the same name and number of arguments on both sides is taken apart, also when no part in it
        // changes; a composite of another shape is written as any other column.
        string name = CanonicalName(sourceNode);
        int arguments = sourceNode.Arguments.Count;
        bool sameShape = name == CanonicalName(targetNode) && arguments == targetNode.Arguments.Count;
        bool keepsStorage = !changes && !HidesValues(sourceNode);
        switch (name)
        {
            case "Array" when sameShape && arguments == 1:
                return new DecodedPartPlan(DecodedPart.Array, new[] { sourceNode.Arguments[0].ToString() }, new[] { targetNode.Arguments[0].ToString() }, keepsStorage: keepsStorage);

            case "Map" when sameShape && arguments == 2:
                return new DecodedPartPlan(
                    DecodedPart.Map,
                    new[] { sourceNode.Arguments[0].ToString(), sourceNode.Arguments[1].ToString() },
                    new[] { targetNode.Arguments[0].ToString(), targetNode.Arguments[1].ToString() },
                    keepsStorage: keepsStorage);

            case "Tuple" when sameShape:
                return new DecodedPartPlan(
                    DecodedPart.Tuple,
                    System.Array.ConvertAll(NamedElementParser.Split(sourceNode), element => element.Type.ToString()),
                    System.Array.ConvertAll(NamedElementParser.Split(targetNode), element => element.Type.ToString()),
                    keepsStorage: keepsStorage);

            case "Variant" when name == CanonicalName(targetNode):
                return PlanVariant(sourceNode, targetNode);

            default:
                return changes
                    ? new DecodedPartPlan(
                        DecodedPart.Refused,
                        refusal: $"The values of {sourceNode} are converted to another type only through Nullable, LowCardinality, Array, Map and Tuple, so a column read as {sourceNode} is written only into a column whose DateTime64, Time64 and Enum types are the same.")
                    : new DecodedPartPlan(DecodedPart.Kept);
        }
    }

    // A Variant into a Variant of other alternatives: each alternative of the source goes to the alternative of the target of
    // the same type, from its storage. The decision uses the two types only, so a column with no row of an alternative
    // that the target does not have is refused too.
    private DecodedPartPlan PlanVariant(TypeNode source, TypeNode target)
    {
        var map = new int[source.Arguments.Count];
        List<string> missing = null;
        for (int i = 0; i < map.Length; i++)
        {
            map[i] = -1;
            for (int j = 0; j < target.Arguments.Count; j++)
            {
                if (SameType(source.Arguments[i], target.Arguments[j]))
                {
                    map[i] = j;
                    break;
                }
            }

            if (map[i] < 0)
            {
                (missing ??= new List<string>()).Add(source.Arguments[i].ToString());
            }
        }

        if (missing is not null)
        {
            return new DecodedPartPlan(
                DecodedPart.Refused,
                refusal: $"A column read as {source} is written into {target} only when each of its alternatives is an alternative of {target}, and {target} has no alternative {string.Join(", ", missing)}. A CAST of the column to {target} in the query converts those values.");
        }

        return new DecodedPartPlan(
            DecodedPart.Variant,
            Names(source.Arguments),
            Names(target.Arguments),
            alternatives: map);
    }

    private static string[] Names(IReadOnlyList<TypeNode> nodes)
    {
        var names = new string[nodes.Count];
        for (int i = 0; i < names.Length; i++)
        {
            names[i] = nodes[i].ToString();
        }

        return names;
    }

    // Whether the type is a Variant whose write places a string and a byte[] in the same alternative, each with no tie, and
    // that alternative is a String or a LowCardinality(String).
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private bool PlacesStringBytesAsString(TypeNode target, in ResolveContext context)
    {
        if (CanonicalName(target) != "Variant")
        {
            return false;
        }

        Derivation derived = Derive(target.ToString(), in context, typeof(object), ConversionDirection.Write);
        if (!derived.Succeeded || derived.Converter is not VariantWriter variant)
        {
            return false;
        }

        int alternative = variant.AlternativeOf(typeof(string));
        if (alternative < 0 || alternative != variant.AlternativeOf(typeof(byte[])))
        {
            return false;
        }

        // A LowCardinality(String) alternative writes a byte[] value with one dictionary entry for each distinct value. A
        // FixedString(N) alternative pads a text and takes a byte[] of exactly N bytes only, so the two give other values.
        TypeNode type = TypeParser.Parse(variant.AlternativeType(alternative));
        if (CanonicalName(type) == "LowCardinality" && type.Arguments.Count == 1)
        {
            type = type.Arguments[0];
        }

        return CanonicalName(type) == "String" && type.Arguments.Count == 0;
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

    // What the count of a DateTime64 or a Time64 means, for the refusal of one into the other.
    private static string TimeKind(string name) => name switch
    {
        "DateTime64" => "an instant",
        "Time64" => "a duration",
        _ => null,
    };

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
