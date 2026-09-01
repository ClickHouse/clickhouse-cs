using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for <c>Enum8</c> / <c>Enum16</c>, whose values ride the wire as their underlying signed ordinal
/// (<see cref="sbyte"/> for <c>Enum8</c>, <see cref="short"/> for <c>Enum16</c>). The read/write of those
/// ordinals is exactly the fixed-width integer path; this codec adds parsing and retention of the
/// <c>'label' = ordinal</c> map from the type string, so a malformed enum definition fails clearly and the map is
/// available on both sides.
///
/// <para>
/// The decoded column surfaces the raw ordinal (an <see cref="IColumn{T}"/> of the underlying integer) and
/// implements <see cref="IEnumColumn"/>, which carries the members. A label is also a reading
/// (<see cref="TryProjectRead"/> offers <see cref="string"/>) and a write shape: a column of labels converts to
/// its ordinals on the way out, so what a caller reads as labels can be written back as labels.
/// </para>
/// </summary>
/// <typeparam name="T">The underlying signed integer type (<see cref="sbyte"/> or <see cref="short"/>).</typeparam>
internal sealed class EnumColumnCodec<T> : IColumnCodec
    where T : unmanaged
{
    private static readonly MethodInfo LabelMethod =
        typeof(EnumMemberTable).GetMethod(nameof(EnumMemberTable.Label), BindingFlags.Public | BindingFlags.Instance);

    private readonly FixedWidthColumnCodec<T> underlying;

    private readonly EnumMemberTable members;

    private readonly T nullPlaceholder;

    private EnumColumnCodec(
        string typeName,
        IReadOnlyDictionary<string, T> labelToOrdinal,
        IReadOnlyDictionary<T, string> ordinalToLabel,
        EnumMemberTable members,
        T nullPlaceholder)
    {
        TypeName = typeName;
        underlying = new FixedWidthColumnCodec<T>(typeName);
        LabelToOrdinal = labelToOrdinal;
        OrdinalToLabel = ordinalToLabel;
        this.members = members;
        this.nullPlaceholder = nullPlaceholder;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(T);

    /// <inheritdoc/>
    // A declared member ordinal, not default(T): the ordinal 0 need not be a member, and the server can reject
    // an undeclared ordinal even at a Nullable(Enum) null position where it is only a placeholder.
    public object NullPlaceholder => nullPlaceholder;

    /// <summary>The enum's declared members, mapping each label to its underlying ordinal.</summary>
    public IReadOnlyDictionary<string, T> LabelToOrdinal { get; }

    /// <summary>The reverse map, from ordinal to label.</summary>
    public IReadOnlyDictionary<T, string> OrdinalToLabel { get; }

    /// <summary>
    /// A label is a reading as well as the ordinal. Diagnostics only; <see cref="TryProjectRead"/> is the
    /// authority.
    /// </summary>
    public IReadOnlyList<Type> ReadableElementTypes { get; } = new[] { typeof(T), typeof(string) };

    /// <summary>A column of labels writes as well as a column of ordinals.</summary>
    public IReadOnlyList<Type> WritableElementTypes { get; } = new[] { typeof(T), typeof(string) };

    /// <summary>
    /// Builds an enum codec by parsing the <c>'label' = ordinal</c> members from the type node's arguments.
    /// </summary>
    /// <param name="node">The parsed <c>Enum8</c>/<c>Enum16</c> type node.</param>
    /// <param name="parseOrdinal">Parses and range-checks a member's ordinal into the underlying type.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">A member is malformed or an ordinal is out of the underlying type's range.</exception>
    public static EnumColumnCodec<T> Create(TypeNode node, Func<long, string, T> parseOrdinal)
    {
        var labelToOrdinal = new Dictionary<string, T>(StringComparer.Ordinal);
        var ordinalToLabel = new Dictionary<T, string>();
        var declared = new List<KeyValuePair<string, long>>(node.Arguments.Count);
        T nullPlaceholder = default;
        bool haveMember = false;

        foreach (TypeNode argument in node.Arguments)
        {
            (string label, long ordinal) = EnumColumnCodec.ParseMember(argument.Name, node);
            T value = parseOrdinal(ordinal, node.Name);
            if (!labelToOrdinal.TryAdd(label, value))
            {
                throw new FormatException($"Enum type '{node}' declares the label '{label}' more than once.");
            }

            if (!ordinalToLabel.TryAdd(value, label))
            {
                throw new FormatException($"Enum type '{node}' declares the ordinal {ordinal} more than once.");
            }

            declared.Add(new KeyValuePair<string, long>(label, ordinal));

            if (!haveMember)
            {
                // The first declared member is a guaranteed-valid placeholder for a Nullable(Enum) null row.
                nullPlaceholder = value;
                haveMember = true;
            }
        }

        if (!haveMember)
        {
            throw new FormatException($"Enum type '{node}' declares no members.");
        }

        string typeName = node.ToString();
        return new EnumColumnCodec<T>(typeName, labelToOrdinal, ordinalToLabel, new EnumMemberTable(typeName, declared), nullPlaceholder);
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        IColumn ordinals = await underlying.ReadColumnAsync(reader, columnName, columnType, rowCount, cancellationToken).ConfigureAwait(false);
        try
        {
            return new EnumColumn<T>((IColumn<T>)ordinals, members);
        }
        catch
        {
            ordinals.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    // The labels are a placeholder shape too, so a Nullable(Enum) written from labels has one for its null rows.
    public object NullPlaceholderAs(Type writeType)
    {
        if (writeType == typeof(T))
        {
            return nullPlaceholder;
        }

        return writeType == typeof(string)
            ? OrdinalToLabel[nullPlaceholder]
            : throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
    }

    /// <inheritdoc/>
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, typeof(T), TypeName);

        if (targetType == typeof(T))
        {
            projected = value;
            return true;
        }

        // The members are a constant of this codec, so the lookup is a call on it with the ordinal widened to long
        // — the same table the public IEnumColumn view answers from.
        projected = targetType == typeof(string)
            ? Expression.Call(Expression.Constant(members), LabelMethod, Expression.Convert(value, typeof(long)))
            : null;
        return projected is not null;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => underlying.CanWrite(column) || column is IColumn<string>;

    /// <inheritdoc/>
    // A column of labels becomes its ordinals here rather than in WriteColumn, so a caller that asks for the
    // canonical write column gets the element type this codec declares for it.
    public IColumn ToCanonicalWriteColumn(IColumn column)
        => column is IColumn<string> labels && column is not IColumn<T>
            ? new ProjectedColumn<string, T>(TypeName, labels, ToOrdinal)
            : column;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
        => underlying.WriteColumn(writer, ToCanonicalWriteColumn(column), start, length);

    /// <summary>The ordinal a label is declared with.</summary>
    /// <exception cref="ArgumentException">The type declares no member with that label, or the label is null.</exception>
    private T ToOrdinal(string label)
        => label is not null && LabelToOrdinal.TryGetValue(label, out T ordinal)
            ? ordinal
            : throw members.NoSuchLabel(label, nameof(label));
}

/// <summary>
/// Factory for the bare <c>Enum</c> name, and the member parsing the three factories share.
/// </summary>
internal static class EnumColumnCodec
{
    /// <summary>Builds the codec for a bare <c>Enum</c>, whose width comes from the declared ordinals.</summary>
    /// <param name="node">The parsed <c>Enum</c> type node.</param>
    /// <returns>An <c>Enum8</c> or <c>Enum16</c> codec, named for the width chosen.</returns>
    /// <exception cref="FormatException">A member is malformed, or no member is declared.</exception>
    public static IColumnCodec Create(TypeNode node)
    {
        // Verified on 26.6: the server takes Enum8 while every ordinal is in the Int8 range and Enum16
        // otherwise, and reports the column under the width it chose.
        bool fitsInt8 = true;
        foreach (TypeNode argument in node.Arguments)
        {
            (_, long ordinal) = ParseMember(argument.Name, node);
            if (ordinal is < sbyte.MinValue or > sbyte.MaxValue)
            {
                fitsInt8 = false;
            }
        }

        var sized = new TypeNode(fitsInt8 ? "Enum8" : "Enum16", node.Arguments, node.HasArgumentList);
        return fitsInt8 ? Enum8ColumnCodec.Create(sized) : Enum16ColumnCodec.Create(sized);
    }

    /// <summary>Parses a single <c>'label' = ordinal</c> member token into its label and ordinal.</summary>
    internal static (string Label, long Ordinal) ParseMember(string token, TypeNode node)
    {
        // A member is a single-quoted label, then '=', then a signed integer, e.g. 'a' = -1. The label carries the
        // server's own escaping (a label with a newline arrives as 'a\nb'), and may contain '=' or a comma inside
        // the quotes, so scan and decode the quoted run rather than splitting naively on '='.
        int open = token.IndexOf('\'');
        if (open < 0)
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': expected a quoted label.");
        }

        if (!QuotedText.TryRead(token, open, out string label, out int afterLabel))
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': unterminated label.");
        }

        string rest = token.Substring(afterLabel).Trim();
        if (rest.Length == 0 || rest[0] != '=')
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': expected '= ordinal' after the label.");
        }

        string ordinalText = rest.Substring(1).Trim();
        if (!long.TryParse(ordinalText, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long ordinal))
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': '{ordinalText}' is not a valid ordinal.");
        }

        return (label, ordinal);
    }
}

/// <summary>Factory for the <c>Enum8</c> codec (underlying <see cref="sbyte"/>).</summary>
internal static class Enum8ColumnCodec
{
    /// <summary>Builds an <c>Enum8</c> codec from its type node.</summary>
    public static IColumnCodec Create(TypeNode node) => EnumColumnCodec<sbyte>.Create(node, static (ordinal, typeName) =>
    {
        if (ordinal is < sbyte.MinValue or > sbyte.MaxValue)
        {
            throw new FormatException($"{typeName} ordinal {ordinal} is out of the Int8 range [-128, 127].");
        }

        return (sbyte)ordinal;
    });
}

/// <summary>Factory for the <c>Enum16</c> codec (underlying <see cref="short"/>).</summary>
internal static class Enum16ColumnCodec
{
    /// <summary>Builds an <c>Enum16</c> codec from its type node.</summary>
    public static IColumnCodec Create(TypeNode node) => EnumColumnCodec<short>.Create(node, static (ordinal, typeName) =>
    {
        if (ordinal is < short.MinValue or > short.MaxValue)
        {
            throw new FormatException($"{typeName} ordinal {ordinal} is out of the Int16 range [-32768, 32767].");
        }

        return (short)ordinal;
    });
}
