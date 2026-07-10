using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for <c>Enum8</c> / <c>Enum16</c>, whose values ride the wire as their underlying signed ordinal
/// (<see cref="sbyte"/> for <c>Enum8</c>, <see cref="short"/> for <c>Enum16</c>). The read/write of those
/// ordinals is exactly the fixed-width integer path; this codec adds parsing and retention of the
/// <c>'label' = ordinal</c> map from the type string, so a malformed enum definition fails clearly and the map
/// is available (a row-materialization tier can map an ordinal back to its label).
///
/// <para>
/// The decoded column surfaces the raw ordinal (an <see cref="IColumn{T}"/> of the underlying integer); the
/// column's stamped type string still carries the full <c>Enum8(...)</c> definition, so the labels are never
/// lost.
/// </para>
/// </summary>
/// <typeparam name="T">The underlying signed integer type (<see cref="sbyte"/> or <see cref="short"/>).</typeparam>
internal sealed class EnumColumnCodec<T> : IColumnCodec
    where T : unmanaged
{
    private readonly FixedWidthColumnCodec<T> underlying;

    private EnumColumnCodec(string typeName, IReadOnlyDictionary<string, T> labelToOrdinal, IReadOnlyDictionary<T, string> ordinalToLabel)
    {
        TypeName = typeName;
        underlying = new FixedWidthColumnCodec<T>(typeName);
        LabelToOrdinal = labelToOrdinal;
        OrdinalToLabel = ordinalToLabel;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int? FixedRowByteSize => underlying.FixedRowByteSize;

    /// <summary>The enum's declared members, mapping each label to its underlying ordinal.</summary>
    public IReadOnlyDictionary<string, T> LabelToOrdinal { get; }

    /// <summary>The reverse map, from ordinal to label.</summary>
    public IReadOnlyDictionary<T, string> OrdinalToLabel { get; }

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

        foreach (TypeNode argument in node.Arguments)
        {
            (string label, long ordinal) = ParseMember(argument.Name, node);
            T value = parseOrdinal(ordinal, node.Name);
            if (!labelToOrdinal.TryAdd(label, value))
            {
                throw new FormatException($"Enum type '{node}' declares the label '{label}' more than once.");
            }

            if (!ordinalToLabel.TryAdd(value, label))
            {
                throw new FormatException($"Enum type '{node}' declares the ordinal {ordinal} more than once.");
            }
        }

        return new EnumColumnCodec<T>(node.ToString(), labelToOrdinal, ordinalToLabel);
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => underlying.ReadColumnAsync(reader, columnName, columnType, rowCount, cancellationToken);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => underlying.CanWrite(column);

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length) => underlying.WriteColumn(writer, column, start, length);

    /// <summary>Parses a single <c>'label' = ordinal</c> member token into its label and ordinal.</summary>
    private static (string Label, long Ordinal) ParseMember(string token, TypeNode node)
    {
        // A member is a single-quoted label, then '=', then a signed integer, e.g. 'a' = -1. The label may
        // contain escaped quotes (\') and backslashes (\\), and may itself contain '=' inside the quotes, so
        // scan the quoted run rather than splitting naively on '='.
        int open = token.IndexOf('\'');
        if (open < 0)
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': expected a quoted label.");
        }

        var label = new System.Text.StringBuilder();
        int i = open + 1;
        bool closed = false;
        for (; i < token.Length; i++)
        {
            char c = token[i];
            if (c == '\\' && i + 1 < token.Length)
            {
                label.Append(token[++i]);
                continue;
            }

            if (c == '\'')
            {
                closed = true;
                i++;
                break;
            }

            label.Append(c);
        }

        if (!closed)
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': unterminated label.");
        }

        string rest = token.Substring(i).Trim();
        if (rest.Length == 0 || rest[0] != '=')
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': expected '= ordinal' after the label.");
        }

        string ordinalText = rest.Substring(1).Trim();
        if (!long.TryParse(ordinalText, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long ordinal))
        {
            throw new FormatException($"Malformed enum member '{token}' in type '{node}': '{ordinalText}' is not a valid ordinal.");
        }

        return (label.ToString(), ordinal);
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
