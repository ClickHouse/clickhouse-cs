using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Types;

internal class EnumType : ParameterizedType
{
    internal readonly Dictionary<string, int> Values;
    private readonly Dictionary<int, string> reverseValues;

    public EnumType()
    {
        Values = new();
        reverseValues = new();
    }

    protected EnumType(Dictionary<string, int> values)
    {
        this.Values = values;
        reverseValues = new Dictionary<int, string>(values.Count);
        foreach (var kvp in values)
        {
            reverseValues[kvp.Value] = kvp.Key;
        }
    }

    public override string Name => "Enum";

    public override Type FrameworkType => typeof(string);

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        string typeName = TypeConverter.ExtractTypeName(node);

        switch (typeName)
        {
            // The server types a bare Enum as Enum16 when an ordinal does not fit in Int8; this client reads it as
            // Enum8 (issue #720).
            case "Enum":
                return new Enum8Type(ParseEnumMembers(node, "Enum16", short.MinValue, short.MaxValue));
            case "Enum8":
                return new Enum8Type(ParseEnumMembers(node, typeName, sbyte.MinValue, sbyte.MaxValue));
            case "Enum16":
                return new Enum16Type(ParseEnumMembers(node, typeName, short.MinValue, short.MaxValue));
            default: throw new ArgumentOutOfRangeException($"Unsupported Enum type: {node.Value}");
        }
    }

    public int Lookup(string key) => Values[key];

    public bool TryLookup(string key, out int value) => Values.TryGetValue(key, out value);

    public string Lookup(int value) => reverseValues.TryGetValue(value, out var key) ? key : throw new KeyNotFoundException($"Enum value {value} not found");

    // A write must reject a value that is not a member: RowBinary carries only the ordinal, and the server stores
    // it without checking it against the members, so text reads of the row then fail with UNKNOWN_ELEMENT_OF_ENUM.
    // Convert maps null to the ordinal 0, which the type need not declare.
    protected object RequireNotNull(object value) => value is null || value is DBNull
        ? throw new ArgumentException($"A null is not a label of '{this}'. Declare the target Nullable to carry nulls.")
        : value;

    // A type parsed without its members (a bare Enum8 in InsertOptions.ColumnTypes) has nothing to check against.
    protected int RequireDeclared(int ordinal) => reverseValues.Count == 0 || reverseValues.ContainsKey(ordinal)
        ? ordinal
        : throw new ArgumentException($"The type '{this}' declares no member with the ordinal {ordinal.ToString(CultureInfo.InvariantCulture)}.");

    // Renders the ClickHouse declaration form, e.g. Enum8('a' = 1, 'b' = 2), so the result round-trips
    // through TypeConverter.ParseClickHouseType — SchemaDescriber writes this into GetSchemaTable()'s
    // ProviderType, and PocoTypeRegistry keys its row-reader cache on it. Labels are quoted and escaped
    // because an unquoted label containing a comma or a quote would otherwise re-parse as the wrong members.
    public override string ToString() =>
        $"{Name}({string.Join(", ", Values.Select(kvp => $"{kvp.Key.Escape().QuoteSingle()} = {kvp.Value.ToString(CultureInfo.InvariantCulture)}"))})";

    public override object Read(ExtendedBinaryReader reader) => throw new NotImplementedException();

    public override void Write(ExtendedBinaryWriter writer, object value) => throw new NotImplementedException();

    // A member may omit its ordinal. The server then numbers it like this: the first member's ordinal (1 when the
    // first member has none) plus the member's position, counted from 0. After the first member, either every
    // member declares an ordinal or none does. When a member omits its ordinal, the server also rejects every
    // ordinal outside the range of the type. A definition in which every member declares its ordinal is not
    // range-checked here: 26.3 accepts an ordinal outside the range and wraps it, as the writers do.
    private static Dictionary<string, int> ParseEnumMembers(SyntaxTreeNode node, string rangeName, int min, int max)
    {
        var members = node.ChildNodes.Select(cn => ParseEnumMember(cn.Value)).ToList();

        var omittedAfterFirst = members.Skip(1).Count(member => member.Ordinal is null);
        if (omittedAfterFirst != 0 && omittedAfterFirst != members.Count - 1)
            throw new FormatException($"Invalid enum definition: {node}. After the first member, either every member declares an ordinal or none does.");

        var numbered = members.Exists(member => member.Ordinal is null);
        long first = members.Count > 0 ? members[0].Ordinal ?? 1 : 1;
        var values = new Dictionary<string, int>(members.Count);
        for (var i = 0; i < members.Count; i++)
        {
            var ordinal = members[i].Ordinal ?? first + i;
            if (numbered && (ordinal < min || ordinal > max))
            {
                throw new FormatException(
                    $"Invalid enum definition: {node}. The ordinal {ordinal.ToString(CultureInfo.InvariantCulture)} of '{members[i].Label}' is out of the {rangeName} " +
                    $"range [{min.ToString(CultureInfo.InvariantCulture)}, {max.ToString(CultureInfo.InvariantCulture)}].");
            }

            values.Add(members[i].Label, (int)ordinal);
        }

        return values;
    }

    private static (string Label, int? Ordinal) ParseEnumMember(string value)
    {
        // The '=' that separates the ordinal is outside the quoted label, which can contain '=' itself.
        var labelEnd = value.Length > 0 && value[0] == '\'' ? ClosingQuoteIndex(value) : -1;
        if (labelEnd > 0 && labelEnd == value.Length - 1)
            return (Regex.Unescape(value[1..^1]), null);

        var separatorIndex = value.LastIndexOf('=');
        if (separatorIndex <= labelEnd)
            throw new FormatException($"Invalid enum member definition: {value}");

        var rawLabel = value[..separatorIndex].Trim();
        var rawValue = value[(separatorIndex + 1)..].Trim();

        if (rawLabel.Length >= 2 && rawLabel[0] == '\'' && rawLabel[^1] == '\'')
        {
            rawLabel = rawLabel[1..^1];
        }

        return (Regex.Unescape(rawLabel), Convert.ToInt32(rawValue, CultureInfo.InvariantCulture));
    }

    // Skips a backslash escape and a doubled quote, both of which the server accepts inside a label.
    // Returns -1 when the label is not closed.
    private static int ClosingQuoteIndex(string value)
    {
        for (var i = 1; i < value.Length; i++)
        {
            if (value[i] == '\\' || (value[i] == '\'' && i + 1 < value.Length && value[i + 1] == '\''))
                i++;
            else if (value[i] == '\'')
                return i;
        }

        return -1;
    }
}
