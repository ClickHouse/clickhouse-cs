using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// One enum type's declared members, both ways round, with the ordinals widened to <see cref="long"/> so neither
/// the public <see cref="IEnumColumn"/> view nor the label projection has to be generic over the storage width.
/// Built once per codec and shared by every column that codec decodes.
/// </summary>
internal sealed class EnumMemberTable
{
    // Enum labels are case-sensitive on the server, so both directions match ordinally.
    private readonly Dictionary<long, string> labelByOrdinal;
    private readonly Dictionary<string, long> ordinalByLabel;

    /// <summary>Initializes a table over the members parsed from a type string.</summary>
    /// <param name="typeName">The full enum type string, for diagnostics.</param>
    /// <param name="members">The members in declaration order; labels and ordinals are both unique.</param>
    public EnumMemberTable(string typeName, IReadOnlyList<KeyValuePair<string, long>> members)
    {
        TypeName = typeName;

        // Copied and wrapped: the caller passes the list it built while parsing, and Members is public API, so a
        // consumer casting it back to that list could otherwise leave it disagreeing with the two maps below.
        var declared = new KeyValuePair<string, long>[members.Count];
        labelByOrdinal = new Dictionary<long, string>(members.Count);
        ordinalByLabel = new Dictionary<string, long>(members.Count, StringComparer.Ordinal);
        for (int i = 0; i < members.Count; i++)
        {
            KeyValuePair<string, long> member = members[i];
            declared[i] = member;
            labelByOrdinal[member.Value] = member.Key;
            ordinalByLabel[member.Key] = member.Value;
        }

        Members = Array.AsReadOnly(declared);
    }

    /// <summary>The full enum type string.</summary>
    public string TypeName { get; }

    /// <summary>The declared members, in declaration order.</summary>
    public IReadOnlyList<KeyValuePair<string, long>> Members { get; }

    /// <summary>The label declared for <paramref name="ordinal"/>, if any.</summary>
    public bool TryGetLabel(long ordinal, out string label) => labelByOrdinal.TryGetValue(ordinal, out label);

    /// <summary>The ordinal declared for <paramref name="label"/>, if any.</summary>
    public bool TryGetOrdinal(string label, out long ordinal)
    {
        ArgumentNullException.ThrowIfNull(label);
        return ordinalByLabel.TryGetValue(label, out ordinal);
    }

    /// <summary>The label declared for <paramref name="ordinal"/>.</summary>
    /// <exception cref="KeyNotFoundException">The type declares no member with that ordinal.</exception>
    public string Label(long ordinal) => TryGetLabel(ordinal, out string label)
        ? label
        : throw new KeyNotFoundException(
            $"The type '{TypeName}' declares no member with the ordinal {ordinal.ToString(CultureInfo.InvariantCulture)}.");

    /// <summary>
    /// Creates the exception for a label the type does not declare. The message names the offending label, which
    /// identifies the value better than its row would: a wrong label is usually wrong at every row.
    /// </summary>
    /// <param name="label">The label that was not found, or null.</param>
    /// <param name="paramName">The parameter name to report.</param>
    /// <returns>The exception to throw.</returns>
    public ArgumentException NoSuchLabel(string label, string paramName) => new(
        label is null
            ? $"A null is not a label of '{TypeName}'. Declare the target Nullable to carry nulls."
            : $"'{label}' is not a label of '{TypeName}'. Its labels are: {Describe()}.",
        paramName);

    // Bounded: an Enum16 may declare thousands of members, and a message listing them all helps nobody.
    private string Describe()
    {
        const int limit = 10;
        string listed = string.Join(", ", Members.Take(limit).Select(member => $"'{member.Key}'"));
        return Members.Count <= limit
            ? listed
            : $"{listed} and {(Members.Count - limit).ToString(CultureInfo.InvariantCulture)} more";
    }
}
