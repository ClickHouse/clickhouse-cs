using System;
using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Parses ClickHouse type strings into a fully recursive <see cref="TypeNode"/> tree. Tokenizing is delegated
/// to <see cref="TypeTokenizer"/> (so quotes and nesting are handled once); this builds the tree by recursive
/// descent and validates structure — an empty input, a missing base name, unbalanced parentheses, or trailing
/// characters are rejected rather than silently mis-parsed.
/// </summary>
internal static class TypeParser
{
    /// <summary>Parses a type string into its recursive node tree.</summary>
    /// <param name="type">The ClickHouse type string (e.g. <c>UInt64</c>, <c>Array(Nullable(String))</c>, <c>Decimal(10, 2)</c>).</param>
    /// <returns>The parsed type.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="type"/> is null.</exception>
    /// <exception cref="FormatException"><paramref name="type"/> is empty or malformed.</exception>
    public static TypeNode Parse(string type)
    {
        ArgumentNullException.ThrowIfNull(type);

        List<string> tokens = TypeTokenizer.Tokenize(type).ToList();
        int position = 0;
        TypeNode node = ParseNode(tokens, ref position, type);

        if (position != tokens.Count)
        {
            throw new FormatException($"Malformed type string '{type}': unexpected trailing input after the type.");
        }

        return node;
    }

    /// <summary>Parses one node (a type name plus its optional parenthesized arguments) starting at <paramref name="position"/>.</summary>
    /// <param name="tokens">The full token list.</param>
    /// <param name="position">The cursor into <paramref name="tokens"/>; advanced past the consumed tokens.</param>
    /// <param name="original">The original type string, for error messages.</param>
    /// <returns>The parsed node.</returns>
    /// <exception cref="FormatException">A structural token was found where a name was expected, or the parentheses are unbalanced.</exception>
    private static TypeNode ParseNode(List<string> tokens, ref int position, string original)
    {
        if (position >= tokens.Count)
        {
            throw new FormatException($"Malformed type string '{original}': expected a type name.");
        }

        string name = tokens[position++];
        if (name.Length == 0 || name is "(" or ")" or ",")
        {
            throw new FormatException($"Malformed type string '{original}': expected a type name, found '{name}'.");
        }

        // A name not followed by '(' is a plain type or a leaf argument (integer, quoted label, name/type pair).
        if (position >= tokens.Count || tokens[position] != "(")
        {
            return new TypeNode(name, Array.Empty<TypeNode>());
        }

        position++; // consume '('
        var arguments = new List<TypeNode>();
        while (true)
        {
            arguments.Add(ParseNode(tokens, ref position, original));

            if (position >= tokens.Count)
            {
                throw new FormatException($"Malformed type string '{original}': unbalanced '(' — missing ')'.");
            }

            string separator = tokens[position++];
            if (separator == ")")
            {
                break;
            }

            if (separator != ",")
            {
                throw new FormatException($"Malformed type string '{original}': expected ',' or ')', found '{separator}'.");
            }
        }

        return new TypeNode(name, arguments);
    }
}
