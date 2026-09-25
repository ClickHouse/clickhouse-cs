using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Splits a composite type node whose arguments use the <c>name Type</c> element syntax — <c>Tuple(a Int32, …)</c>
/// and <c>Nested(a T1, …)</c> — into its elements, separating each element's name from its type.
/// </summary>
internal static class NamedElementParser
{
    /// <summary>
    /// Splits a composite node's argument nodes into (element name, element type) pairs. The tokenizer breaks only
    /// on <c>,</c> <c>(</c> <c>)</c>, so a named element arrives as a single node whose <see cref="TypeNode.Name"/>
    /// glues the element name to the base type name with a space — <c>a Int32</c>, or <c>a Array</c> with the
    /// array's own argument nodes hanging off it. An unnamed element's name has no space. The name is taken up to
    /// the first space; the remainder plus the node's arguments reconstruct the element's own type node.
    /// </summary>
    /// <param name="composite">The parsed composite node (e.g. a <c>Tuple(...)</c> or <c>Nested(...)</c>).</param>
    /// <returns>One pair per element, in declaration order; the name is null for an unnamed element.</returns>
    public static (string Name, TypeNode Type)[] Split(TypeNode composite)
    {
        var elements = new (string, TypeNode)[composite.Arguments.Count];
        for (int i = 0; i < composite.Arguments.Count; i++)
        {
            elements[i] = SplitElement(composite.Arguments[i]);
        }

        return elements;
    }

    /// <summary>Separates one element's name from its type.</summary>
    /// <param name="argument">The element's argument node.</param>
    /// <returns>The pair; the name is null when the element is unnamed.</returns>
    private static (string Name, TypeNode Type) SplitElement(TypeNode argument)
    {
        // Split after a quoted name and decode the server's identifier escapes.
        if (argument.Name.Length > 0 && argument.Name[0] == '`')
        {
            if (QuotedText.TryRead(argument.Name, 0, out string quoted, out int afterQuote)
                && afterQuote < argument.Name.Length
                && char.IsWhiteSpace(argument.Name[afterQuote]))
            {
                return (quoted, WithBaseName(argument, argument.Name.Substring(afterQuote).TrimStart()));
            }

            // Leave malformed quoted elements intact for the caller's resolution error.
            return (null, argument);
        }

        int space = IndexOfWhitespace(argument.Name);
        if (space < 0)
        {
            return (null, argument);
        }

        // Split at the first whitespace run without retaining leading whitespace in the type.
        return (argument.Name.Substring(0, space), WithBaseName(argument, argument.Name.Substring(space).TrimStart()));
    }

    /// <summary>Rebuilds an element's own type node from its base name, keeping the argument list.</summary>
    /// <param name="argument">The element's argument node.</param>
    /// <param name="baseName">The element's type name, with the element name removed.</param>
    /// <returns>The element's type node.</returns>
    // Preserve an explicit empty argument list such as Tuple().
    private static TypeNode WithBaseName(TypeNode argument, string baseName)
        => new(baseName, argument.Arguments, argument.HasArgumentList);

    private static int IndexOfWhitespace(string value)
    {
        for (int i = 0; i < value.Length; i++)
        {
            if (char.IsWhiteSpace(value[i]))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Parses a full <c>Tuple(...)</c> type string and returns its per-element type strings (names stripped),
    /// validating that the element count matches <paramref name="expectedArity"/>. Used by a typed tuple column's
    /// convenience constructor to stamp its child columns with the right element type.
    /// </summary>
    /// <param name="tupleTypeName">The full tuple type string.</param>
    /// <param name="expectedArity">The number of elements the caller expects.</param>
    /// <returns>The element type strings, in order.</returns>
    /// <exception cref="FormatException">The type is not a tuple of exactly <paramref name="expectedArity"/> elements.</exception>
    public static string[] ElementTypeStrings(string tupleTypeName, int expectedArity)
    {
        TypeNode node = TypeParser.Parse(tupleTypeName);
        if (node.Name != "Tuple" || node.Arguments.Count != expectedArity)
        {
            throw new FormatException(
                $"Type '{tupleTypeName}' is not a Tuple of {expectedArity} element(s).");
        }

        (string Name, TypeNode Type)[] elements = Split(node);
        var types = new string[expectedArity];
        for (int i = 0; i < expectedArity; i++)
        {
            types[i] = elements[i].Type.ToString();
        }

        return types;
    }
}
