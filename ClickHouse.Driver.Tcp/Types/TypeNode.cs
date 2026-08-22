using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A node in a parsed ClickHouse type: a base <see cref="Name"/> plus zero or more argument nodes. The tree is
/// fully recursive — a nested type argument (<c>Array(String)</c>) is itself a <see cref="TypeNode"/> with its
/// own arguments, while a non-type argument (an integer like <c>16</c>, a quoted enum label like
/// <c>'a' = 1</c>, or a <c>name Type</c> tuple element) is a childless node whose <see cref="Name"/> is the
/// raw token text for the caller to interpret.
/// </summary>
internal sealed class TypeNode
{
    /// <summary>Initializes a new instance of the <see cref="TypeNode"/> class.</summary>
    /// <param name="name">The base type name, or the raw token for a non-type argument.</param>
    /// <param name="arguments">The argument nodes; empty for a plain type or a leaf token.</param>
    public TypeNode(string name, IReadOnlyList<TypeNode> arguments)
    {
        Name = name;
        Arguments = arguments;
    }

    /// <summary>The base type name (e.g. <c>UInt64</c>, <c>Array</c>), or the raw token of a non-type argument.</summary>
    public string Name { get; }

    /// <summary>The argument nodes, in source order; empty when the type takes none.</summary>
    public IReadOnlyList<TypeNode> Arguments { get; }

    /// <summary>Reconstructs a canonical type string (arguments comma-separated), useful for diagnostics.</summary>
    /// <returns>The type string.</returns>
    public override string ToString()
    {
        if (Arguments.Count == 0)
        {
            return Name;
        }

        var builder = new StringBuilder(Name);
        builder.Append('(');
        for (int i = 0; i < Arguments.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(Arguments[i]);
        }

        builder.Append(')');
        return builder.ToString();
    }
}
