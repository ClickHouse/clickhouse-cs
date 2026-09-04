using System;
using System.Globalization;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The two aggregate-state type families, which the wire treats very differently.
/// <c>SimpleAggregateFunction(func, T)</c> is a plain <c>T</c> — the functions it admits are the ones whose
/// state <em>is</em> the value — so it resolves to <c>T</c>'s codec. <c>AggregateFunction(func, ...)</c> is the
/// opposite: its body is the function's own intermediate state, encoded by that function alone, so there is
/// nothing generic to decode and the client refuses it by name.
/// </summary>
internal static class AggregateFunctionColumnCodecs
{
    /// <summary>
    /// Resolves <c>SimpleAggregateFunction(func, T)</c> to <c>T</c>'s codec. The function name affects only how
    /// the server merges rows, never their encoding, so the alias adds no bytes and needs no codec of its own.
    /// The inner type is resolved recursively, so a composite or aliased <c>T</c> works like any other.
    /// </summary>
    /// <param name="node">The parsed node; its arguments are the function name and the inner type.</param>
    /// <param name="context">The resolution context, forwarded to the inner codec's factory.</param>
    /// <param name="registry">The registry used to resolve the inner type's codec.</param>
    /// <returns>The inner type's codec.</returns>
    /// <exception cref="FormatException">The type has other than two arguments.</exception>
    public static IColumnCodec CreateSimple(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        // Always exactly two: the function and one value type. A parameterized function
        // ("groupArrayLastArray(10)", which the server does accept) parses as a single node with its own
        // arguments, so it does not widen this list — and the server rejects a second value type outright.
        if (node.Arguments.Count != 2)
        {
            throw new FormatException($"SimpleAggregateFunction type '{node}' must have a function name and exactly one inner type argument.");
        }

        return registry.ResolveNode(node.Arguments[1], in context);
    }

    /// <summary>
    /// Refuses <c>AggregateFunction(func, ...)</c>, naming the function and the query that reads it. The column
    /// holds serialized intermediate states, not values: the encoding belongs to the aggregate function rather
    /// than to the Native format, so no client-side decode is possible for the family as a whole. Merging the
    /// state server-side turns the column into an ordinary typed one this client does read.
    /// </summary>
    /// <param name="node">The parsed node; its first argument names the function.</param>
    /// <returns>Never returns.</returns>
    /// <exception cref="FormatException">The type names no function.</exception>
    /// <exception cref="NotSupportedException">Otherwise always — the refusal this method exists for.</exception>
    public static IColumnCodec RefuseAggregateFunction(TypeNode node)
    {
        if (node.Arguments.Count == 0)
        {
            throw new FormatException($"AggregateFunction type '{node}' must name an aggregate function.");
        }

        // A parameterized function ("quantiles(0.5, 0.9)") parses as a node of its own: the Merge combinator
        // attaches to the bare name, and the parameters move to their own list ahead of the column —
        // quantilesMerge(0.5, 0.9)(column). Suggesting the bare form there hands back a query the server rejects.
        TypeNode function = FunctionOf(node);
        string parameters = function.Arguments.Count > 0 ? $"({string.Join(", ", function.Arguments)})" : string.Empty;

        throw new NotSupportedException(
            $"Column type '{node}' holds the intermediate states of the '{function.Name}' aggregate function, whose encoding is the " +
            $"function's own; this client cannot decode it. Merge the state in the query instead — " +
            $"'SELECT {function.Name}Merge{parameters}(column) ...' — which returns an ordinary column.");
    }

    /// <summary>Picks out the argument that names the aggregate function.</summary>
    /// <param name="node">The parsed <c>AggregateFunction</c> node, with at least one argument.</param>
    /// <returns>The argument naming the function.</returns>
    private static TypeNode FunctionOf(TypeNode node)
    {
        // Some states carry a leading serialization version, which is not a function name: 26.6 reports
        // sumMapState(...) as AggregateFunction(1, sumMap, Array(UInt64), Array(UInt64)), and the same for
        // minMap, maxMap and sumMapFiltered([1, 2]). Naming that argument suggests '1Merge(column)'.
        if (node.Arguments.Count > 1
            && node.Arguments[0].Arguments.Count == 0
            && uint.TryParse(node.Arguments[0].Name, NumberStyles.None, CultureInfo.InvariantCulture, out _))
        {
            return node.Arguments[1];
        }

        return node.Arguments[0];
    }
}
