using System;
using System.Linq;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Represents a ClickHouse named tuple type (Tuple with field names).
/// Can write from an ITuple, NamedTuple, or IList
/// </summary>
internal class NamedTupleType : ParameterizedType
{
    /// <summary>
    /// Gets or sets the field names for the tuple.
    /// </summary>
    public string[] FieldNames { get; set; }

    public ClickHouseType[] UnderlyingTypes { get; set; }

    public override string Name => "NamedTuple";

    /// <summary>
    /// Parses a named tuple type from syntax tree.
    /// </summary>
    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return NamedTupleType.ParseType(node, parseClickHouseTypeFunc, settings);
    }

    internal static NamedTupleType ParseType(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var fieldNames = new string[node.ChildNodes.Count];
        var types = new ClickHouseType[node.ChildNodes.Count];

        for (int i = 0; i < node.ChildNodes.Count; i++)
        {
            var childNode = node.ChildNodes[i];

            // Named tuple syntax: "field_name Type"
            var parts = childNode.Value.Split(new[] { ' ' }, 2);
            if (parts.Length == 2)
            {
                fieldNames[i] = parts[0];
                // Parse the type part
                var typeNode = new SyntaxTreeNode { Value = parts[1] };
                foreach (var subNode in childNode.ChildNodes)
                {
                    typeNode.ChildNodes.Add(subNode);
                }
                types[i] = parseClickHouseTypeFunc(typeNode);
            }
            else
            {
                throw new ArgumentException($"Wrong number of elements in NamedTuple. Node value: {childNode.Value}");
            }
        }

        return new NamedTupleType
        {
            FieldNames = fieldNames,
            UnderlyingTypes = types
        };
    }

    /// <summary>
    /// Returns a string representation of the named tuple type.
    /// </summary>
    public override string ToString()
    {
        var fields = FieldNames.Zip(UnderlyingTypes, (name, type) => $"{name} {type}");
        return $"{Name}({string.Join(", ", fields)})";
    }

    public override Type FrameworkType => typeof(NamedTuple);

    /// <summary>
    /// Reads a named tuple from the binary reader.
    /// </summary>
    public override object Read(ExtendedBinaryReader reader)
    {
        var count = UnderlyingTypes.Length;
        var values = new object[count];

        for (var i = 0; i < count; i++)
        {
            var value = UnderlyingTypes[i].Read(reader);
            values[i] = ClearDBNull(value);
        }

        return new NamedTuple(FieldNames, values)
        {
            UnderlyingTypes = UnderlyingTypes,
        };
    }

    /// <summary>
    /// Writes a named tuple to the binary writer.
    /// </summary>
    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        // Named tuple can be written like a regular tuple
        if (value is NamedTuple namedTuple)
        {
            if (namedTuple.Length != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in NamedTuple", nameof(value));

            for (var i = 0; i < namedTuple.Length; i++)
            {
                UnderlyingTypes[i].Write(writer, namedTuple[i]);
            }
            return;
        }

#if !NET462
        if (value is ITuple tuple)
        {
            if (tuple.Length != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in Tuple", nameof(value));
            for (var i = 0; i < tuple.Length; i++)
            {
                UnderlyingTypes[i].Write(writer, tuple[i]);
            }
            return;
        }
#endif

        if (value is System.Collections.IList list)
        {
            if (list.Count != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in Tuple", nameof(value));
            for (var i = 0; i < list.Count; i++)
            {
                UnderlyingTypes[i].Write(writer, list[i]);
            }
            return;
        }

        if (value is System.Collections.Generic.Dictionary<string, object> dict)
        {
            if (dict.Count != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in Dictionary", nameof(value));

            for (var i = 0; i < FieldNames.Length; i++)
            {
                var fieldName = FieldNames[i];
                if (!dict.TryGetValue(fieldName, out var fieldValue))
                    throw new ArgumentException($"Dictionary is missing field '{fieldName}'", nameof(value));

                UnderlyingTypes[i].Write(writer, fieldValue);
            }
            return;
        }
    }
}
