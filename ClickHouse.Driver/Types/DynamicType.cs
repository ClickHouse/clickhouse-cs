using System;
using System.Collections.Concurrent;
using System.Globalization;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class DynamicType : ParameterizedType
{
    /// <summary>
    /// Cache for inferred ClickHouse types from .NET types.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, ClickHouseType> InferredTypeCache = new();

    public override Type FrameworkType => typeof(object);

    public override string Name => "Dynamic";

    public TypeSettings TypeSettings { get; init; }

    /// <summary>
    /// The type as the server spells it, which is <c>Dynamic</c> or <c>Dynamic(max_types=N)</c>.
    /// </summary>
    private string DeclaredName { get; init; } = "Dynamic";

    public override string ToString() => DeclaredName;

    /// <summary>
    /// Parses the arguments of a <c>Dynamic(max_types=N)</c> declaration. <c>max_types</c> only
    /// bounds the set of types the server tracks for the column; it does not change the wire
    /// layout, which is self-describing per value, so the argument is kept in the type name and
    /// otherwise ignored.
    /// </summary>
    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        foreach (var argument in node.ChildNodes)
        {
            if (!IsMaxTypes(argument.Value))
            {
                throw new ArgumentException($"Dynamic type '{node}' has unsupported argument '{argument.Value}'; only 'max_types=N' is recognized.", nameof(node));
            }
        }

        return new DynamicType
        {
            TypeSettings = settings,

            // Not node.ToString(): a named element carries its name in the node value
            // ("d Dynamic(max_types=3)"), and the name is not part of the type.
            DeclaredName = node.ChildNodes.Count == 0
                ? "Dynamic"
                : $"Dynamic({string.Join(", ", node.ChildNodes)})",
        };
    }

    private static bool IsMaxTypes(string argument)
    {
        var separator = argument.IndexOf('=');
        if (separator < 0)
            return false;

        return string.Equals(argument.Substring(0, separator).Trim(), "max_types", StringComparison.OrdinalIgnoreCase)
            && int.TryParse(argument.AsSpan(separator + 1).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out _);
    }

    public override object Read(ExtendedBinaryReader reader) =>
        BinaryTypeDecoder.
            FromByteCode(reader, TypeSettings).
            Read(reader);

    /// <summary>
    /// Writes a value with its type header for dynamic type encoding.
    /// The type is inferred from the value's .NET type and cached.
    /// </summary>
    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is null || value is DBNull)
        {
            writer.Write(BinaryTypeIndex.Nothing);
            return;
        }
        var inferredType = GetCachedInferredType(value.GetType());
        BinaryTypeDescriptionWriter.WriteTypeHeader(writer, inferredType);
        inferredType.Write(writer, value);
    }

    /// <summary>
    /// Use the type converter to infer the ClickHouse type from the .NET type, and cache the results.
    /// </summary>
    private static ClickHouseType GetCachedInferredType(Type type)
        => InferredTypeCache.GetOrAdd(type, TypeConverter.ToClickHouseType);
}
