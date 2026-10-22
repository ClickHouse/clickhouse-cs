using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// One column of an outgoing INSERT block: the wire header (<see cref="Name"/>, <see cref="TypeName"/>), the
/// <see cref="Codec"/> of the target type, the <see cref="Write"/> that serializes the body, and the caller's
/// <see cref="Values"/>. Header and codec come from the server's sample block, not the value column, so the wire always
/// carries the target's name and type.
/// </summary>
internal readonly struct InsertColumn
{
    /// <summary>Initializes a descriptor that writes the caller's values with <paramref name="write"/>.</summary>
    /// <param name="name">The target column name, written to the block header.</param>
    /// <param name="typeName">The target's resolved type string, written to the block header.</param>
    /// <param name="codec">The codec of the target type, resolved from <paramref name="typeName"/>.</param>
    /// <param name="values">The caller-supplied values for this column.</param>
    /// <param name="write">How the body of <paramref name="values"/> is written (<see cref="InsertColumnWrite.For"/>).</param>
    public InsertColumn(string name, string typeName, IColumnCodec codec, IColumn values, InsertColumnWrite write)
    {
        Name = name;
        TypeName = typeName;
        Codec = codec;
        Values = values;
        Write = write;
    }

    /// <summary>The target column name written to the block header.</summary>
    public string Name { get; }

    /// <summary>The target's resolved type string written to the block header.</summary>
    public string TypeName { get; }

    /// <summary>The codec of the target type.</summary>
    public IColumnCodec Codec { get; }

    /// <summary>How the body is written: through the codec, or through the converter tree of the values' CLR type.</summary>
    public InsertColumnWrite Write { get; }

    /// <summary>The caller-supplied values.</summary>
    public IColumn Values { get; }
}
