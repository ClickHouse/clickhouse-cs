using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// One column of an outgoing INSERT block: the wire header (<see cref="Name"/>, <see cref="TypeName"/>), the
/// <see cref="Codec"/> that serializes the body, and the caller's <see cref="Values"/>. Header and codec come
/// from the server's sample block, not the value column, so the wire always carries the target's name and type.
/// </summary>
internal readonly struct InsertColumn
{
    /// <summary>Initializes a descriptor pairing a target header and codec with the caller's values.</summary>
    /// <param name="name">The target column name, written to the block header.</param>
    /// <param name="typeName">The target's resolved type string, written to the block header.</param>
    /// <param name="codec">The codec that serializes <paramref name="values"/>, resolved from <paramref name="typeName"/>.</param>
    /// <param name="values">The caller-supplied values for this column.</param>
    public InsertColumn(string name, string typeName, IColumnCodec codec, IColumn values)
    {
        Name = name;
        TypeName = typeName;
        Codec = codec;
        Values = values;
    }

    /// <summary>The target column name written to the block header.</summary>
    public string Name { get; }

    /// <summary>The target's resolved type string written to the block header.</summary>
    public string TypeName { get; }

    /// <summary>The codec that serializes the body.</summary>
    public IColumnCodec Codec { get; }

    /// <summary>The caller-supplied values.</summary>
    public IColumn Values { get; }
}
