using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Resolves a ClickHouse type string to the codec that reads/writes it. The type string is parsed into a
/// <see cref="TypeNode"/> and dispatched on its base name. This slice registers the fixed-width integers,
/// <c>String</c>, and minimal <c>DateTime</c> / <c>Enum8</c> / <c>Enum16</c> aliases — the latter three are
/// needed to consume the server's always-present ProfileEvents block, whose columns include a <c>DateTime</c>
/// and an <c>Enum8</c>. The enum aliases surface the raw underlying integer; label mapping is a later type.
/// Parameterized dispatch that consumes <see cref="TypeNode.Arguments"/> is added with composite types.
/// </summary>
internal sealed class ColumnCodecRegistry
{
    /// <summary>The default registry with the built-in codecs.</summary>
    public static readonly ColumnCodecRegistry Default = CreateDefault();

    private readonly Dictionary<string, IColumnCodec> byName;

    private ColumnCodecRegistry(Dictionary<string, IColumnCodec> byName) => this.byName = byName;

    /// <summary>Resolves the codec for a ClickHouse type string.</summary>
    /// <param name="typeString">The type string from a column header (e.g. <c>UInt64</c>, <c>DateTime('UTC')</c>).</param>
    /// <returns>The codec for that type.</returns>
    /// <exception cref="FormatException"><paramref name="typeString"/> is malformed.</exception>
    /// <exception cref="NotSupportedException">The type is well-formed but not yet supported by this client.</exception>
    public IColumnCodec Resolve(string typeString)
    {
        TypeNode node = TypeParser.Parse(typeString);
        if (byName.TryGetValue(node.Name, out IColumnCodec codec))
        {
            return codec;
        }

        throw new NotSupportedException($"ClickHouse type '{typeString}' is not supported by this client yet.");
    }

    private static ColumnCodecRegistry CreateDefault()
    {
        var byName = new Dictionary<string, IColumnCodec>(StringComparer.Ordinal);

        void Add(IColumnCodec codec) => byName[codec.TypeName] = codec;

        Add(new IntegerColumnCodec<byte>("UInt8"));
        Add(new IntegerColumnCodec<sbyte>("Int8"));
        Add(new IntegerColumnCodec<ushort>("UInt16"));
        Add(new IntegerColumnCodec<short>("Int16"));
        Add(new IntegerColumnCodec<uint>("UInt32"));
        Add(new IntegerColumnCodec<int>("Int32"));
        Add(new IntegerColumnCodec<ulong>("UInt64"));
        Add(new IntegerColumnCodec<long>("Int64"));
        Add(new IntegerColumnCodec<UInt128>("UInt128"));
        Add(new IntegerColumnCodec<Int128>("Int128"));
        Add(new IntegerColumnCodec<UInt256>("UInt256"));
        Add(new IntegerColumnCodec<Int256>("Int256"));
        Add(new IntegerColumnCodec<sbyte>("Enum8"));   // underlying Int8; raw ordinal, label mapping deferred
        Add(new IntegerColumnCodec<short>("Enum16"));  // underlying Int16; raw ordinal, label mapping deferred
        Add(StringColumnCodec.Instance);
        Add(DateTimeColumnCodec.Instance);

        return new ColumnCodecRegistry(byName);
    }
}
