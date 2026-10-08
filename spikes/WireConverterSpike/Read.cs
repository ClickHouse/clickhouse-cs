// SPIKE for ClickHouse/integrations#801. Not production code.
//
// Read side of the candidate structure. The current codecs already decode to the dense wire layout
// (DateTimeColumn = raw uint seconds, StringColumn = blob + offsets, LowCardinality = dictionary + keys,
// Nullable = null map + inner, Array = offsets + flat child). So this spike keeps the current decoders as
// the "wire layer" and adds only the converter layer on top of their dense surfaces.
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace WireConverterSpike;

/// <summary>A converter tree for one (ClickHouse type, CLR type) pair. Stateless; bind it to a column per block.</summary>
internal abstract class ColumnReader<T>
{
    public abstract BoundReader<T> Bind(IColumn column);
}

/// <summary>A converter bound to one decoded column. Per-block state (a converted dictionary) lives here.</summary>
internal abstract class BoundReader<T>
{
    public abstract void Fill(int start, Span<T> destination);
}

/// <summary>One leaf conversion over a fixed-width canonical value. A struct so the JIT inlines it in the loop.</summary>
internal interface IFixedLeaf<TCanon, T>
{
    T Read(TCanon value);
}

/// <summary>One leaf conversion over a byte-run canonical value (String, FixedString).</summary>
internal interface IBytesLeaf<T>
{
    T Read(ReadOnlySpan<byte> value);
}

// ---------------------------------------------------------------- leaves

internal readonly struct DateTimeToOffset : IFixedLeaf<uint, DateTimeOffset>
{
    private readonly ResolvedTimeZone zone;

    public DateTimeToOffset(ResolvedTimeZone zone) => this.zone = zone;

    public DateTimeOffset Read(uint value) => ColumnValueProjections.DateTimeToOffset(value, zone);
}

internal readonly struct Identity<T> : IFixedLeaf<T, T>
{
    public T Read(T value) => value;
}

internal readonly struct BytesToString : IBytesLeaf<string>
{
    public string Read(ReadOnlySpan<byte> value) => value.IsEmpty ? string.Empty : Encoding.UTF8.GetString(value);
}

internal readonly struct BytesToArray : IBytesLeaf<byte[]>
{
    public byte[] Read(ReadOnlySpan<byte> value) => value.ToArray();
}

/// <summary>A leaf over a fixed-width column. <typeparamref name="TConv"/> is built per column (timezone).</summary>
internal sealed class FixedLeafReader<TCanon, T, TConv> : ColumnReader<T>
    where TConv : struct, IFixedLeaf<TCanon, T>
{
    private readonly Func<IColumn, TConv> make;

    public FixedLeafReader(Func<IColumn, TConv> make) => this.make = make;

    public override BoundReader<T> Bind(IColumn column) => new Bound((IColumn<TCanon>)column, make(column));

    private sealed class Bound : BoundReader<T>
    {
        private readonly IColumn<TCanon> column;
        private readonly TConv conv;

        public Bound(IColumn<TCanon> column, TConv conv)
        {
            this.column = column;
            this.conv = conv;
        }

        public override void Fill(int start, Span<T> destination)
        {
            ReadOnlySpan<TCanon> source = column.Values.Slice(start, destination.Length);
            for (int i = 0; i < source.Length; i++)
            {
                destination[i] = conv.Read(source[i]);
            }
        }
    }
}

/// <summary>A leaf over a <c>String</c> column's blob and offsets.</summary>
internal sealed class BytesLeafReader<T, TConv> : ColumnReader<T>
    where TConv : struct, IBytesLeaf<T>
{
    public override BoundReader<T> Bind(IColumn column) => new Bound((IStringColumn)column);

    private sealed class Bound : BoundReader<T>
    {
        private readonly IStringColumn column;

        public Bound(IStringColumn column) => this.column = column;

        public override void Fill(int start, Span<T> destination)
        {
            ReadOnlySpan<byte> blob = column.Bytes;
            ReadOnlySpan<int> offsets = column.Offsets;
            TConv conv = default;
            for (int i = 0; i < destination.Length; i++)
            {
                int from = offsets[start + i];
                destination[i] = conv.Read(blob[from..offsets[start + i + 1]]);
            }
        }
    }
}

// ---------------------------------------------------------------- combinators

/// <summary><c>Nullable(X)</c> read as a reference type: fill from the inner reader, then clear the null rows.</summary>
internal sealed class LiftRef<T> : ColumnReader<T>
    where T : class
{
    private readonly ColumnReader<T> inner;

    public LiftRef(ColumnReader<T> inner) => this.inner = inner;

    public override BoundReader<T> Bind(IColumn column)
    {
        var nullable = (INullableColumn)column;
        return new Bound(nullable, inner.Bind(nullable.Inner));
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly INullableColumn column;
        private readonly BoundReader<T> inner;

        public Bound(INullableColumn column, BoundReader<T> inner)
        {
            this.column = column;
            this.inner = inner;
        }

        public override void Fill(int start, Span<T> destination)
        {
            // Converts the placeholder too, then drops it. A per-row skip would need a second hook.
            inner.Fill(start, destination);
            ReadOnlySpan<byte> nulls = column.NullMap.Slice(start, destination.Length);
            for (int i = 0; i < nulls.Length; i++)
            {
                if (nulls[i] != 0)
                {
                    destination[i] = null;
                }
            }
        }
    }
}

/// <summary>
/// <c>Nullable(X)</c> read as <c>T?</c>. <c>Span&lt;T?&gt;</c> cannot be passed as <c>Span&lt;T&gt;</c>, so the
/// value-type lift needs a scratch buffer. This is the one place the value/reference doubling survives.
/// </summary>
internal sealed class LiftValue<T> : ColumnReader<T?>
    where T : struct
{
    private readonly ColumnReader<T> inner;

    public LiftValue(ColumnReader<T> inner) => this.inner = inner;

    public override BoundReader<T?> Bind(IColumn column)
    {
        var nullable = (INullableColumn)column;
        return new Bound(nullable, inner.Bind(nullable.Inner));
    }

    private sealed class Bound : BoundReader<T?>
    {
        private readonly INullableColumn column;
        private readonly BoundReader<T> inner;

        public Bound(INullableColumn column, BoundReader<T> inner)
        {
            this.column = column;
            this.inner = inner;
        }

        public override void Fill(int start, Span<T?> destination)
        {
            T[] scratch = ArrayPool<T>.Shared.Rent(destination.Length);
            try
            {
                Span<T> values = scratch.AsSpan(0, destination.Length);
                inner.Fill(start, values);
                ReadOnlySpan<byte> nulls = column.NullMap.Slice(start, destination.Length);
                for (int i = 0; i < values.Length; i++)
                {
                    destination[i] = nulls[i] != 0 ? null : values[i];
                }
            }
            finally
            {
                ArrayPool<T>.Shared.Return(scratch);
            }
        }
    }
}

/// <summary><c>Array(X)</c> read as <c>T[]</c>: one bulk inner fill per row, straight into the row's array.</summary>
internal sealed class Each<T> : ColumnReader<T[]>
{
    private readonly ColumnReader<T> inner;

    public Each(ColumnReader<T> inner) => this.inner = inner;

    public override BoundReader<T[]> Bind(IColumn column)
    {
        var array = (IArrayColumn)column;
        return new Bound(array, inner.Bind(array.Inner));
    }

    private sealed class Bound : BoundReader<T[]>
    {
        private readonly IArrayColumn column;
        private readonly BoundReader<T> inner;

        public Bound(IArrayColumn column, BoundReader<T> inner)
        {
            this.column = column;
            this.inner = inner;
        }

        public override void Fill(int start, Span<T[]> destination)
        {
            ReadOnlySpan<int> offsets = column.Offsets;
            for (int i = 0; i < destination.Length; i++)
            {
                int from = offsets[start + i];
                int length = offsets[start + i + 1] - from;
                if (length == 0)
                {
                    destination[i] = Array.Empty<T>();
                    continue;
                }

                var row = new T[length];
                inner.Fill(from, row);
                destination[i] = row;
            }
        }
    }
}

/// <summary>
/// <c>LowCardinality(X)</c>: converts each dictionary entry once per block (at bind), then reads rows by key.
/// A null-carrying dictionary (<c>LowCardinality(Nullable(X))</c>) maps key 0 to <c>default</c>.
/// </summary>
internal sealed class DictionaryReader<T> : ColumnReader<T>
{
    private readonly ColumnReader<T> inner;

    public DictionaryReader(ColumnReader<T> inner) => this.inner = inner;

    public override BoundReader<T> Bind(IColumn column)
    {
        var lc = (ILowCardinalityColumn)column;
        IColumn dictionary = lc.Dictionary;
        var entries = new T[dictionary.RowCount];
        inner.Bind(dictionary).Fill(0, entries);
        if (lc.ReservedSlotCount == 2)
        {
            entries[0] = default;
        }

        return new Bound(lc, entries);
    }

    private sealed class Bound : BoundReader<T>
    {
        private readonly ILowCardinalityColumn column;
        private readonly T[] entries;

        public Bound(ILowCardinalityColumn column, T[] entries)
        {
            this.column = column;
            this.entries = entries;
        }

        public override void Fill(int start, Span<T> destination)
        {
            ReadOnlySpan<int> keys = column.Keys.Slice(start, destination.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                destination[i] = entries[keys[i]];
            }
        }
    }
}

// ---------------------------------------------------------------- derivation

/// <summary>A derived converter tree, or the reason there is none.</summary>
internal sealed record Derivation(object Reader, string Refusal)
{
    public bool Succeeded => Reader is not null;
}

/// <summary>
/// <c>Derive(type, T)</c> for every tier. The leaf table is the only place that knows a (leaf, CLR type) pair;
/// composites only recurse. Partial: DateTime, String, Nullable, Array, LowCardinality.
/// </summary>
internal static class ReadDerivation
{
    private static readonly ConcurrentDictionary<(string, string, Type), Derivation> Cache = new();

    // (leaf name, target) -> factory. Adding a leaf conversion is one line here, and nothing else.
    private static readonly Dictionary<(string, Type), Func<TypeNode, ResolveContext, object>> Leaves = new()
    {
        [("DateTime", typeof(uint))] = static (_, _) => new FixedLeafReader<uint, uint, Identity<uint>>(static _ => default),
        [("DateTime", typeof(DateTimeOffset))] = static (node, ctx) =>
        {
            string tz = node.Arguments.Count > 0 ? DateTimeZones.UnquoteTimezone(node.Arguments[0]) : null;
            ResolvedTimeZone zone = DateTimeZones.Resolve(tz, ctx.ServerTimezone);
            return new FixedLeafReader<uint, DateTimeOffset, DateTimeToOffset>(_ => new DateTimeToOffset(zone));
        },
        [("String", typeof(string))] = static (_, _) => new BytesLeafReader<string, BytesToString>(),
        [("String", typeof(byte[]))] = static (_, _) => new BytesLeafReader<byte[], BytesToArray>(),
    };

    public static Derivation Derive(string type, in ResolveContext context, Type target)
    {
        ResolveContext ctx = context;
        return Cache.GetOrAdd((type, context.ServerTimezone, target), key => Derive(TypeParser.Parse(key.Item1), ctx, key.Item3));
    }

    public static ColumnReader<T> Derive<T>(string type, in ResolveContext context)
    {
        Derivation derivation = Derive(type, context, typeof(T));
        return derivation.Succeeded
            ? (ColumnReader<T>)derivation.Reader
            : throw new InvalidCastException(derivation.Refusal);
    }

    private static Derivation Derive(TypeNode node, ResolveContext context, Type target)
    {
        switch (node.Name)
        {
            case "Nullable":
            {
                Type underlying = Nullable.GetUnderlyingType(target);
                if (underlying is not null)
                {
                    return Wrap(typeof(LiftValue<>), underlying, Derive(node.Arguments[0], context, underlying));
                }

                if (target.IsValueType)
                {
                    // The opt-in "nullable into non-nullable, throw on null" rule would be a separate combinator.
                    return Refuse($"'{node}' can hold NULL, and {target} cannot.");
                }

                return Wrap(typeof(LiftRef<>), target, Derive(node.Arguments[0], context, target));
            }

            case "Array":
            {
                if (!target.IsArray || target.GetArrayRank() != 1)
                {
                    return Refuse($"'{node}' reads as an array, not as {target}.");
                }

                Type element = target.GetElementType();
                return Wrap(typeof(Each<>), element, Derive(node.Arguments[0], context, element));
            }

            case "LowCardinality":
            {
                TypeNode innerNode = node.Arguments[0];
                if (innerNode.Name == "Nullable")
                {
                    // The dictionary of LowCardinality(Nullable(X)) is a plain X column, so derive X's reader and
                    // let DictionaryReader put the null in slot 0. Value types would need a lifting dictionary.
                    if (target.IsValueType)
                    {
                        return Refuse($"spike: '{node}' as a value type is not implemented.");
                    }

                    innerNode = innerNode.Arguments[0];
                }

                return Wrap(typeof(DictionaryReader<>), target, Derive(innerNode, context, target));
            }

            default:
                return Leaves.TryGetValue((node.Name, target), out var make)
                    ? new Derivation(make(node, context), null)
                    : Refuse($"'{node}' offers no reading as {target}.");
        }
    }

    // MakeGenericType is the one place that needs dynamic code. See the note on AOT in NOTES.md.
    private static Derivation Wrap(Type combinator, Type argument, Derivation inner)
        => inner.Succeeded
            ? new Derivation(Activator.CreateInstance(combinator.MakeGenericType(argument), inner.Reader), null)
            : inner;

    private static Derivation Refuse(string reason) => new(null, reason);
}
