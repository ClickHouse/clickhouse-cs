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
using System.Linq.Expressions;
using System.Text;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace WireConverterSpike;

/// <summary>A converter tree for one (ClickHouse type, CLR type) pair. Stateless; bind it to a column per block.</summary>
internal abstract class ColumnReader<T>
{
    public abstract BoundReader<T> Bind(IColumn column);

    /// <summary>
    /// Emits the per-block setup into <paramref name="scope"/> (casts, span locals, a converted dictionary) and
    /// returns an expression of type <typeparamref name="T"/> for the value at <paramref name="row"/>. The POCO
    /// scatter splices this into its loop, so the whole tree compiles into one loop per column.
    /// </summary>
    public abstract Expression Emit(Expression column, ParameterExpression row, EmitScope scope);
}

/// <summary>Locals and statements that run once per scatter call, before the loop.</summary>
internal sealed class EmitScope
{
    public List<ParameterExpression> Locals { get; } = new();

    public List<Expression> Prologue { get; } = new();

    public ParameterExpression Local(Type type, string name, Expression init)
    {
        ParameterExpression local = Expression.Variable(type, name + Locals.Count);
        Locals.Add(local);
        Prologue.Add(Expression.Assign(local, init));
        return local;
    }

    public static Expression At<TElement>(Expression span, Expression index)
        => Expression.Call(typeof(EmitScope).GetMethod(nameof(SpanAt)).MakeGenericMethod(typeof(TElement)), span, index);

    public static TElement SpanAt<TElement>(ReadOnlySpan<TElement> span, int index) => span[index];
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

    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression conv = scope.Local(typeof(TConv), "conv", Expression.Invoke(Expression.Constant(make), column));
        ParameterExpression values = scope.Local(
            typeof(ReadOnlySpan<TCanon>), "values", Expression.Property(Expression.Convert(column, typeof(IColumn<TCanon>)), "Values"));
        return Expression.Call(conv, typeof(TConv).GetMethod(nameof(IFixedLeaf<TCanon, T>.Read)), EmitScope.At<TCanon>(values, row));
    }

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

    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression text = scope.Local(typeof(IStringColumn), "text", Expression.Convert(column, typeof(IStringColumn)));
        ParameterExpression blob = scope.Local(typeof(ReadOnlySpan<byte>), "blob", Expression.Property(text, nameof(IStringColumn.Bytes)));
        ParameterExpression offsets = scope.Local(typeof(ReadOnlySpan<int>), "offsets", Expression.Property(text, nameof(IStringColumn.Offsets)));
        return Expression.Call(typeof(BytesLeafReader<T, TConv>).GetMethod(nameof(ReadAt)), blob, offsets, row);
    }

    public static T ReadAt(ReadOnlySpan<byte> blob, ReadOnlySpan<int> offsets, int row)
        => default(TConv).Read(blob[offsets[row]..offsets[row + 1]]);

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

    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => LiftEmit.Emit(inner, column, row, scope, typeof(T));

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

    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
        => LiftEmit.Emit(inner, column, row, scope, typeof(T?));

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

    // Per row: allocate the row's array and fill it with one bulk call into the bound child.
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression array = scope.Local(typeof(IArrayColumn), "array", Expression.Convert(column, typeof(IArrayColumn)));
        ParameterExpression child = scope.Local(
            typeof(BoundReader<T>), "child", Expression.Call(Expression.Constant(inner), nameof(Bind), null, Expression.Property(array, nameof(IArrayColumn.Inner))));
        ParameterExpression offsets = scope.Local(typeof(ReadOnlySpan<int>), "offsets", Expression.Property(array, nameof(IArrayColumn.Offsets)));
        return Expression.Call(typeof(Each<T>).GetMethod(nameof(RowAt)), child, offsets, row);
    }

    public static T[] RowAt(BoundReader<T> child, ReadOnlySpan<int> offsets, int row)
    {
        int from = offsets[row];
        int length = offsets[row + 1] - from;
        if (length == 0)
        {
            return Array.Empty<T>();
        }

        var values = new T[length];
        child.Fill(from, values);
        return values;
    }

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

    public override BoundReader<T> Bind(IColumn column) => new Bound((ILowCardinalityColumn)column, Entries(column));

    // The converted dictionary is cached by column identity, so windows over one block convert it once.
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression entries = scope.Local(typeof(T[]), "entries", Expression.Call(Expression.Constant(new EntryCache(this)), nameof(EntryCache.For), null, column));
        ParameterExpression keys = scope.Local(
            typeof(ReadOnlySpan<int>), "keys", Expression.Property(Expression.Convert(column, typeof(ILowCardinalityColumn)), nameof(ILowCardinalityColumn.Keys)));
        return Expression.ArrayIndex(entries, EmitScope.At<int>(keys, row));
    }

    private T[] Entries(IColumn column)
    {
        var lc = (ILowCardinalityColumn)column;
        IColumn dictionary = lc.Dictionary;
        var entries = new T[dictionary.RowCount];
        inner.Bind(dictionary).Fill(0, entries);
        if (lc.ReservedSlotCount == 2)
        {
            entries[0] = default;
        }

        return entries;
    }

    private sealed class EntryCache
    {
        private readonly DictionaryReader<T> owner;
        private (IColumn Column, T[] Entries) last;

        public EntryCache(DictionaryReader<T> owner) => this.owner = owner;

        public T[] For(IColumn column)
        {
            (IColumn Column, T[] Entries) current = last;
            if (ReferenceEquals(current.Column, column))
            {
                return current.Entries;
            }

            T[] entries = owner.Entries(column);
            last = (column, entries);
            return entries;
        }
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

/// <summary>Experiment: the DateTime leaf emits a direct static call with the time zone as a constant.</summary>
internal sealed class DirectDateTimeReader : ColumnReader<DateTimeOffset>
{
    private readonly ColumnReader<DateTimeOffset> bulk;
    private readonly ResolvedTimeZone zone;

    public DirectDateTimeReader(ColumnReader<DateTimeOffset> bulk, ResolvedTimeZone zone)
    {
        this.bulk = bulk;
        this.zone = zone;
    }

    public override BoundReader<DateTimeOffset> Bind(IColumn column) => bulk.Bind(column);

    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression values = scope.Local(
            typeof(ReadOnlySpan<uint>), "values", Expression.Property(Expression.Convert(column, typeof(IColumn<uint>)), "Values"));
        return Expression.Call(
            typeof(ColumnValueProjections).GetMethod(nameof(ColumnValueProjections.DateTimeToOffset)),
            EmitScope.At<uint>(values, row),
            Expression.Constant(zone));
    }
}

/// <summary>Experiment: the String leaf emits the column's own indexer, as the current scatter does.</summary>
internal sealed class DirectStringReader : ColumnReader<string>
{
    private readonly ColumnReader<string> bulk;

    public DirectStringReader(ColumnReader<string> bulk) => this.bulk = bulk;

    public override BoundReader<string> Bind(IColumn column) => bulk.Bind(column);

    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression typed = scope.Local(typeof(IColumn<string>), "typed", Expression.Convert(column, typeof(IColumn<string>)));
        return Expression.MakeIndex(typed, typeof(IColumn<string>).GetProperty("Item"), new Expression[] { row });
    }
}

/// <summary>
/// The Nullable emit for both lifts. The conditional converts only the non-null rows, so a leaf converter
/// never sees the placeholder here, unlike the bulk <c>Fill</c> path.
/// </summary>
internal static class LiftEmit
{
    public static Expression Emit<TInner>(ColumnReader<TInner> inner, Expression column, ParameterExpression row, EmitScope scope, Type surface)
    {
        ParameterExpression nullable = scope.Local(typeof(INullableColumn), "nullable", Expression.Convert(column, typeof(INullableColumn)));
        ParameterExpression nulls = scope.Local(typeof(ReadOnlySpan<byte>), "nulls", Expression.Property(nullable, nameof(INullableColumn.NullMap)));
        ParameterExpression innerColumn = scope.Local(typeof(IColumn), "innerColumn", Expression.Property(nullable, nameof(INullableColumn.Inner)));
        Expression value = inner.Emit(innerColumn, row, scope);
        return Expression.Condition(
            Expression.NotEqual(EmitScope.At<byte>(nulls, row), Expression.Constant((byte)0)),
            Expression.Default(surface),
            value.Type == surface ? value : Expression.Convert(value, surface));
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

    // Experiment switch: leaves emit the same direct calls as the current scatter instead of the struct-converter
    // helpers. Bind (the bulk path) is the same either way.
    private static readonly bool DirectLeaves = Environment.GetEnvironmentVariable("SPIKE_LEAF") == "direct";

    // (leaf name, target) -> factory. Adding a leaf conversion is one line here, and nothing else.
    private static readonly Dictionary<(string, Type), Func<TypeNode, ResolveContext, object>> Leaves = new()
    {
        [("DateTime", typeof(uint))] = static (_, _) => new FixedLeafReader<uint, uint, Identity<uint>>(static _ => default),
        [("DateTime", typeof(DateTimeOffset))] = static (node, ctx) =>
        {
            string tz = node.Arguments.Count > 0 ? DateTimeZones.UnquoteTimezone(node.Arguments[0]) : null;
            ResolvedTimeZone zone = DateTimeZones.Resolve(tz, ctx.ServerTimezone);
            var reader = new FixedLeafReader<uint, DateTimeOffset, DateTimeToOffset>(_ => new DateTimeToOffset(zone));
            return DirectLeaves ? new DirectDateTimeReader(reader, zone) : reader;
        },
        [("String", typeof(string))] = static (_, _) => DirectLeaves
            ? new DirectStringReader(new BytesLeafReader<string, BytesToString>())
            : new BytesLeafReader<string, BytesToString>(),
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
