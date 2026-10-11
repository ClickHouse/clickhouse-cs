using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>Reads rows <c>[start, start + count)</c> of a bound column.</summary>
internal delegate T[] RowReader<T>(int start, int count);

/// <summary>Writes rows <c>[start, start + length)</c> of a bound column: <c>BeginWrite</c>, the state prefix and the body.</summary>
internal delegate void SliceWriter(ClickHouseBinaryWriter writer, int start, int length);

/// <summary>A read tier: binds to the only column of a block (<c>value</c>). A refusal throws from <see cref="Bind{T}"/>, a failure on a value from the reader.</summary>
internal abstract class ReadArm(string name)
{
    public string Name { get; } = name;

    public abstract RowReader<T> Bind<T>(Block block);

    /// <inheritdoc/>
    public override string ToString() => Name;
}

/// <summary>A write tier: binds to a column written as a type. A refusal throws from <see cref="Bind{T}"/>, a failure on a value from the writer.</summary>
internal abstract class WriteArm(string name)
{
    public string Name { get; } = name;

    public abstract SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context);

    /// <inheritdoc/>
    public override string ToString() => Name;
}

/// <summary>The client's read and columnar write tiers, and the independent write that checks the slicing of the insert.</summary>
internal static class ClientArms
{
    /// <summary><c>Block.ReadAs&lt;T&gt;</c>, read through <c>Values</c>. The indexer must give the same values.</summary>
    public static readonly ReadArm ReadAs = new ReadAsArm();

    /// <summary>The POCO read plan, as <c>QueryAsync&lt;T&gt;</c> uses it (<see cref="PocoScatterTier.Emit"/> wherever the tests run).</summary>
    public static readonly ReadArm Poco = new PocoArm("Client.Poco", tier: null);

    /// <summary>The POCO read plan in the tier of a runtime without dynamic code.</summary>
    public static readonly ReadArm PocoFill = new PocoArm("Client.Poco: Fill", PocoScatterTier.Fill);

    /// <summary>The write of one column of the insert plan (<see cref="InsertColumnWrite.For"/>), as the block writer runs it.</summary>
    public static readonly WriteArm Write = new InsertWriteArm();

    /// <summary>
    /// An independent insert write: a column that the codec writes from its storage goes to the codec, any other through
    /// the derived tree, from a span of the values of the slice that it gathers through the indexer, not through the
    /// source of <see cref="InsertColumnWrite"/> (span, stored values or indexer, start, length, first row). So the two
    /// writes give the same bytes only when the insert slices right. A refusal is an <see cref="ArmRefusal"/>.
    /// </summary>
    public static readonly WriteArm IndexerWrite = new IndexerWriteArm();

    /// <summary>The POCO write plan of <c>InsertRowsAsync&lt;T&gt;</c>, with the gather tier that the runtime chooses.</summary>
    public static readonly WriteArm PocoWrite = new RowWriteArms.ClientPocoArm("Client.PocoWrite", tier: null);

    /// <summary>The POCO write plan with the gather of a runtime without dynamic code.</summary>
    public static readonly WriteArm PocoWriteDelegate = new RowWriteArms.ClientPocoArm("Client.PocoWrite: Delegate", PocoGatherTier.Delegate);

    /// <summary>The untyped row insert of <c>InsertRowsAsync(object[])</c>.</summary>
    public static readonly WriteArm UntypedWrite = new RowWriteArms.ClientUntypedArm("Client.UntypedWrite");

    private sealed class ReadAsArm() : ReadArm("Client.ReadAs")
    {
        public override RowReader<T> Bind<T>(Block block)
        {
            IColumn<T> view = block.ReadAs<T>(0);
            return (start, count) =>
            {
                T[] values = view.Values.Slice(start, count).ToArray();
                for (int i = 0; i < count; i++)
                {
                    if (ValueComparer.Difference(values[i], view[start + i]) is string difference)
                    {
                        throw new ArmInvariantException($"Row {start + i}: the indexer gives {difference} from Values.");
                    }
                }

                return values;
            };
        }
    }

    /// <summary>The POCO read plan of the client, with the scatter tier that the runtime chooses or a forced one.</summary>
    internal sealed class PocoArm(string name, PocoScatterTier? tier) : ReadArm(name)
    {
        // Plans are cached by POCO type, block shape and tier, as the client caches them.
        private static readonly PocoTypeRegistry Plans = new();

        // rows[0] is row `start` of the result, so a failure names the row of the column.
        public override RowReader<T> Bind<T>(Block block)
        {
            PocoReadPlan<Row<T>> plan = Plans.ReadPlanFor<Row<T>>(block, tier);
            return (start, count) =>
            {
                var rows = new Row<T>[count];
                plan.Materialize(block, rows, start, count, rowOffset: start);
                return Array.ConvertAll(rows, row => row.Value);
            };
        }
    }

    private sealed class InsertWriteArm() : WriteArm("Client.Write")
    {
        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            InsertColumnWrite write = RowWriteArms.ClientWrite(column, columnType, context)
                ?? throw new ArmRefusal($"The insert plan of '{columnType}' refuses a column of {TypeNames.Of(typeof(T))} ({column.GetType().Name}).");
            return (writer, start, length) => RowWriteArms.WriteSlice(write, column, writer, start, length);
        }
    }

    private sealed class IndexerWriteArm() : WriteArm("Insert write through the indexer")
    {
        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, context);
            if (codec.CanWrite(column))
            {
                return (writer, start, length) =>
                {
                    IColumnWriteState state = codec.BeginWrite(column, start, length);
                    try
                    {
                        codec.WriteStatePrefix(writer, column, start, length, state);
                        codec.WriteColumn(writer, column, start, length, state);
                    }
                    finally
                    {
                        state?.Dispose();
                    }
                };
            }

            Derivation derivation = ConverterDerivation.Default.Derive(columnType, context, typeof(T), ConversionDirection.Write);
            var tree = derivation.Succeeded ? (ColumnWriter<T>)derivation.Converter : throw new ArmRefusal(derivation.Refusal);
            return (writer, start, length) =>
            {
                T[] values = ArrayPool<T>.Shared.Rent(Math.Max(length, 1));
                try
                {
                    for (int i = 0; i < length; i++)
                    {
                        values[i] = column[start + i];
                    }

                    ConverterHarness.WriteAll(tree, writer, ValueSource<T>.Of(values.AsSpan(0, length), start, column.Name));
                }
                finally
                {
                    ArrayPool<T>.Shared.Return(values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
                }
            };
        }
    }
}
