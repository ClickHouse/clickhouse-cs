using System;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The client's entry points, one arm for each tier. <see cref="DifferentialRegistry.WithClientArms"/> makes them the
/// first candidates, so they are the baseline of every facet.
/// </summary>
internal static class ClientArms
{
    /// <summary><c>Block.ReadAs&lt;T&gt;</c>, read through <c>Values</c>. The indexer must give the same values.</summary>
    public static readonly ReadArm ReadAs = new ReadAsArm("Client.ReadAs");

    /// <summary>
    /// The POCO read plan, as <c>QueryAsync&lt;T&gt;</c> uses it, into <c>Row&lt;T&gt;.Value</c>. The runtime chooses the
    /// scatter tier, which is <see cref="PocoScatterTier.Emit"/> wherever the tests run.
    /// </summary>
    public static readonly ReadArm Poco = new PocoArm("Client.Poco", tier: null);

    /// <summary><c>ClickHouseTcpTypes.CanRead</c>.</summary>
    public static readonly AnswerArm CanRead = new FunctionAnswerArm("Client.CanRead", Tier.CanRead, ClickHouseTcpTypes.CanRead);

    /// <summary>
    /// The write of one column of the insert plan (<see cref="InsertColumnWrite.For"/>), then its <c>Begin</c>, state prefix
    /// and body for the rows, as the block writer runs them.
    /// </summary>
    public static readonly WriteArm Write = new InsertWriteArm("Client.Write");

    /// <summary><c>ClickHouseTcpTypes.CanWrite</c>.</summary>
    public static readonly AnswerArm CanWrite = new FunctionAnswerArm("Client.CanWrite", Tier.CanWrite, ClickHouseTcpTypes.CanWrite);

    /// <summary>
    /// The POCO write plan of <c>InsertRowsAsync&lt;T&gt;</c>, with the gather tier that the runtime chooses, which is
    /// <see cref="PocoGatherTier.Compiled"/> wherever the tests run, then the insert plan's write of the gathered column.
    /// </summary>
    public static readonly WriteArm PocoWrite = new RowWriteArms.ClientPocoArm("Client.PocoWrite", tier: null);

    /// <summary>Whether the POCO write plan of <c>InsertRowsAsync&lt;T&gt;</c> builds for a property of the type.</summary>
    public static readonly AnswerArm PocoCanWrite = new FunctionAnswerArm("Client.PocoCanWrite", Tier.PocoCanWrite, RowWriteArms.ClientPocoAnswer.Answer);

    /// <summary>The untyped row insert of <c>InsertRowsAsync(object[])</c>, then the insert plan's write of the gathered column.</summary>
    public static readonly WriteArm UntypedWrite = new RowWriteArms.ClientUntypedArm("Client.UntypedWrite");

    /// <summary>Whether the untyped row insert of <c>InsertRowsAsync(object[])</c> takes values of the type.</summary>
    public static readonly AnswerArm UntypedCanWrite = new FunctionAnswerArm("Client.UntypedCanWrite", Tier.UntypedCanWrite, RowWriteArms.ClientUntypedAnswer.Answer);

    /// <summary><c>Block.ReadAs&lt;T&gt;</c> of a block's only column; a subclass can read the column another way.</summary>
    internal class ReadAsArm : ReadArm
    {
        public ReadAsArm(string name)
            : base(name, Tier.ReadAs)
        {
        }

        public override RowReader<T> Bind<T>(Block block)
        {
            IColumn<T> view = View<T>(block);
            return (start, count) =>
            {
                T[] values = view.Values.Slice(start, count).ToArray();
                for (int i = 0; i < count; i++)
                {
                    string difference = ValueComparer.Difference(values[i], view[start + i]);
                    if (difference is not null)
                    {
                        throw new ArmInvariantException($"Row {start + i}: the indexer gives {difference} from Values.");
                    }
                }

                return values;
            };
        }

        /// <summary>The column read as <typeparamref name="T"/>.</summary>
        protected virtual IColumn<T> View<T>(Block block) => block.ReadAs<T>(0);
    }

    /// <summary>The POCO read plan of the client, with the scatter tier that the runtime chooses or a forced one.</summary>
    internal sealed class PocoArm : ReadArm
    {
        // Plans are cached by POCO type, block shape and tier, as the client caches them.
        private static readonly PocoTypeRegistry Plans = new();

        private readonly PocoScatterTier? tier;

        public PocoArm(string name, PocoScatterTier? tier)
            : base(name, Tier.Poco) => this.tier = tier;

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

    private sealed class InsertWriteArm : WriteArm
    {
        public InsertWriteArm(string name)
            : base(name)
        {
        }

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, context);
            InsertColumnWrite write = InsertColumnWrite.For(codec, column, columnType, context, ColumnCodecRegistry.Default.Converters)
                ?? throw new ArmRefusal($"The insert plan of '{columnType}' refuses a column of {TypeNames.Of(typeof(T))} ({column.GetType().Name}).");

            return (writer, start, length) =>
            {
                IColumnWriteState state = write.Begin(column, start, length);
                try
                {
                    write.WritePrefix(writer, column, start, length, state);
                    write.Write(writer, column, start, length, state);
                }
                finally
                {
                    state?.Dispose();
                }
            };
        }
    }

    internal sealed class FunctionAnswerArm : AnswerArm
    {
        private readonly Func<string, Type, bool> answer;

        public FunctionAnswerArm(string name, Tier tier, Func<string, Type, bool> answer)
            : base(name, tier) => this.answer = answer;

        public override bool Answer(string columnType, Type elementType) => answer(columnType, elementType);
    }
}
