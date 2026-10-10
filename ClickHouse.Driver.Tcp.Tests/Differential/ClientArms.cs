using System;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The old path: the arm of each tier that the candidates are compared with. <see cref="ReadAs"/> and
/// <see cref="CanRead"/> run the old dispatch of the columnar read tier (<see cref="LegacyColumnarRead"/>),
/// <see cref="Poco"/> runs the old POCO read plan (<see cref="LegacyPocoRead"/>), <see cref="Write"/> and
/// <see cref="CanWrite"/> run the old dispatch of the columnar write tier (<see cref="LegacyColumnarWrite"/>), and the
/// row insert tiers run the old POCO write plan and the old untyped write type choice (<see cref="LegacyRowWrite"/>),
/// because the client's entry points read and write through the converter derivation.
/// </summary>
/// <remarks>
/// The old members that the converter layer replaces stay in production until the old path is removed. When a tier
/// of the client moves onto the derivation, copy the old dispatch of that tier (the lines that call the old members,
/// for example the hook order of <c>ColumnProjection.For</c>) into an arm in this test project, and point the tier's
/// member here at it. If the old tier has more than about 100 lines, keep it in production as an internal member
/// whose name marks it as the reference path (for example <c>LegacyColumnProjection</c>), and call that member from
/// the arm. Then register the client's entry point (<see cref="ClientArms"/>) as a candidate. The reference outcome
/// counts in <c>DifferentialTests</c> must stay the same.
/// </remarks>
internal static class ReferenceArms
{
    public static ReadArm ReadAs { get; } = new LegacyColumnarRead.ReadAsArm("Old path: ReadAs");

    public static ReadArm Poco { get; } = new LegacyPocoRead.PocoArm("Old path: Poco");

    public static AnswerArm CanRead { get; } = new ClientArms.FunctionAnswerArm("Old path: CanRead", Tier.CanRead, LegacyColumnarRead.CanRead);

    public static WriteArm Write { get; } = new LegacyColumnarWrite.WriteArm("Old path: Write");

    public static AnswerArm CanWrite { get; } = new ClientArms.FunctionAnswerArm("Old path: CanWrite", Tier.CanWrite, LegacyColumnarWrite.CanWrite);

    public static WriteArm PocoWrite { get; } = new LegacyRowWrite.PocoArm("Old path: PocoWrite");

    public static AnswerArm PocoCanWrite { get; } = new ClientArms.FunctionAnswerArm("Old path: PocoCanWrite", Tier.PocoCanWrite, LegacyRowWrite.PocoAnswer.Answer);

    public static WriteArm UntypedWrite { get; } = new LegacyRowWrite.UntypedArm("Old path: UntypedWrite");

    public static AnswerArm UntypedCanWrite { get; } = new ClientArms.FunctionAnswerArm("Old path: UntypedCanWrite", Tier.UntypedCanWrite, LegacyRowWrite.UntypedAnswer.Answer);
}

/// <summary>The client's entry points, one arm for each tier.</summary>
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
