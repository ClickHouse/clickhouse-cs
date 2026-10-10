using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The row inserts as the differential tests run them: the values of the column of a write facet, as POCO rows
/// (<c>Row&lt;T&gt;.Value</c>) or as untyped rows (one boxed value in each <c>object[]</c>), in an insert whose target is
/// one column called <c>value</c>. The plan of the insert is made when the arm binds (a refusal), and the writer gathers
/// the rows of the slice into the column and writes the column of the block, as the insert does (a failure).
/// </summary>
internal static class RowWriteArms
{
    /// <summary>The server's sample block of an insert into one column called <c>value</c>.</summary>
    /// <param name="columnType">The type of the column.</param>
    /// <param name="context">The context of the sample block.</param>
    /// <returns>The block.</returns>
    public static Block Schema(string columnType, ResolveContext context)
        => new(string.Empty, BlockInfo.Default, rowCount: 0, new IColumn[] { new ArrayColumn<object>("value", columnType, Array.Empty<object>()) }, ColumnCodecRegistry.Default, context);

    /// <summary>The write of the gathered column of the insert plan today: through the converter derivation.</summary>
    /// <param name="column">The gathered column.</param>
    /// <param name="columnType">The type of the target.</param>
    /// <param name="context">The context of the sample block.</param>
    /// <returns>The write, or null when the plan refuses the column.</returns>
    public static InsertColumnWrite ClientWrite(IColumn column, string columnType, ResolveContext context)
        => InsertColumnWrite.For(ColumnCodecRegistry.Default.Resolve(columnType, context), column, columnType, context, ColumnCodecRegistry.Default.Converters);

    /// <summary>Calls a generic method of a type argument, with the exception of the method, not a wrapper.</summary>
    /// <param name="method">The generic method definition.</param>
    /// <param name="typeArgument">The type argument.</param>
    /// <param name="arguments">The arguments.</param>
    /// <returns>The result.</returns>
    public static object Invoke(MethodInfo method, Type typeArgument, params object[] arguments)
    {
        try
        {
            return method.MakeGenericMethod(typeArgument).Invoke(null, arguments);
        }
        catch (TargetInvocationException e) when (e.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            throw;
        }
    }

    // Gathers rows [start, start + length) of the insert into the column, then writes rows [0, length) of the column with
    // the write of the plan, as the block writer does. The source and the buffer serve one write.
    private static SliceWriter Gathered(IInsertColumnSource source, IDisposable buffer, InsertColumnWrite write)
        => (writer, start, length) =>
        {
            try
            {
                source.Gather(start, length);
                IColumn gathered = source.Columns[0];
                IColumnWriteState state = write.Begin(gathered, 0, length);
                try
                {
                    write.WritePrefix(writer, gathered, 0, length, state);
                    write.Write(writer, gathered, 0, length, state);
                }
                finally
                {
                    state?.Dispose();
                }
            }
            finally
            {
                source.Dispose();
                buffer.Dispose();
            }
        };

    /// <summary>The POCO insert: a plan of <c>Row&lt;T&gt;</c>, and the write of the gathered column.</summary>
    internal abstract class PocoArm : WriteArm
    {
        protected PocoArm(string name)
            : base(name, Tier.PocoWrite)
        {
        }

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            var rows = new Row<T>[column.RowCount];
            for (int i = 0; i < rows.Length; i++)
            {
                rows[i] = new Row<T> { Value = column[i] };
            }

            PocoWritePlan<Row<T>> plan = Plan<T>(Schema(columnType, context));
            var buffer = PocoRowBuffer<Row<T>>.Create(rows, "rows", rows.Length, CancellationToken.None);
            PocoInsertSource<Row<T>> source = plan.CreateSource(buffer, rows.Length);
            try
            {
                InsertColumnWrite write = Write(source.Columns[0], columnType, context)
                    ?? throw new ArmRefusal($"The insert plan of '{columnType}' refuses the gathered column ({source.Columns[0].GetType().Name}).");
                return Gathered(source, buffer, write);
            }
            catch
            {
                source.Dispose();
                buffer.Dispose();
                throw;
            }
        }

        /// <summary>The write plan for the sample block. Throw to refuse.</summary>
        protected abstract PocoWritePlan<Row<T>> Plan<T>(Block schema);

        /// <summary>The write of the gathered column, or null when the insert plan refuses it.</summary>
        protected abstract InsertColumnWrite Write(IColumn column, string columnType, ResolveContext context);
    }

    /// <summary>The untyped insert: the source of the untyped rows, and the write of the gathered column.</summary>
    internal abstract class UntypedArm : WriteArm
    {
        protected UntypedArm(string name)
            : base(name, Tier.UntypedWrite)
        {
        }

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            var rows = new object[column.RowCount][];
            for (int i = 0; i < rows.Length; i++)
            {
                rows[i] = new[] { column.GetValue(i) };
            }

            var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", rows.Length, CancellationToken.None);
            PocoInsertSource<object[]> source;
            try
            {
                source = CreateSource(Schema(columnType, context), buffer, rows.Length);
            }
            catch
            {
                buffer.Dispose();
                throw;
            }

            try
            {
                InsertColumnWrite write = Write(source.Columns[0], columnType, context)
                    ?? throw new ArmRefusal($"The insert plan of '{columnType}' refuses the gathered column ({source.Columns[0].GetType().Name}).");
                return Gathered(source, buffer, write);
            }
            catch
            {
                source.Dispose();
                buffer.Dispose();
                throw;
            }
        }

        /// <summary>The source of the insert. Throw to refuse.</summary>
        protected abstract PocoInsertSource<object[]> CreateSource(Block schema, PocoRowBuffer<object[]> rows, int blockRows);

        /// <summary>The write of the gathered column, or null when the insert plan refuses it.</summary>
        protected abstract InsertColumnWrite Write(IColumn column, string columnType, ResolveContext context);
    }

    /// <summary>The client's POCO insert: the write plan that the client caches, with the gather tier of the registry.</summary>
    internal sealed class ClientPocoArm : PocoArm
    {
        private readonly PocoTypeRegistry plans;

        public ClientPocoArm(string name, PocoGatherTier? tier)
            : base(name) => plans = new PocoTypeRegistry { ForcedGatherTier = tier };

        protected override PocoWritePlan<Row<T>> Plan<T>(Block schema) => plans.WritePlanFor<Row<T>>(schema);

        protected override InsertColumnWrite Write(IColumn column, string columnType, ResolveContext context) => ClientWrite(column, columnType, context);
    }

    /// <summary>The client's untyped insert.</summary>
    internal sealed class ClientUntypedArm : UntypedArm
    {
        public ClientUntypedArm(string name)
            : base(name)
        {
        }

        protected override PocoInsertSource<object[]> CreateSource(Block schema, PocoRowBuffer<object[]> rows, int blockRows)
            => UntypedRowColumns.CreateSource(schema, rows, blockRows);

        protected override InsertColumnWrite Write(IColumn column, string columnType, ResolveContext context) => ClientWrite(column, columnType, context);
    }

    /// <summary>Whether the client's POCO write plan builds for a <c>Row&lt;T&gt;.Value</c> property of the type.</summary>
    internal static class ClientPocoAnswer
    {
        private static readonly MethodInfo BuildsMethod = typeof(ClientPocoAnswer).GetMethod(nameof(Builds), BindingFlags.NonPublic | BindingFlags.Static);

        private static readonly ConcurrentDictionary<(string, Type), bool> Answers = new();

        public static bool Answer(string columnType, Type elementType)
            => Answers.GetOrAdd((columnType, elementType), key => (bool)Invoke(BuildsMethod, key.Item2, key.Item1));

        private static bool Builds<T>(string columnType)
        {
            try
            {
                PocoWritePlan<Row<T>>.Build(PocoTypeDescriptor<Row<T>>.Build(), Schema(columnType, DifferentialEngine.Context));
                return true;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Whether the client's untyped insert takes values whose CLR type is the type, or the value type under it (a boxed
    /// nullable value is its value type).
    /// </summary>
    internal static class ClientUntypedAnswer
    {
        public static bool Answer(string columnType, Type elementType)
        {
            ResolveContext context = DifferentialEngine.Context;
            Block schema = Schema(columnType, context);
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, context);
            try
            {
                UntypedRowColumns.WriteTypeFor(ColumnCodecRegistry.Default.Converters, in context, codec, schema[0], 0, Nullable.GetUnderlyingType(elementType) ?? elementType);
                return true;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }
    }
}
