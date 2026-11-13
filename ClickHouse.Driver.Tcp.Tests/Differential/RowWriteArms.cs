using System;
using System.Linq;
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
/// The row inserts as the differential tests run them: the values of a column as POCO rows (<c>Row&lt;T&gt;.Value</c>) or
/// as untyped rows (one boxed value in each <c>object[]</c>), in an insert into one column called <c>value</c>.
/// </summary>
internal static class RowWriteArms
{
    /// <summary>The server's sample block of an insert into one column called <c>value</c>.</summary>
    public static Block Schema(string columnType, ResolveContext context)
        => new(string.Empty, BlockInfo.Default, rowCount: 0, new IColumn[] { new ArrayColumn<object>("value", columnType, Array.Empty<object>()) }, ColumnCodecRegistry.Default, context);

    /// <summary>The write that the insert plan gives a column (<see cref="InsertColumnWrite.For"/>), or null when it refuses the column.</summary>
    public static InsertColumnWrite ClientWrite(IColumn column, string columnType, ResolveContext context)
        => InsertColumnWrite.For(ColumnCodecRegistry.Default.Resolve(columnType, context), column, columnType, context, ColumnCodecRegistry.Default.Converters);

    /// <summary>Calls a static generic method of a type argument, and throws the exception of the method, not a wrapper.</summary>
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

    /// <summary>Writes rows [start, start + length) of a column with a write of the insert plan, as the block writer does.</summary>
    public static void WriteSlice(InsertColumnWrite write, IColumn column, ClickHouseBinaryWriter writer, int start, int length)
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
    }

    // Opens the insert of the rows (the plan: a refusal), and gives the writer that gathers rows [start, start + length)
    // into the column and writes the column from row 0, as the insert does for each block (a failure). The source and the
    // buffer serve one write.
    private static SliceWriter Gathered<TRow>(TRow[] rows, string columnType, ResolveContext context, Func<PocoRowBuffer<TRow>, PocoInsertSource<TRow>> open)
        where TRow : class
    {
        var buffer = PocoRowBuffer<TRow>.Create(rows, "rows", rows.Length, CancellationToken.None);
        PocoInsertSource<TRow> source = null;
        try
        {
            source = open(buffer);
            InsertColumnWrite write = ClientWrite(source.Columns[0], columnType, context)
                ?? throw new ArmRefusal($"The insert plan of '{columnType}' refuses the gathered column ({source.Columns[0].GetType().Name}).");
            return (writer, start, length) =>
            {
                try
                {
                    source.Gather(start, length);
                    WriteSlice(write, source.Columns[0], writer, 0, length);
                }
                finally
                {
                    source.Dispose();
                    buffer.Dispose();
                }
            };
        }
        catch
        {
            source?.Dispose();
            buffer.Dispose();
            throw;
        }
    }

    /// <summary>The client's POCO insert: the write plan of <c>Row&lt;T&gt;</c>, with a gather tier, and the write of the gathered column.</summary>
    internal sealed class ClientPocoArm(string name, PocoGatherTier? tier) : WriteArm(name)
    {
        private readonly PocoTypeRegistry plans = new() { ForcedGatherTier = tier };

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            Row<T>[] rows = Enumerable.Range(0, column.RowCount).Select(i => new Row<T> { Value = column[i] }).ToArray();
            return Gathered(rows, columnType, context, buffer => plans.WritePlanFor<Row<T>>(Schema(columnType, context)).CreateSource(buffer, rows.Length));
        }
    }

    /// <summary>The client's untyped insert: one boxed value in each row, and the write of the gathered column.</summary>
    internal sealed class ClientUntypedArm(string name) : WriteArm(name)
    {
        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            object[][] rows = Enumerable.Range(0, column.RowCount).Select(i => new[] { column.GetValue(i) }).ToArray();
            return Gathered(rows, columnType, context, buffer => UntypedRowColumns.CreateSource(Schema(columnType, context), buffer, rows.Length));
        }
    }
}
