using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The reference row inserts of the differential tests (SPEC invariant 11): the POCO write plan and the untyped write
/// type choice over the codecs' preferred write types, with the gathered columns written through their codecs. The POCO
/// write plan is the internal <see cref="PocoWritePlan{T}.BuildLegacy"/> (more than 100 lines:
/// <see cref="LegacyPocoColumnBuilderFactory"/> and <see cref="LegacyPocoWriteConversion"/>); the untyped write type
/// choice is copied here.
/// </summary>
internal static class LegacyRowWrite
{
    /// <summary>The old insert plan of a gathered column: the codec's <c>CanWrite</c>, then the codec's write.</summary>
    /// <param name="column">The gathered column.</param>
    /// <param name="columnType">The type of the target.</param>
    /// <param name="context">The context of the sample block.</param>
    /// <returns>The write, or null when the codec refuses the column.</returns>
    public static InsertColumnWrite CodecWrite(IColumn column, string columnType, ResolveContext context)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, context);
        return codec.CanWrite(column) ? InsertColumnWrite.ThroughCodec(codec) : null;
    }

    /// <summary>
    /// The old choice of the write type of an untyped column: the first of the codec's preferred write types that the
    /// type of the first value that is not null is assignable to, with a boxed nullable value as its value type.
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="target">The target column.</param>
    /// <param name="rows">The insert's rows.</param>
    /// <param name="index">The column's position in every row.</param>
    /// <returns>The write type.</returns>
    public static Type ChooseWriteType(IColumnCodec codec, IColumn target, PocoRowBuffer<object[]> rows, int index)
    {
        if (PocoWriteConversion.AcceptedWriteTypes(codec).Count == 0)
        {
            throw PocoWriteErrors.NotBuildableFromRows(target);
        }

        return WriteTypeFor(codec, target, index, UntypedRowColumns.FirstValueType(rows, index));
    }

    /// <summary>The old choice of the write type of an untyped column for values of <paramref name="present"/>.</summary>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="target">The target column.</param>
    /// <param name="index">The column's position in every row.</param>
    /// <param name="present">The CLR type of the first value that is not null, or null when every value is null.</param>
    /// <returns>The write type.</returns>
    public static Type WriteTypeFor(IColumnCodec codec, IColumn target, int index, Type present)
    {
        IReadOnlyList<Type> accepted = PocoWriteConversion.AcceptedWriteTypes(codec);
        if (accepted.Count == 0)
        {
            throw PocoWriteErrors.NotBuildableFromRows(target);
        }

        if (present is null)
        {
            return accepted[0];
        }

        for (int i = 0; i < accepted.Count; i++)
        {
            // Nullable<T> boxes as T, so compare the underlying type.
            if ((Nullable.GetUnderlyingType(accepted[i]) ?? accepted[i]).IsAssignableFrom(present))
            {
                return accepted[i];
            }
        }

        throw PocoWriteErrors.ValuesNotWritable(index, target, codec, present);
    }

    /// <summary>The old POCO insert.</summary>
    internal sealed class PocoArm : RowWriteArms.PocoArm
    {
        public PocoArm(string name)
            : base(name)
        {
        }

        protected override PocoWritePlan<Row<T>> Plan<T>(Block schema) => Plans<T>.For(schema);

        protected override InsertColumnWrite Write(IColumn column, string columnType, ResolveContext context) => CodecWrite(column, columnType, context);
    }

    /// <summary>The old untyped insert.</summary>
    internal sealed class UntypedArm : RowWriteArms.UntypedArm
    {
        public UntypedArm(string name)
            : base(name)
        {
        }

        protected override PocoInsertSource<object[]> CreateSource(Block schema, PocoRowBuffer<object[]> rows, int blockRows)
            => UntypedRowColumns.CreateSource(schema, rows, blockRows, ChooseWriteType);

        protected override InsertColumnWrite Write(IColumn column, string columnType, ResolveContext context) => CodecWrite(column, columnType, context);
    }

    /// <summary>Whether the old POCO write plan builds for a <c>Row&lt;T&gt;.Value</c> property of the type.</summary>
    internal static class PocoAnswer
    {
        private static readonly MethodInfo BuildsMethod = typeof(PocoAnswer).GetMethod(nameof(Builds), BindingFlags.NonPublic | BindingFlags.Static);

        public static bool Answer(string columnType, Type elementType) => (bool)RowWriteArms.Invoke(BuildsMethod, elementType, columnType);

        private static bool Builds<T>(string columnType)
        {
            try
            {
                Plans<T>.For(RowWriteArms.Schema(columnType, DifferentialEngine.Context));
                return true;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }
    }

    /// <summary>Whether the old untyped insert takes values whose CLR type is the type, or the value type under it.</summary>
    internal static class UntypedAnswer
    {
        public static bool Answer(string columnType, Type elementType)
        {
            Block schema = RowWriteArms.Schema(columnType, DifferentialEngine.Context);
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, DifferentialEngine.Context);
            try
            {
                WriteTypeFor(codec, schema[0], 0, Nullable.GetUnderlyingType(elementType) ?? elementType);
                return true;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }
    }

    // The old plans, cached by column type and context, as the client caches its plans. A build that fails is not kept.
    private static class Plans<T>
    {
        private static readonly ConcurrentDictionary<string, PocoWritePlan<Row<T>>> Cache = new(StringComparer.Ordinal);

        public static PocoWritePlan<Row<T>> For(Block schema)
            => Cache.GetOrAdd(PocoWritePlan.SignatureOf(schema), _ => PocoWritePlan<Row<T>>.BuildLegacy(PocoTypeDescriptor<Row<T>>.Build(), schema));
    }
}
