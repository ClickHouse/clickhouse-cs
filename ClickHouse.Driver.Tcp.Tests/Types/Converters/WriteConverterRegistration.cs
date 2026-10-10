using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Runs the write combinators in the differential tests, for every case whose column type is not a leaf
/// (<see cref="LeafConverterRegistration"/> runs the leaf cases): writes from the built and read-back columns, for all
/// rows and for a tail that starts above row 0, and the answer of the derivation. Each arm must give the bytes and
/// answers of the client's insert write and of <c>ClickHouseTcpTypes.CanWrite</c>.
/// </summary>
/// <remarks>
/// <para>
/// The write arm routes a column as the insert tier does: a column that the codec writes from its own storage (a dense
/// <c>Nested</c> or <c>Variant</c> column that a case builds from decoded columns) goes to the codec, and every other
/// column through the derived tree (decision D3). The decoded input is not run here: a decoded column is written from its own
/// storage.
/// </para>
/// <para>
/// A refusal of the derivation is an <see cref="ArmRefusal"/>, as in <see cref="LeafConverterRegistration"/>.
/// </para>
/// </remarks>
internal sealed class WriteConverterRegistration : IDifferentialRegistration
{
    // The Write facets of the case list whose column type is not a leaf, without the decoded inputs, and the CanWrite
    // facets of those cases. A new case changes these counts.
    internal const int CompositeWriteFacets = 349;
    internal const int CompositeCanWriteFacets = 573;

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.Add(new CompositeWriteArm(), CompositeWriteFacets);
        registry.Add(new CanWriteArm(), CompositeCanWriteFacets);
    }

    private static bool IsComposite(Facet facet) => !LeafConverterRegistration.IsLeafType(facet.Case.ColumnType);

    // Writes a slice through Begin, WritePrefix and Write, from one span of the column's values.
    private sealed class CompositeWriteArm : WriteArm
    {
        public CompositeWriteArm()
            : base("Composite write converters: Write")
        {
        }

        public override bool Covers(Facet facet) => facet.Input.Kind != WriteInputKind.Decoded && IsComposite(facet);

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, context);
            if (codec.WritesFromStorage(column))
            {
                return (output, start, length) => WriteThroughCodec(codec, column, output, start, length);
            }

            Derivation derivation = ConverterDerivation.Default.Derive(columnType, context, typeof(T), ConversionDirection.Write);
            if (!derivation.Succeeded)
            {
                throw new ArmRefusal(derivation.Refusal);
            }

            var writer = (ColumnWriter<T>)derivation.Converter;
            return (output, start, length) =>
            {
                // A caller's column gives its values through the indexer; the values span of a view can throw.
                T[] values = ArrayPool<T>.Shared.Rent(Math.Max(length, 1));
                try
                {
                    for (int i = 0; i < length; i++)
                    {
                        values[i] = column[start + i];
                    }

                    ConverterHarness.WriteAll(writer, output, ValueSource<T>.Of(values.AsSpan(0, length), start, column.Name));
                }
                finally
                {
                    ArrayPool<T>.Shared.Return(values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
                }
            };
        }

        private static void WriteThroughCodec(IColumnCodec codec, IColumn column, ClickHouseBinaryWriter output, int start, int length)
        {
            IColumnWriteState state = codec.BeginWrite(column, start, length);
            try
            {
                codec.WriteStatePrefix(output, column, start, length, state);
                codec.WriteColumn(output, column, start, length, state);
            }
            finally
            {
                state?.Dispose();
            }
        }
    }

    // CanWrite: whether the derivation succeeds, with the context of ClickHouseTcpTypes.
    private sealed class CanWriteArm : AnswerArm
    {
        public CanWriteArm()
            : base("Composite write converters: CanWrite", Tier.CanWrite)
        {
        }

        public override bool Covers(Facet facet) => IsComposite(facet);

        public override bool Answer(string columnType, Type elementType)
            => ConverterDerivation.Default.Derive(columnType, ResolveContext.ForWrite, elementType, ConversionDirection.Write).Succeeded;
    }
}
