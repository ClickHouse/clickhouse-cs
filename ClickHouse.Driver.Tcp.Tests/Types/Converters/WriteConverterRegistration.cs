using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Runs an independent insert write in the differential tests, for every case: the write of the built and read-back
/// columns, for all rows and for a tail that starts above row 0. Each must give the bytes of the client's insert write.
/// </summary>
/// <remarks>
/// <para>
/// The arm routes a column as the insert does: a column that the codec writes from its own storage (a dense
/// <c>Nested</c> or <c>Variant</c> column that a case builds from decoded columns) goes to the codec, and every other
/// column through the derived tree (decision D3). It gathers the values of the slice through the indexer of the column,
/// and not through the source of <see cref="Format.InsertColumnWrite"/> (a span, the stored values, or the indexer,
/// with the start, the length and the first row), so it checks the slicing of the insert. The decoded input is not run
/// here: a decoded column is written from its own storage.
/// </para>
/// <para>
/// A refusal of the derivation is an <see cref="ArmRefusal"/>: its message has another text than the refusal of the
/// client's insert, and the refusal texts have their own tests.
/// </para>
/// </remarks>
internal sealed class WriteConverterRegistration : IDifferentialRegistration
{
    // The Write facets of the case list, without the decoded inputs. A new case changes this count.
    internal const int WriteFacets = 545;

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry) => registry.Add(new IndexerWriteArm(), WriteFacets);

    // Writes a slice through Begin, WritePrefix and Write, from one span of the values of the slice.
    private sealed class IndexerWriteArm : WriteArm
    {
        public IndexerWriteArm()
            : base("Insert write through the indexer")
        {
        }

        public override bool Covers(Facet facet) => facet.Input.Kind != WriteInputKind.Decoded;

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
}
