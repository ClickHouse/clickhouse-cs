using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// A write leaf whose canonical value is a fixed-width wire value (see <see cref="IWriteConversion{T, TCanon}"/>):
/// every leaf that has a writer, except <c>String</c>, <c>FixedString</c> and <c>JSON</c>. A LowCardinality writer
/// can intern and write its dictionary with <see cref="ToCanonical(ReadOnlySpan{T}, Span{TCanon}, int)"/> and
/// <see cref="Encode"/>.
/// </summary>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
/// <typeparam name="TCanon">The canonical value.</typeparam>
internal abstract class FixedLeafWriter<T, TCanon> : ColumnWriter<T>
    where TCanon : unmanaged, IEquatable<TCanon>
{
    /// <summary>Initializes a leaf with its canonical placeholder.</summary>
    /// <param name="placeholder">The canonical value that the leaf writes at a position that has no value.</param>
    protected FixedLeafWriter(TCanon placeholder) => Placeholder = placeholder;

    /// <summary>
    /// The canonical value that the leaf writes at a position that has no value (for example under a NULL). It is the
    /// same for every CLR type that the leaf writes.
    /// </summary>
    public TCanon Placeholder { get; }

    /// <inheritdoc/>
    public sealed override bool IsFlat => true;

    /// <summary>
    /// Whether two equal values of <typeparamref name="T"/> always convert to equal canonical values
    /// (<see cref="IWriteConversion{T, TCanon}.ClrEqualityImpliesCanonicalEquality"/>).
    /// </summary>
    public abstract bool ClrEqualityImpliesCanonicalEquality { get; }

    /// <summary>Writes canonical values, for example the entries of a LowCardinality dictionary.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The canonical values.</param>
    public static void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<TCanon> values) => writer.WriteBytes(MemoryMarshal.AsBytes(values));

    /// <summary>Converts one value to its canonical value.</summary>
    /// <param name="value">The value.</param>
    /// <param name="position">The zero-based position of the value in the write, for error messages.</param>
    /// <returns>The canonical value.</returns>
    public abstract TCanon ToCanonical(T value, int position);

    /// <summary>Converts a run of values to their canonical values.</summary>
    /// <param name="values">The values.</param>
    /// <param name="destination">Receives the canonical values. It must be at least as long as <paramref name="values"/>.</param>
    /// <param name="firstPosition">The position of <c>values[0]</c> in the write, for error messages.</param>
    public abstract void ToCanonical(ReadOnlySpan<T> values, Span<TCanon> destination, int firstPosition);
}

/// <summary>A write leaf over the conversion <typeparamref name="TConv"/>.</summary>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
/// <typeparam name="TCanon">The canonical value.</typeparam>
/// <typeparam name="TConv">The conversion from <typeparamref name="T"/> to <typeparamref name="TCanon"/>.</typeparam>
internal sealed class FixedLeafWriter<T, TCanon, TConv> : FixedLeafWriter<T, TCanon>
    where TCanon : unmanaged, IEquatable<TCanon>
    where TConv : struct, IWriteConversion<T, TCanon>
{
    // The values are converted in chunks of this many bytes on the stack, and each chunk is written in one copy.
    private const int ChunkBytes = 4096;

    private readonly TConv conversion;

    /// <summary>Initializes a leaf.</summary>
    /// <param name="conversion">The conversion, with the state of the column type (for example its scale).</param>
    /// <param name="placeholder">The canonical placeholder of the leaf.</param>
    public FixedLeafWriter(TConv conversion, TCanon placeholder)
        : base(placeholder)
        => this.conversion = conversion;

    /// <inheritdoc/>
    public override bool ClrEqualityImpliesCanonicalEquality => conversion.ClrEqualityImpliesCanonicalEquality;

    /// <inheritdoc/>
    public override TCanon ToCanonical(T value, int position) => conversion.Convert(value, position);

    /// <inheritdoc/>
    public override void ToCanonical(ReadOnlySpan<T> values, Span<TCanon> destination, int firstPosition)
    {
        if (destination.Length < values.Length)
        {
            throw new ArgumentException($"The destination holds {destination.Length} values, but {values.Length} are given.", nameof(destination));
        }

        TConv convert = conversion;
        for (int i = 0; i < values.Length; i++)
        {
            destination[i] = convert.Convert(values[i], firstPosition + i);
        }
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        TConv convert = conversion;
        if (convert.IsReinterpretation && !values.HasAbsent)
        {
            for (int r = 0; r < values.RunCount; r++)
            {
                writer.WriteBytes(StoredBytes(values.Run(r)));
            }

            return;
        }

        Span<TCanon> chunk = stackalloc TCanon[Math.Max(1, ChunkBytes / Unsafe.SizeOf<TCanon>())];
        ReadOnlySpan<byte> absent = values.Absent;
        bool marked = values.HasAbsent;
        TCanon placeholder = Placeholder;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            ReadOnlySpan<T> run = values.Run(r);
            while (!run.IsEmpty)
            {
                int count = Math.Min(chunk.Length, run.Length);
                for (int i = 0; i < count; i++)
                {
                    int at = position + i;
                    chunk[i] = marked && absent[at] != 0 ? placeholder : convert.Convert(run[i], at);
                }

                writer.WriteBytes(MemoryMarshal.AsBytes(chunk.Slice(0, count)));
                run = run.Slice(count);
                position += count;
            }
        }
    }

    // The bytes of the values as stored. Used only when the conversion is a reinterpretation, so T is unmanaged and
    // has the layout of TCanon.
    private static ReadOnlySpan<byte> StoredBytes(ReadOnlySpan<T> values)
        => MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.As<T, byte>(ref MemoryMarshal.GetReference(values)),
            checked(values.Length * Unsafe.SizeOf<T>()));
}
