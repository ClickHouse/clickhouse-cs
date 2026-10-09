using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// A write leaf whose canonical value is a run of bytes: <c>String</c>, <c>FixedString(N)</c> and <c>JSON</c>. A
/// LowCardinality writer can intern and write its dictionary with <see cref="ToCanonical"/> and <see cref="Encode"/>.
/// </summary>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
internal abstract class BytesLeafWriter<T> : ColumnWriter<T>
{
    /// <summary>
    /// The canonical bytes that the leaf writes at a position that has no value (for example under a NULL). They are
    /// the same for every CLR type that the leaf writes.
    /// </summary>
    public abstract ReadOnlySpan<byte> Placeholder { get; }

    /// <summary>
    /// Whether two values that are equal by <see cref="EqualityComparer{T}.Default"/> always have equal canonical
    /// bytes. When true, a LowCardinality writer can look a value up by its CLR value first, and convert it only when
    /// the lookup fails. The converse is not necessary: two values that are not equal and have the same bytes still
    /// get one dictionary entry, because the writer interns the bytes.
    /// </summary>
    public virtual bool ClrEqualityImpliesCanonicalEquality => false;

    /// <summary>The canonical bytes of one value.</summary>
    /// <param name="value">The value.</param>
    /// <param name="position">The zero-based position of the value in the write, for error messages.</param>
    /// <param name="scratch">
    /// A buffer that the leaf can use: an array from <see cref="ArrayPool{T}.Shared"/>, or an empty array. The leaf
    /// replaces it with a larger one when necessary and returns the old one to the pool. The caller returns the last
    /// one.
    /// </param>
    /// <returns>The bytes. They can be in <paramref name="scratch"/> or in the value, so they are valid only until the next call.</returns>
    /// <exception cref="ArgumentException">The value cannot be stored in the ClickHouse type.</exception>
    public abstract ReadOnlySpan<byte> ToCanonical(T value, int position, ref byte[] scratch);

    /// <summary>Writes canonical bytes as one value, for example one entry of a LowCardinality dictionary.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="canonical">The canonical bytes.</param>
    public abstract void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical);

    /// <summary>Makes <paramref name="scratch"/> at least <paramref name="length"/> bytes long.</summary>
    /// <param name="scratch">A pooled or empty array, which is replaced when it is too short.</param>
    /// <param name="length">The length that is necessary.</param>
    protected static void EnsureScratch(ref byte[] scratch, int length)
    {
        if (scratch.Length >= length)
        {
            return;
        }

        byte[] old = scratch;
        scratch = ArrayPool<byte>.Shared.Rent(length);
        if (old.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(old);
        }
    }
}

/// <summary><c>String</c> or <c>JSON</c> from text. The canonical bytes are the UTF-8 encoding of the text.</summary>
internal sealed class TextStringWriter : BytesLeafWriter<string>
{
    /// <summary><c>String</c> from text. Its placeholder is the empty string.</summary>
    public static readonly TextStringWriter String = new(Array.Empty<byte>(), jsonPrefix: false);

    /// <summary>
    /// <c>JSON</c> from text, in the String serialization. Its prefix is the serialization version, and its
    /// placeholder is an empty object, because the server parses the value under a NULL too.
    /// </summary>
    public static readonly TextStringWriter Json = new("{}"u8.ToArray(), jsonPrefix: true);

    // The serialization version of JSON as text, which is the only JSON serialization that this client writes.
    private const ulong JsonStringVersion = 1;

    private readonly byte[] placeholder;
    private readonly bool jsonPrefix;

    private TextStringWriter(byte[] placeholder, bool jsonPrefix)
    {
        this.placeholder = placeholder;
        this.jsonPrefix = jsonPrefix;
    }

    /// <inheritdoc/>
    public override bool HasPrefix => jsonPrefix;

    /// <inheritdoc/>
    public override ReadOnlySpan<byte> Placeholder => placeholder;

    /// <inheritdoc/>
    // Equal strings have equal UTF-8. Two strings that are not equal can have the same UTF-8: each lone surrogate
    // encodes as EF BF BD.
    public override bool ClrEqualityImpliesCanonicalEquality => true;

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<string> values, IColumnWriteState state)
    {
        if (jsonPrefix)
        {
            writer.WriteUInt64(JsonStringVersion);
        }
    }

    /// <inheritdoc/>
    public override ReadOnlySpan<byte> ToCanonical(string value, int position, ref byte[] scratch)
    {
        ArgumentNullException.ThrowIfNull(value);
        EnsureScratch(ref scratch, Encoding.UTF8.GetMaxByteCount(value.Length));
        return scratch.AsSpan(0, Encoding.UTF8.GetBytes(value, scratch));
    }

    /// <inheritdoc/>
    public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical) => writer.WriteString(canonical);

    /// <inheritdoc/>
    // The writer encodes each string into its own buffer, so a value is not copied to a canonical form first.
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<string> values, IColumnWriteState state)
    {
        ReadOnlySpan<byte> absent = values.Absent;
        bool marked = values.HasAbsent;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            foreach (string value in values.Run(r))
            {
                if (marked && absent[position] != 0)
                {
                    writer.WriteString(placeholder);
                }
                else
                {
                    writer.WriteString(value);
                }

                position++;
            }
        }
    }
}

/// <summary><c>String</c> from raw bytes. The bytes are the canonical value and are written with no change.</summary>
internal sealed class BytesStringWriter : BytesLeafWriter<byte[]>
{
    /// <summary>The shared instance. The leaf has no state.</summary>
    public static readonly BytesStringWriter Instance = new();

    private BytesStringWriter()
    {
    }

    /// <inheritdoc/>
    public override ReadOnlySpan<byte> Placeholder => ReadOnlySpan<byte>.Empty;

    /// <inheritdoc/>
    public override ReadOnlySpan<byte> ToCanonical(byte[] value, int position, ref byte[] scratch)
        => value ?? throw NullValue(position);

    /// <inheritdoc/>
    public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical) => writer.WriteString(canonical);

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<byte[]> values, IColumnWriteState state)
    {
        ReadOnlySpan<byte> absent = values.Absent;
        bool marked = values.HasAbsent;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            foreach (byte[] value in values.Run(r))
            {
                if (marked && absent[position] != 0)
                {
                    writer.WriteString(ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    writer.WriteString(value ?? throw NullValue(values.FirstRow + position));
                }

                position++;
            }
        }
    }

    // The parameter name is the one that the String codec reports for the same value.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
    private static ArgumentException NullValue(int row)
        => new($"A String column cannot hold a null value (at row {row}); wrap the type in Nullable to write nulls.", "column");
#pragma warning restore CA2208
}

/// <summary>
/// <c>FixedString(N)</c> from raw bytes. Each value must be exactly <c>N</c> bytes: the writer does not pad or cut a
/// value, because that would change the data without a signal.
/// </summary>
internal sealed class FixedStringBytesWriter : BytesLeafWriter<byte[]>
{
    private readonly int size;
    private readonly string typeName;
    private readonly byte[] placeholder;

    /// <summary>Initializes the leaf for one width.</summary>
    /// <param name="size">The <c>N</c> of <c>FixedString(N)</c>.</param>
    /// <param name="typeName">The ClickHouse type, for the messages.</param>
    public FixedStringBytesWriter(int size, string typeName)
    {
        this.size = size;
        this.typeName = typeName;
        placeholder = new byte[size];
    }

    /// <inheritdoc/>
    public override ReadOnlySpan<byte> Placeholder => placeholder;

    /// <inheritdoc/>
    public override ReadOnlySpan<byte> ToCanonical(byte[] value, int position, ref byte[] scratch)
        => Checked(value, position, "row");

    /// <inheritdoc/>
    public override void Encode(ClickHouseBinaryWriter writer, ReadOnlySpan<byte> canonical) => writer.WriteBytes(canonical);

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<byte[]> values, IColumnWriteState state)
    {
        ReadOnlySpan<byte> absent = values.Absent;
        bool marked = values.HasAbsent;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            ReadOnlySpan<byte[]> run = values.Run(r);
            for (int i = 0; i < run.Length; i++)
            {
                if (marked && absent[position] != 0)
                {
                    writer.WriteBytes(placeholder);
                }
                else if (values.IsSegmented && !marked)
                {
                    // A segment is the array of one row, so a message names the element of that array. Under marks
                    // the values come from a Nullable child, and a message names the flat position, as for a column.
                    writer.WriteBytes(Checked(run[i], i, "element"));
                }
                else
                {
                    writer.WriteBytes(Checked(run[i], values.FirstRow + position, "row"));
                }

                position++;
            }
        }
    }

    private byte[] Checked(byte[] value, int position, string positionNoun)
    {
        if (value is null)
        {
            throw new ArgumentException(
                $"A {typeName} column cannot hold a null value (at {positionNoun} {position}); wrap the type in Nullable to write nulls.",
                nameof(value));
        }

        if (value.Length != size)
        {
            throw new ArgumentException(
                $"A {typeName} value at {positionNoun} {position} is {value.Length} bytes; every value must be exactly {size} bytes. Resize it to {size} bytes before writing it — the write path will not pad or truncate, since doing so would silently alter the data.",
                nameof(value));
        }

        return value;
    }
}
