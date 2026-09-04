using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>Reads and writes one ClickHouse column type.</summary>
internal interface IColumnCodec
{
    /// <summary>The ClickHouse type name handled by this codec.</summary>
    string TypeName { get; }

    /// <summary>The canonical CLR type returned by <see cref="ReadColumnAsync"/>.</summary>
    Type ElementType { get; }

    /// <summary>
    /// Preferred CLR write types. Composite combinations may be omitted; test them with
    /// <see cref="CanWriteElementType"/>.
    /// </summary>
    IReadOnlyList<Type> WritableElementTypes => new[] { ElementType };

    /// <summary>
    /// Common readable CLR types. Composite combinations may be omitted; test them with
    /// <see cref="ColumnProjection.Offers"/>, which is the authority.
    /// </summary>
    IReadOnlyList<Type> ReadableElementTypes => new[] { ElementType };

    /// <summary>
    /// Builds an expression that projects a canonical value to <paramref name="targetType"/>. This is the reading
    /// of a codec whose conversion is elementwise, and the read tiers inline it: the POCO scatter splices it into
    /// its own row loop, and <see cref="ColumnProjection"/> compiles it into a view. A reading that is not a
    /// function of one value goes to <see cref="TryProjectColumnRead"/> instead.
    /// </summary>
    /// <param name="value">An expression of type <see cref="ElementType"/>.</param>
    /// <param name="targetType">The requested CLR type.</param>
    /// <param name="projected">An expression of type <paramref name="targetType"/>, or null when none is offered.</param>
    /// <returns>Whether a projection to <paramref name="targetType"/> exists.</returns>
    /// <exception cref="ArgumentException"><paramref name="value"/> is not of type <see cref="ElementType"/>.</exception>
    bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, ElementType, TypeName);
        projected = targetType == ElementType ? value : null;
        return projected is not null;
    }

    /// <summary>
    /// Builds a projection from a decoded column to a view of it reading as <paramref name="targetType"/>. Asked
    /// before <see cref="TryProjectRead"/>, and offered by no codec by default.
    ///
    /// <para>
    /// It is where a reading goes when it is not a function of one value:
    /// </para>
    /// <list type="bullet">
    /// <item>a <c>String</c>'s bytes, which its canonical UTF-8 text has already lost — a byte UTF-8 cannot spell
    /// decodes to U+FFFD, so re-encoding that text would hand back the replacement character;</item>
    /// <item>a <c>LowCardinality</c> row, which is a dictionary slot: the conversion belongs to the dictionary, and
    /// its result is then shared by every row holding that key;</item>
    /// <item>a composite's, which is its child column's — projected once, then addressed per row through the
    /// offsets or children the composite already holds.</item>
    /// </list>
    ///
    /// <para>
    /// A composite offers one exactly where a child does, so a reading every child expresses elementwise stays on
    /// the cheaper <see cref="TryProjectRead"/> path. <c>LowCardinality</c> is the exception: it offers one
    /// whenever its inner offers any reading at all, because converting per dictionary entry rather than per row is
    /// the point of the type.
    /// </para>
    /// </summary>
    /// <param name="targetType">The requested CLR type.</param>
    /// <param name="projection">A projection producing an <c>IColumn&lt;targetType&gt;</c>, or null when none is offered.</param>
    /// <returns>Whether a column-level reading as <paramref name="targetType"/> exists.</returns>
    bool TryProjectColumnRead(Type targetType, out ColumnReadProjection projection)
    {
        projection = null;
        return false;
    }

    /// <summary>A valid canonical value for the hidden inner value of a null.</summary>
    object NullPlaceholder { get; }

    /// <summary>
    /// Returns a writable placeholder of <paramref name="writeType"/>. This must support every type accepted by
    /// <see cref="CanWriteElementType"/>.
    /// </summary>
    /// <param name="writeType">The CLR write type to express the placeholder in.</param>
    /// <returns>The placeholder value, assignable to <paramref name="writeType"/>.</returns>
    /// <exception cref="NotSupportedException"><paramref name="writeType"/> is not a writable element type.</exception>
    object NullPlaceholderAs(Type writeType) => writeType == ElementType
        ? NullPlaceholder
        : throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");

    /// <summary>Reads the column's serialization state prefix, if any. Default: none.</summary>
    /// <param name="reader">The reader positioned at the prefix.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the prefix has been consumed.</returns>
    ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken) => default;

    /// <summary>Reads exactly <paramref name="rowCount"/> values into a column.</summary>
    /// <param name="reader">The reader positioned at the column body.</param>
    /// <param name="columnName">The column name from the block header.</param>
    /// <param name="columnType">The full ClickHouse type string from the block header (stamped onto the column).</param>
    /// <param name="rowCount">The number of values to read.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded column.</returns>
    ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken);

    /// <summary>Whether <see cref="WriteColumn"/> accepts this concrete column.</summary>
    /// <param name="column">The column to test.</param>
    /// <returns><see langword="true"/> if <see cref="WriteColumn"/> accepts <paramref name="column"/>.</returns>
    bool CanWrite(IColumn column);

    /// <summary>Whether a row-oriented column with <paramref name="elementType"/> can be written.</summary>
    /// <param name="elementType">The candidate CLR element type.</param>
    /// <returns>Whether a column of that element type can be written.</returns>
    bool CanWriteElementType(Type elementType)
    {
        if (elementType == ElementType)
        {
            return true;
        }

        IReadOnlyList<Type> writable = WritableElementTypes;
        for (int i = 0; i < writable.Count; i++)
        {
            if (writable[i] == elementType)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The CLR type accepted by <see cref="WriteCanonicalColumn"/> after write conversion.</summary>
    Type CanonicalWriteElementType => ElementType;

    /// <summary>
    /// Whether <see cref="ToCanonicalWriteColumn"/> can express a column of <paramref name="writeType"/> as
    /// <see cref="CanonicalWriteElementType"/>. Every writable type can by default, and a codec whose conversions
    /// cover all of them need not override this.
    ///
    /// <para>
    /// It exists for the codecs that go through the canonical form — <c>LowCardinality</c>, which deduplicates the
    /// canonical values — because they have to refuse such a type <em>before</em> the write rather than fault
    /// part-way through it. A codec that accepts a write type it cannot canonicalize (<c>String</c> takes raw
    /// bytes, which have no lossless spelling as its canonical <see cref="string"/>) says so here.
    /// </para>
    /// </summary>
    /// <param name="writeType">The candidate CLR write type.</param>
    /// <returns>Whether a column of that type can be projected to the canonical write form.</returns>
    bool CanCanonicalizeWriteType(Type writeType) => CanWriteElementType(writeType);

    /// <summary>A valid placeholder expressed as <see cref="CanonicalWriteElementType"/>.</summary>
    object CanonicalWritePlaceholder => NullPlaceholder;

    /// <summary>
    /// Whether <paramref name="value"/> belongs to this type rather than to a sibling that surfaces the same CLR
    /// element type. Asked only to break a tie between <c>Variant</c> alternatives that collide on
    /// <see cref="ElementType"/>, and only for a value whose type already reached this codec, so it is never on
    /// the path of an unambiguous write.
    ///
    /// <para>
    /// Exactly one alternative must claim the value for the tie to resolve; zero or several is a refusal. The
    /// default therefore claims everything, which is the safe answer for a codec with no value-level test: it can
    /// never win a tie on its own, so a genuine ambiguity (<c>Variant(JSON, String)</c> given a string) stays a
    /// refusal rather than becoming a silent pick.
    /// </para>
    ///
    /// <para>
    /// This is narrower than <see cref="CanWrite"/>: a codec may decline a value here that its writer would
    /// otherwise accept, because the question is which alternative the value <em>means</em>, not which one could
    /// store it. <c>IPv6</c> writes an IPv4 address by mapping it, but declines it beside an <c>IPv4</c>
    /// alternative that is the better home for it.
    /// </para>
    /// </summary>
    /// <param name="value">The non-null value being placed, of this codec's element type.</param>
    /// <returns>Whether this codec claims the value.</returns>
    bool ClaimsValue(object value) => true;

    /// <summary>
    /// Projects a writable column to <see cref="CanonicalWriteElementType"/>. Equal projected values must produce
    /// identical bytes through <see cref="WriteCanonicalColumn"/>. The returned column borrows the source and
    /// preserves its row indexes.
    /// </summary>
    IColumn ToCanonicalWriteColumn(IColumn column) => column;

    /// <summary>Writes values already converted to <see cref="CanonicalWriteElementType"/>.</summary>
    void WriteCanonicalColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
        => WriteColumn(writer, column, start, length);

    /// <summary>Prepares state shared by the prefix and body. The caller disposes it after the body.</summary>
    /// <param name="column">The column about to be written; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row the write will cover.</param>
    /// <param name="length">The number of rows the write will cover.</param>
    /// <returns>The per-operation write state, or <see langword="null"/> when the codec needs none.</returns>
    IColumnWriteState BeginWrite(IColumn column, int start, int length) => null;

    /// <summary>Writes the serialization prefix. The default writes nothing.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose prefix to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row the following body will write.</param>
    /// <param name="length">The number of rows the following body will write.</param>
    void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
    }

    /// <summary>Writes the prefix using state from <see cref="BeginWrite"/>.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose prefix to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row the following body will write.</param>
    /// <param name="length">The number of rows the following body will write.</param>
    /// <param name="state">The scratch from this codec's <see cref="BeginWrite"/>; null only for a codec that returns null from it.</param>
    void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => WriteStatePrefix(writer, column, start, length);

    /// <summary>Writes the selected column rows.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose values to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length);

    /// <summary>Writes the body using state from <see cref="BeginWrite"/>.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose values to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    /// <param name="state">The scratch from this codec's <see cref="BeginWrite"/>; null only for a codec that returns null from it.</param>
    void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => WriteColumn(writer, column, start, length);
}
