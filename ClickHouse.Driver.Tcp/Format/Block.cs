using System;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// A decoded block: the columnar unit exchanged for Data, Totals, Extremes, Log, and ProfileEvents. Carries an
/// optional name, the block info, and the columns (each holding <see cref="RowCount"/> values).
///
/// <para>
/// A block <b>borrows</b> its columns' storage, which may be pooled. It is disposed by the reader that produced
/// it — for a streamed query, when the consumer advances to the next block or stops enumerating. A consumer
/// must not read a block's columns after that point; to retain values beyond the block, copy them (for example
/// <c>Values.ToArray()</c>) while iterating. Do <b>not</b> dispose a block yielded by a query yourself: the
/// reader owns its lifetime, and disposing it early would return borrowed pooled storage the reader still manages.
/// </para>
/// </summary>
public sealed class Block : IDisposable
{
    private string[] columnNames;

    /// <summary>Initializes a new instance of the <see cref="Block"/> class.</summary>
    /// <param name="name">The block name (usually empty for result blocks).</param>
    /// <param name="info">The block info prefix.</param>
    /// <param name="rowCount">The number of rows every column holds.</param>
    /// <param name="columns">The decoded columns, in header order.</param>
    /// <param name="codecs">The registry the columns' codecs came from.</param>
    /// <param name="context">The context they were resolved with.</param>
    internal Block(string name, BlockInfo info, int rowCount, IReadOnlyList<IColumn> columns, ColumnCodecRegistry codecs, ResolveContext context)
    {
        Name = name;
        Info = info;
        RowCount = rowCount;
        Columns = columns;
        Codecs = codecs;
        Context = context;
    }

    /// <summary>The block name.</summary>
    public string Name { get; }

    /// <summary>The block info prefix.</summary>
    internal BlockInfo Info { get; }

    /// <summary>
    /// The registry used to resolve this block's codecs, retained for projections and INSERT sample-block planning.
    /// </summary>
    internal ColumnCodecRegistry Codecs { get; }

    /// <summary>The context the columns' codecs were resolved with (the session timezone). See <see cref="Codecs"/>.</summary>
    internal ResolveContext Context { get; }

    /// <summary>The number of rows in the block.</summary>
    public int RowCount { get; }

    /// <summary>The number of columns in the block.</summary>
    public int ColumnCount => Columns.Count;

    /// <summary>The decoded columns, in header order.</summary>
    public IReadOnlyList<IColumn> Columns { get; }

    /// <summary>
    /// The column names, in header order — the same order as <see cref="Columns"/> and the <c>object[]</c> rows
    /// produced by the client's untyped read. Pair this with a row to address values by name. Computed once and
    /// cached; the returned list is owned (safe to retain past the block, unlike the columns themselves).
    /// </summary>
    public IReadOnlyList<string> ColumnNames
    {
        get
        {
            // Fully populate a local, then publish the reference with a release write so a concurrent reader
            // never observes the array before its elements are written. A benign double-compute (two readers
            // racing the first access) yields equivalent arrays, so only the torn-publication needs guarding.
            string[] existing = Volatile.Read(ref columnNames);
            if (existing is not null)
            {
                return existing;
            }

            var names = new string[Columns.Count];
            for (int i = 0; i < names.Length; i++)
            {
                names[i] = Columns[i].Name;
            }

            Volatile.Write(ref columnNames, names);
            return names;
        }
    }

    /// <summary>The column at <paramref name="index"/>.</summary>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The column at that position.</returns>
    public IColumn this[int index] => Columns[index];

    /// <summary>The column called <paramref name="name"/>, matched ordinally.</summary>
    /// <remarks>
    /// The lookup is a scan of <see cref="Columns"/>, so bind a column once before a row loop rather than
    /// addressing it per row. ClickHouse column names are case-sensitive, and so is this.
    /// </remarks>
    /// <param name="name">The column name.</param>
    /// <returns>The column with that name.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The block has no column with that name.</exception>
    public IColumn this[string name]
    {
        get
        {
            ArgumentNullException.ThrowIfNull(name);
            return TryGetColumn(name, out IColumn column) ? column : throw NoSuchColumn(name);
        }
    }

    /// <summary>Finds the column called <paramref name="name"/>, matched ordinally.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="column">The column with that name, or null when there is none.</param>
    /// <returns>Whether the block has a column with that name.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    public bool TryGetColumn(string name, out IColumn column)
    {
        ArgumentNullException.ThrowIfNull(name);

        // Scanned rather than looked up in a dictionary: building one would allocate per block, and a block is
        // decoded, read and released. Callers bind their columns once, outside the row loop.
        IReadOnlyList<IColumn> columns = Columns;
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, name, StringComparison.Ordinal))
            {
                column = columns[i];
                return true;
            }
        }

        column = null;
        return false;
    }

    /// <summary>
    /// The column called <paramref name="name"/>, as the typed view its values read through. This is a cast:
    /// <typeparamref name="T"/> is the type the column decoded to, so a <c>DateTime64</c> column is an
    /// <c>IColumn&lt;long&gt;</c> and an <c>Enum8</c> an <c>IColumn&lt;sbyte&gt;</c>. To read a column as another
    /// type its ClickHouse type offers, use <see cref="ReadAs{T}(string)"/>.
    /// </summary>
    /// <remarks>Same scan and the same advice as <see cref="this[string]"/>: bind once, outside the row loop.</remarks>
    /// <typeparam name="T">The CLR element type the column's values read as.</typeparam>
    /// <param name="name">The column name.</param>
    /// <returns>The typed column.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The block has no column with that name.</exception>
    /// <exception cref="InvalidCastException">The column's values are not <typeparamref name="T"/>.</exception>
    public IColumn<T> Column<T>(string name) => Typed<T>(this[name]);

    /// <summary>
    /// The column at <paramref name="index"/>, as the typed view its values read through. A cast, as
    /// <see cref="Column{T}(string)"/> is.
    /// </summary>
    /// <typeparam name="T">The CLR element type the column's values read as.</typeparam>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The typed column.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is not a column of this block.</exception>
    /// <exception cref="InvalidCastException">The column's values are not <typeparamref name="T"/>.</exception>
    public IColumn<T> Column<T>(int index) => Typed<T>(At(index));

    /// <summary>
    /// The column called <paramref name="name"/> read as <typeparamref name="T"/>, converting each value if the
    /// column did not decode to that type: a <c>DateTime64(3)</c> column as a <see cref="DateTime"/>, an
    /// <c>Enum8</c> as its label, an <c>Array(DateTime)</c> as a <see cref="DateTime"/><c>[]</c> per row, a
    /// <c>String</c> as its raw <c>byte[]</c>, a <c>FixedString(N)</c> as the text of its <c>N</c> bytes. Which
    /// readings a type offers is the type's own business, and the same set the POCO tier maps from — a
    /// <c>UInt32</c> column reads as a <c>uint</c> and nothing else, and asking for anything else fails naming
    /// what it does read as.
    ///
    /// <para>
    /// When <typeparamref name="T"/> is the column's own element type this <em>is</em> the column, so the fast
    /// path costs nothing and <see cref="IColumn{T}.Values"/> is still the borrowed span. Otherwise the result is
    /// a converting view: the indexer projects one value per call, and <see cref="IColumn{T}.Values"/>
    /// materializes the whole column into an array of its own, once. Bind it once outside the row loop, as with
    /// every accessor here, and read it while the block is alive — the values underneath belong to the block.
    /// </para>
    ///
    /// <para>
    /// A reading is taken from whichever of the column's two forms can express it. Most come from the decoded
    /// value, but a <c>String</c>'s bytes come off the column's storage, because the decoded value is text and
    /// text cannot spell a byte string: see <see cref="IStringColumn"/>, which is the same bytes borrowed rather
    /// than copied per row.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The CLR type to read the values as.</typeparam>
    /// <param name="name">The column name.</param>
    /// <returns>The column read as <typeparamref name="T"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The block has no column with that name.</exception>
    /// <exception cref="InvalidCastException">The column's ClickHouse type offers no reading as <typeparamref name="T"/>.</exception>
    public IColumn<T> ReadAs<T>(string name) => Codecs.Projections.ReadAs<T>(this[name], Context);

    /// <summary>
    /// The column at <paramref name="index"/> read as <typeparamref name="T"/>. Same rules and same costs as
    /// <see cref="ReadAs{T}(string)"/>.
    /// </summary>
    /// <typeparam name="T">The CLR type to read the values as.</typeparam>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The column read as <typeparamref name="T"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is not a column of this block.</exception>
    /// <exception cref="InvalidCastException">The column's ClickHouse type offers no reading as <typeparamref name="T"/>.</exception>
    public IColumn<T> ReadAs<T>(int index) => Codecs.Projections.ReadAs<T>(At(index), Context);

    /// <summary>Releases the columns' storage (returning any pooled buffers). Idempotent.</summary>
    public void Dispose()
    {
        foreach (IColumn column in Columns)
        {
            column.Dispose();
        }
    }

    private static IColumn<T> Typed<T>(IColumn column)
        => column as IColumn<T>
            ?? throw new InvalidCastException(
                $"Column '{column.Name}' has type '{column.TypeName}', whose values are {column.ElementType}, not {typeof(T)}. " +
                $"{nameof(ReadAs)} converts a column to another reading, where its ClickHouse type offers one.");

    private IColumn At(int index)
        => index >= 0 && index < Columns.Count
            ? Columns[index]
            : throw new ArgumentOutOfRangeException(nameof(index), index, $"{Describe()} has {Columns.Count} columns.");

    private ArgumentException NoSuchColumn(string name)
        => new($"{Describe()} has no column named '{name}'. Its columns are: {string.Join(", ", ColumnNames)}.", nameof(name));

    private string Describe() => string.IsNullOrEmpty(Name) ? "The block" : $"Block '{Name}'";
}
