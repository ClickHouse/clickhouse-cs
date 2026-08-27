using System;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Format;

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

    /// <summary>The column called <paramref name="name"/>, as the typed view its values read through.</summary>
    /// <remarks>Same scan and the same advice as <see cref="this[string]"/>: bind once, outside the row loop.</remarks>
    /// <typeparam name="T">The CLR element type the column's values read as.</typeparam>
    /// <param name="name">The column name.</param>
    /// <returns>The typed column.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The block has no column with that name.</exception>
    /// <exception cref="InvalidCastException">The column's values cannot be read as <typeparamref name="T"/>.</exception>
    public IColumn<T> Column<T>(string name) => Typed<T>(this[name]);

    /// <summary>The column at <paramref name="index"/>, as the typed view its values read through.</summary>
    /// <typeparam name="T">The CLR element type the column's values read as.</typeparam>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The typed column.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is not a column of this block.</exception>
    /// <exception cref="InvalidCastException">The column's values cannot be read as <typeparamref name="T"/>.</exception>
    public IColumn<T> Column<T>(int index)
    {
        if (index < 0 || index >= Columns.Count)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index),
                index,
                $"{Describe()} has {Columns.Count} columns.");
        }

        return Typed<T>(Columns[index]);
    }

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
                $"Column '{column.Name}' has type '{column.TypeName}', whose values cannot be read as {typeof(T).Name}.");

    private ArgumentException NoSuchColumn(string name)
        => new($"{Describe()} has no column named '{name}'. Its columns are: {string.Join(", ", ColumnNames)}.", nameof(name));

    private string Describe() => string.IsNullOrEmpty(Name) ? "The block" : $"Block '{Name}'";
}
