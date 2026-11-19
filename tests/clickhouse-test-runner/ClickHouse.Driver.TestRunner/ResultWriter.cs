using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.TestRunner;

/// <summary>Writes one result set in a format of the TabSeparated family.</summary>
internal sealed class ResultWriter
{
    private readonly Utf8Buffer output;
    private readonly OutputFormat format;
    private readonly ValueRenderer renderer;
    private ChType[] types = [];
    private bool headerWritten;

    public ResultWriter(Utf8Buffer output, OutputFormat format, ValueRenderer renderer)
    {
        this.output = output;
        this.format = format;
        this.renderer = renderer;
    }

    public ValueRenderer Renderer => renderer;

    /// <summary>Appends rows that another writer wrote (the totals or the extremes).</summary>
    public void Append(Utf8Buffer rows) => output.Append(rows);

    /// <summary>
    /// Sets the columns. It writes the header lines of the <c>WithNames</c> formats the first time.
    /// </summary>
    /// <param name="names">The column names.</param>
    /// <param name="typeNames">The column type names, as the client gives them.</param>
    public void Columns(IReadOnlyList<string> names, IReadOnlyList<string> typeNames)
    {
        if (types.Length != typeNames.Count || !headerWritten)
        {
            types = typeNames.Select(ChType.Parse).ToArray();
        }
        if (headerWritten || format.Discard)
        {
            headerWritten = true;
            return;
        }
        headerWritten = true;
        if (format.Names)
        {
            HeaderLine(names);
        }
        if (format.Types)
        {
            HeaderLine(typeNames);
        }
    }

    /// <summary>Writes one row. <paramref name="value"/> gives the value of column i.</summary>
    /// <param name="value">The value of a column.</param>
    /// <param name="column">TCP only: the column of index i, so that the renderer can take time zones and enum
    /// labels from the client.</param>
    /// <param name="row">TCP only: the row in <paramref name="column"/>.</param>
    public void Row(Func<int, object?> value, Func<int, IColumn?>? column = null, int row = -1)
    {
        if (format.Discard)
        {
            // `FORMAT Null`: the values are read, but not written.
            for (var i = 0; i < types.Length; i++)
            {
                _ = value(i);
            }
            return;
        }
        for (var i = 0; i < types.Length; i++)
        {
            if (i > 0)
            {
                output.Append((byte)'\t');
            }
            var v = value(i);
            var c = column?.Invoke(i);
            if (format.Raw && ValueRenderer.IsStringType(types[i]) && renderer.TryRenderRaw(output, types[i], v, c, row))
            {
                continue;
            }
            renderer.Render(output, types[i], v, false, c, row);
        }
        output.Append((byte)'\n');
    }

    /// <summary>Writes the empty line before the totals or the extremes.</summary>
    public void Separator()
    {
        if (!format.Discard)
        {
            output.Append((byte)'\n');
        }
    }

    private void HeaderLine(IReadOnlyList<string> values)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (i > 0)
            {
                output.Append((byte)'\t');
            }
            output.AppendEscaped(values[i], false);
        }
        output.Append((byte)'\n');
    }
}
