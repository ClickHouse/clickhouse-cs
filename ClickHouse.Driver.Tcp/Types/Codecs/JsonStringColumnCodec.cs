using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>JSON</c> column in its String serialization (version 1) — every value is its
/// compact JSON text, so the column is a <see cref="StringColumnCodec"/> body behind a version marker. The type
/// string is irrelevant to this layout: <c>JSON</c>, <c>JSON(a UInt32)</c> and <c>JSON(max_dynamic_paths=8)</c>
/// all arrive as the same String column.
///
/// <para>
/// The wire layout per non-empty block is a <c>UInt64</c> version (1) — the <em>state prefix</em> — then a
/// standard String column of <c>num_rows</c> values. So under a composite the version precedes the composite's
/// own framing: <c>Array(JSON)</c> is the version, then the array offsets, then the texts.
/// </para>
///
/// <para>
/// The two directions are not symmetric in what they need. Reading requires the query setting
/// <c>output_format_native_write_json_as_string = 1</c> (the client sets it by default), because the server
/// otherwise emits one of the per-path binary encodings, which this codec rejects. Writing needs no setting at
/// all: the server reads whichever version the prefix declares, so a written version 1 makes it parse the text
/// into the real paths server-side — including into the typed paths a <c>JSON(a UInt32)</c> column declares.
/// </para>
///
/// <para>
/// Because a value is parsed rather than stored verbatim, the server normalizes what comes back: keys are sorted
/// ordinally, whitespace is dropped, numbers are re-rendered canonically, and a JSON <c>null</c> or an empty
/// object contributes no path at all. Text in is therefore not always text out.
/// </para>
/// </summary>
internal sealed class JsonStringColumnCodec : IColumnCodec
{
    /// <summary>
    /// The serialization version that heads a <c>JSON</c>/<c>Object</c> column's state prefix in the String
    /// layout — the only layout this client reads or writes. The others (<c>0</c> = V1, <c>2</c> = V2, <c>3</c> =
    /// FLATTENED, <c>4</c> = V3) split the column into one sub-column per JSON path.
    /// </summary>
    private const ulong StringVersion = 1;

    private JsonStringColumnCodec(string typeName) => TypeName = typeName;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(string);

    /// <summary>
    /// The JSON text an empty object serializes to. Unlike every other codec's placeholder, this one reaches the
    /// server as real input: a <c>Nullable(JSON)</c> column's values stream is parsed at every position, the null
    /// ones included, so the empty string a String column would use is rejected as unparseable JSON.
    /// </summary>
    public object NullPlaceholder => "{}";

    /// <summary>Builds a <c>JSON</c> codec.</summary>
    /// <param name="node">The parsed <c>JSON</c> node. Its arguments — typed paths, <c>max_dynamic_paths=N</c>, <c>SKIP</c> hints — do not affect the String layout and are kept only in the type name.</param>
    /// <returns>The codec.</returns>
    public static JsonStringColumnCodec Create(TypeNode node) => new(node.ToString());

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        ulong version = await reader.ReadUInt64Async(cancellationToken).ConfigureAwait(false);
        if (version != StringVersion)
        {
            throw new ClickHouseTcpProtocolException(
                $"JSON column '{TypeName}' uses serialization version {version}; this client supports only the String version {StringVersion}. " +
                "Enable it with the query setting output_format_native_write_json_as_string=1.");
        }
    }

    /// <inheritdoc/>
    public ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
        => StringColumnCodec.Instance.ReadColumnAsync(reader, columnName, columnType, rowCount, cancellationToken);

    /// <inheritdoc/>
    // Text only, unlike String, which also takes a byte[] per row: a JSON value is a document the server parses,
    // so raw bytes are not a shape this type has a meaning for. Stated here rather than delegated so it agrees
    // with WritableElementTypes, and so Array(JSON) and a JSON target answer the same.
    public bool CanWrite(IColumn column) => column is IColumn<string>;

    /// <inheritdoc/>
    // The prefix is a fixed version marker, independent of the data; the column/slice is unused.
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
        => writer.WriteUInt64(StringVersion);

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
        => StringColumnCodec.Instance.WriteColumn(writer, column, start, length);
}
