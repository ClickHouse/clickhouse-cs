using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.ADO.Readers;

// TODO: implement IDbColumnSchemaGenerator
public class ClickHouseDataReader : DbDataReader, IEnumerator<IDataReader>, IEnumerable<IDataReader>, IDataRecord
{
    private const int BufferSize = 512 * 1024;
    private const string ExceptionTagHeader = "X-ClickHouse-Exception-Tag";

    private readonly HttpResponseMessage httpResponse; // Used to dispose at the end of reader
    private readonly ExtendedBinaryReader reader;
    private readonly ExceptionTagAwareStream exceptionTagStream; // Can be null
    private readonly IReadValueConverter readValueConverter; // Can be null
    private readonly string[] columnTypeNames; // Raw server-sent type strings; null when no converter

    private ClickHouseDataReader(HttpResponseMessage httpResponse, ExtendedBinaryReader reader, string[] names, ClickHouseType[] types, string[] rawTypeNames, ExceptionTagAwareStream exceptionTagStream = null, IReadValueConverter readValueConverter = null)
    {
        this.httpResponse = httpResponse ?? throw new ArgumentNullException(nameof(httpResponse));
        this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        this.exceptionTagStream = exceptionTagStream;
        this.readValueConverter = readValueConverter;
        RawTypes = types;
        FieldNames = names;
        CurrentRow = new object[FieldNames.Length];

        if (readValueConverter != null)
            columnTypeNames = rawTypeNames;
    }

    internal static async Task<ClickHouseDataReader> FromHttpResponseAsync(HttpResponseMessage httpResponse, TypeSettings settings, IReadValueConverter readValueConverter = null)
    {
        if (httpResponse is null) throw new ArgumentNullException(nameof(httpResponse));

        // Extract exception tag from header if present
        string exceptionTag = null;
        if (httpResponse.Headers.TryGetValues(ExceptionTagHeader, out var tagValues))
            exceptionTag = System.Linq.Enumerable.FirstOrDefault(tagValues);

        ExtendedBinaryReader reader = null;
        ExceptionTagAwareStream exceptionStream = null;
        try
        {
            var rawStream = await httpResponse.Content.ReadAsStreamAsync().ConfigureAwait(false);
            var buffered = new BufferedStream(rawStream, BufferSize);

            // Conditionally wrap with exception-aware stream
            Stream streamForReader = buffered;
            if (!string.IsNullOrEmpty(exceptionTag))
            {
                exceptionStream = new ExceptionTagAwareStream(buffered, exceptionTag);
                streamForReader = exceptionStream;
            }

            reader = new ExtendedBinaryReader(streamForReader); // will dispose of stream
            var (names, types, rawTypeNames) = ReadHeaders(reader, settings, readValueConverter != null);
            return new ClickHouseDataReader(httpResponse, reader, names, types, rawTypeNames, exceptionStream, readValueConverter);
        }
        catch (Exception)
        {
            httpResponse?.Dispose();
            reader?.Dispose();
            throw;
        }
    }

    internal ClickHouseType GetEffectiveClickHouseType(int ordinal)
    {
        var type = RawTypes[ordinal];
        return type is NullableType nt ? nt.UnderlyingType : type;
    }

    internal ClickHouseType GetClickHouseType(int ordinal) => RawTypes[ordinal];

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => this[GetOrdinal(name)];

    public override int Depth { get; }

    public override int FieldCount => RawTypes?.Length ?? throw new InvalidOperationException();

    public override bool IsClosed => false;

    public sealed override bool HasRows => true;

    public override int RecordsAffected { get; }

    protected object[] CurrentRow { get; set; }

    protected string[] FieldNames { get; set; }

    private protected ClickHouseType[] RawTypes { get; set; }

    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal), CultureInfo.InvariantCulture);

    public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

    public override char GetChar(int ordinal) => (char)GetValue(ordinal);

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

    public override string GetDataTypeName(int ordinal) => GetClickHouseType(ordinal).ToString();

    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);

    public virtual DateTimeOffset GetDateTimeOffset(int ordinal) => GetEffectiveClickHouseType(ordinal) is AbstractDateTimeType adt ?
        adt.CoerceToDateTimeOffset(GetDateTime(ordinal)) : throw new InvalidCastException();

    public override decimal GetDecimal(int ordinal)
    {
        var value = GetValue(ordinal);
        return value is ClickHouseDecimal clickHouseDecimal ? clickHouseDecimal.ToDecimal(CultureInfo.InvariantCulture) : (decimal)value;
    }

    public override double GetDouble(int ordinal) => (double)GetValue(ordinal);

    public override Type GetFieldType(int ordinal)
    {
        var rawType = RawTypes[ordinal];
        return rawType is NullableType nt ? nt.UnderlyingType.FrameworkType : rawType.FrameworkType;
    }

    public override float GetFloat(int ordinal) => (float)GetValue(ordinal);

    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

    public override short GetInt16(int ordinal) => (short)GetValue(ordinal);

    public override int GetInt32(int ordinal) => (int)GetValue(ordinal);

    public override long GetInt64(int ordinal) => (long)GetValue(ordinal);

    public override string GetName(int ordinal) => FieldNames[ordinal];

    public override int GetOrdinal(string name)
    {
        var index = Array.FindIndex(FieldNames, (fn) => fn == name);
        if (index == -1)
        {
            throw new ArgumentException("Column does not exist", nameof(name));
        }

        return index;
    }

    public override string GetString(int ordinal) => GetValue(ordinal)?.ToString();

    public override object GetValue(int ordinal)
        => readValueConverter == null
            ? CurrentRow[ordinal]
            : readValueConverter.ConvertValue(CurrentRow[ordinal], FieldNames[ordinal], columnTypeNames[ordinal]);

    public override int GetValues(object[] values)
    {
        if (CurrentRow == null)
        {
            throw new InvalidOperationException();
        }

        var count = Math.Min(CurrentRow.Length, values.Length);

        if (readValueConverter != null)
        {
            for (var i = 0; i < count; i++)
                values[i] = readValueConverter.ConvertValue(CurrentRow[i], FieldNames[i], columnTypeNames[i]);
        }
        else
        {
            Array.Copy(CurrentRow, values, count);
        }

        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        // Read CurrentRow directly rather than going through GetValue so a configured
        // IReadValueConverter does not run during a null check — it could throw, do
        // expensive work, or change the nullness of the result.
        var value = CurrentRow[ordinal];
        return value is DBNull || value is null;
    }

    public override bool NextResult() => false;

    public override void Close() => Dispose();

    /// <summary>
    /// Returns the value of the specified column typed as <typeparamref name="T"/>.
    /// For rectangular multidimensional CLR array types (<c>T[,]</c>, <c>T[,,]</c>, …) the
    /// jagged ClickHouse value is materialised in place. <see cref="InvalidCastException"/>
    /// signals a type-structure mismatch — the column is null/DBNull, the value isn't a
    /// collection, its structural depth differs from the target rank, or a leaf can't be
    /// assigned to the target element type (this matches the ADO.NET
    /// <see cref="DbDataReader.GetFieldValue{T}"/> contract). <see cref="InvalidOperationException"/>
    /// signals a shape-validation failure — the value's structure matches
    /// <typeparamref name="T"/> but rows are ragged or an intermediate row is null. For every
    /// other <typeparamref name="T"/> this is a plain cast and follows the standard ADO.NET
    /// behaviour.
    /// </summary>
    public override T GetFieldValue<T>(int ordinal)
    {
        if (FieldValueDispatcher<T>.RequiresMultidimConversion)
        {
            var raw = GetValue(ordinal);

            // Pre-check the column-level type-mismatch cases so the message names the column
            // ordinal directly. Structural-depth and leaf-type mismatches caught by the helper
            // are wrapped in the catch below to add the same ordinal context.
            if (raw is null || raw is DBNull || raw is not IList)
            {
                throw new InvalidCastException(
                    $"Column [{ordinal}] value '{raw?.GetType().FullName ?? "null"}' " +
                    $"cannot be converted to '{typeof(T)}'.");
            }
            try
            {
                return MultiDimArrayHelper.ToMultidimensional<T>(raw);
            }
            catch (InvalidCastException ex)
            {
                // Helper-detected type mismatch (shallow/deep source, leaf-type mismatch, null
                // into value-type leaf). Add column context so callers see ordinal + T alongside
                // the helper's structural detail.
                throw new InvalidCastException(
                    $"Column [{ordinal}] cannot be converted to '{typeof(T)}': {ex.Message}", ex);
            }
        }

        var value = (T)CurrentRow[ordinal];
        if (readValueConverter != null)
            return readValueConverter.ConvertValue<T>(value, FieldNames[ordinal], columnTypeNames[ordinal]);
        return value;
    }

    /// <summary>
    /// Per-<typeparamref name="T"/> cached predicate driving <see cref="GetFieldValue{T}"/>'s
    /// dispatch. The .NET runtime instantiates this generic exactly once per closed
    /// <typeparamref name="T"/>, so the <c>typeof</c> + <c>IsArray</c> + <c>GetArrayRank</c>
    /// work runs once on first use; thereafter the hot path is a single static <see cref="bool"/>
    /// load and branch.
    /// </summary>
    private static class FieldValueDispatcher<T>
    {
        public static readonly bool RequiresMultidimConversion =
            typeof(T).IsArray && typeof(T).GetArrayRank() >= 2;
    }

    // Custom extension
    public T GetFieldValue<T>(string name) => GetFieldValue<T>(GetOrdinal(name));

    public override DataTable GetSchemaTable() => SchemaDescriber.DescribeSchema(this);

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

    // Custom extension
    public ushort GetUInt16(int ordinal) => (ushort)GetValue(ordinal);

    // Custom extension
    public uint GetUInt32(int ordinal) => (uint)GetValue(ordinal);

    // Custom extension
    public ulong GetUInt64(int ordinal) => (ulong)GetValue(ordinal);

    // Custom extension
    public IPAddress GetIPAddress(int ordinal) => (IPAddress)GetValue(ordinal);

    // Custom extension
    public ITuple GetTuple(int ordinal) => (ITuple)GetValue(ordinal);

    // Custom extension
    public sbyte GetSByte(int ordinal) => (sbyte)GetValue(ordinal);

    // Custom extension
    public BigInteger GetBigInteger(int ordinal) => (BigInteger)GetValue(ordinal);

    public override bool Read()
    {
        if (reader.PeekChar() == -1)
            return false; // End of stream reached

        var count = RawTypes.Length;
        var data = CurrentRow;

        try
        {
            for (var i = 0; i < count; i++)
            {
                var rawType = RawTypes[i];
                data[i] = rawType.Read(reader);
            }
            return true;
        }
        catch (EndOfStreamException) when (exceptionTagStream != null)
        {
            var serverEx = exceptionTagStream.TryExtractMidStreamException();
            if (serverEx != null)
                throw serverEx;
            throw;
        }
    }

#pragma warning disable CA2215 // Dispose methods should call base class dispose
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            httpResponse?.Dispose();
            reader?.Dispose();
        }
    }
#pragma warning restore CA2215 // Dispose methods should call base class dispose

    private static (string[], ClickHouseType[], string[]) ReadHeaders(ExtendedBinaryReader reader, TypeSettings settings, bool captureRawTypeNames)
    {
        if (reader.PeekChar() == -1)
        {
            return ([], [], []);
        }

        var count = reader.Read7BitEncodedInt();

        // Check for GZip marker: 0x1F (31) as column count, followed by 0x8B
        // This happens when compression is misconfigured
        if (count == 0x1F && reader.PeekChar() == 0x8B)
        {
            throw new InvalidOperationException("ClickHouse server returned compressed data but HttpClient did not decompress it. Ensure HttpClientHandler.AutomaticDecompression is set to DecompressionMethods.All or DecompressionMethods.GZip.");
        }

        var names = new string[count];
        var types = new ClickHouseType[count];
        var rawTypeNames = captureRawTypeNames ? new string[count] : null;

        for (var i = 0; i < count; i++)
        {
            names[i] = reader.ReadString();
        }

        for (var i = 0; i < count; i++)
        {
            var chType = reader.ReadString();
            if (captureRawTypeNames)
                rawTypeNames[i] = chType;
            types[i] = TypeConverter.ParseClickHouseType(chType, settings);
        }
        return (names, types, rawTypeNames);
    }

    public bool MoveNext() => Read();

    public void Reset() => throw new NotSupportedException();

    public override IEnumerator GetEnumerator() => this;

    IEnumerator<IDataReader> IEnumerable<IDataReader>.GetEnumerator() => this;

    public IDataReader Current => this;

    object IEnumerator.Current => this;
}
