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
using ClickHouse.Driver.Poco;
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
    private readonly PocoTypeRegistry pocoRegistry;
    private readonly Dictionary<Type, object> bindingPlanCache = new();
    private bool hasCurrentRow;

    private ClickHouseDataReader(HttpResponseMessage httpResponse, ExtendedBinaryReader reader, string[] names, ClickHouseType[] types, PocoTypeRegistry pocoRegistry, ExceptionTagAwareStream exceptionTagStream = null)
    {
        this.httpResponse = httpResponse ?? throw new ArgumentNullException(nameof(httpResponse));
        this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        this.exceptionTagStream = exceptionTagStream;
        // pocoRegistry may be null when the reader is used purely for ADO.NET-style access
        // (GetValue / typed accessors) — MapTo<T> guards against null and surfaces the standard
        // "not registered" error.
        this.pocoRegistry = pocoRegistry;
        RawTypes = types;
        FieldNames = names;
        CurrentRow = new object[FieldNames.Length];
    }

    internal static Task<ClickHouseDataReader> FromHttpResponseAsync(HttpResponseMessage httpResponse, TypeSettings settings)
        => FromHttpResponseAsync(httpResponse, settings, pocoRegistry: null);

    internal static async Task<ClickHouseDataReader> FromHttpResponseAsync(HttpResponseMessage httpResponse, TypeSettings settings, PocoTypeRegistry pocoRegistry)
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
            var (names, types) = ReadHeaders(reader, settings);
            return new ClickHouseDataReader(httpResponse, reader, names, types, pocoRegistry, exceptionStream);
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

    public override object GetValue(int ordinal) => CurrentRow[ordinal];

    public override int GetValues(object[] values)
    {
        if (CurrentRow == null)
        {
            throw new InvalidOperationException();
        }

        CurrentRow.CopyTo(values, 0);
        return CurrentRow.Length;
    }

    public override bool IsDBNull(int ordinal)
    {
        var value = GetValue(ordinal);
        return value is DBNull || value is null;
    }

    public override bool NextResult() => false;

    public override void Close() => Dispose();

    public override T GetFieldValue<T>(int ordinal) => (T)GetValue(ordinal);

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

    /// <summary>
    /// Materializes the current row into a new instance of <typeparamref name="T"/>.
    /// Does not call <see cref="Read"/> and does not advance the reader.
    /// </summary>
    /// <typeparam name="T">The registered POCO type. Must have been registered via
    /// <c>RegisterPocoType&lt;T&gt;</c> on the owning client or connection.</typeparam>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <typeparamref name="T"/> is not registered, or if a column value cannot
    /// be assigned to the corresponding property under the strict v1 assignment rules
    /// (no conversions, no widening, no enum coercion).
    /// </exception>
    public T MapTo<T>()
        where T : class
    {
        if (!hasCurrentRow)
        {
            throw new InvalidOperationException(
                "MapTo<T> requires a current row. Call Read() and verify it returned true before calling MapTo<T>.");
        }

        var mapping = pocoRegistry?.GetReadMapping<T>()
            ?? throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not registered for POCO read. " +
                $"Call RegisterPocoType<{typeof(T).Name}>() on the client or connection first.");

        var plan = GetOrBuildBindingPlan(mapping);
        var instance = mapping.Constructor();
        for (var i = 0; i < plan.Length; i++)
        {
            var col = plan[i].ColumnOrdinal;
            var binding = plan[i].Binding;
            var value = CurrentRow[col];

            if (value is null || value is DBNull)
            {
                if (binding.PropInfo.CanAssignNull)
                {
                    binding.Setter(instance, null);
                    continue;
                }

                throw new InvalidOperationException(BuildAssignmentErrorMessage(
                    typeof(T), binding.PropInfo, FieldNames[col], RawTypes[col].ToString(), null));
            }

            try
            {
                binding.Setter(instance, value);
            }
            catch (InvalidCastException)
            {
                throw new InvalidOperationException(BuildAssignmentErrorMessage(
                    typeof(T), binding.PropInfo, FieldNames[col], RawTypes[col].ToString(), value.GetType()));
            }
        }

        return instance;
    }

    private (int ColumnOrdinal, ColumnBinding<T> Binding)[] GetOrBuildBindingPlan<T>(PocoReadMapping<T> mapping)
        where T : class
    {
        if (bindingPlanCache.TryGetValue(typeof(T), out var cached))
            return ((int, ColumnBinding<T>)[])cached;

        var matched = new List<(int, ColumnBinding<T>)>(mapping.Bindings.Count);
        for (var i = 0; i < FieldNames.Length; i++)
        {
            if (!mapping.Bindings.TryGetValue(FieldNames[i], out var binding))
                continue;

            ValidateBinding(typeof(T), binding.PropInfo, i);
            matched.Add((i, binding));
        }
        var plan = matched.ToArray();
        bindingPlanCache[typeof(T)] = plan;
        return plan;
    }

    /// <summary>
    /// Fail-fast static check at plan build: if a column's declared <see cref="ClickHouseType.FrameworkType"/>
    /// is not assignable to the target property's CLR type (or its nullable underlying type), throw before
    /// any rows are materialized into POCOs so users see the diagnostic up front.
    /// Polymorphic columns (FrameworkType=object — e.g. Variant/Dynamic/JSON/Object) skip the static check;
    /// their actual per-row CLR type can vary, so any mismatch surfaces via the per-row catch in MapTo{T}.
    /// </summary>
    private void ValidateBinding(Type pocoType, PocoPropertyInfo propInfo, int columnOrdinal)
    {
        var colType = RawTypes[columnOrdinal];
        var colFrameworkType = colType.FrameworkType;
        var unwrappedColFrameworkType = Nullable.GetUnderlyingType(colFrameworkType) ?? colFrameworkType;

        if (unwrappedColFrameworkType == typeof(object))
            return;

        var assignable = propInfo.PropertyType.IsAssignableFrom(unwrappedColFrameworkType)
            || (propInfo.NullableUnderlyingType != null && propInfo.NullableUnderlyingType.IsAssignableFrom(unwrappedColFrameworkType));

        if (!assignable)
        {
            throw new InvalidOperationException(BuildAssignmentErrorMessage(
                pocoType, propInfo, FieldNames[columnOrdinal], colType.ToString(), unwrappedColFrameworkType));
        }
    }

    private static string BuildAssignmentErrorMessage(
        Type targetType,
        PocoPropertyInfo propInfo,
        string columnName,
        string clickHouseType,
        Type returnedType)
    {
        var returnedDescription = returnedType is null ? "null" : returnedType.FullName;
        return
            $"Cannot map ClickHouse column '{columnName}' ({clickHouseType}) to property " +
            $"{targetType.Name}.{propInfo.PropertyName} ({propInfo.PropertyType.FullName}). " +
            $"The reader returned {returnedDescription}, which is not assignable to {propInfo.PropertyType.FullName}.";
    }

    public override bool Read()
    {
        if (reader.PeekChar() == -1)
        {
            hasCurrentRow = false;
            return false; // End of stream reached
        }

        var count = RawTypes.Length;
        var data = CurrentRow;

        // Clear before the per-column loop so a mid-row throw cannot leave a stale
        // CurrentRow visible to MapTo<T> if the caller catches and continues.
        hasCurrentRow = false;
        try
        {
            for (var i = 0; i < count; i++)
            {
                var rawType = RawTypes[i];
                data[i] = rawType.Read(reader);
            }
            hasCurrentRow = true;
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

    private static (string[], ClickHouseType[]) ReadHeaders(ExtendedBinaryReader reader, TypeSettings settings)
    {
        if (reader.PeekChar() == -1)
        {
            return ([], []);
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

        for (var i = 0; i < count; i++)
        {
            names[i] = reader.ReadString();
        }

        for (var i = 0; i < count; i++)
        {
            var chType = reader.ReadString();
            types[i] = TypeConverter.ParseClickHouseType(chType, settings);
        }
        return (names, types);
    }

    public bool MoveNext() => Read();

    public void Reset() => throw new NotSupportedException();

    public override IEnumerator GetEnumerator() => this;

    IEnumerator<IDataReader> IEnumerable<IDataReader>.GetEnumerator() => this;

    public IDataReader Current => this;

    object IEnumerator.Current => this;
}
