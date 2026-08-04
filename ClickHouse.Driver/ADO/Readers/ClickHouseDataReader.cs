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
using ClickHouse.Driver.Http;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Poco;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.ADO.Readers;

// TODO: implement IDbColumnSchemaGenerator
public class ClickHouseDataReader : DbDataReader, IEnumerator<IDataReader>, IEnumerable<IDataReader>, IDataRecord
{
    private const int DefaultBufferSize = ClickHouseDefaults.ReadBufferSize;
    private const string ExceptionTagHeader = "X-ClickHouse-Exception-Tag";

    private readonly HttpResponseMessage httpResponse; // Used to dispose at the end of reader
    private readonly ExtendedBinaryReader reader;
    private readonly PooledReadBufferStream pooledReadBuffer; // Returns its pooled buffer on dispose
    private readonly Stream decompressor; // Can be null: only set when the response body was transport-compressed
    private readonly ExceptionTagAwareStream exceptionTagStream; // Can be null
    private readonly IReadValueConverter readValueConverter; // Can be null
    private readonly string[] columnTypeNames; // Raw server-sent type strings; null when no converter
    private readonly PocoTypeRegistry pocoRegistry;
    private readonly Dictionary<Type, object> bindingPlanCache = new();

    // Per-column typed storage for the current row; replaces the old shared object[] buffer. Built on the
    // first Read() and mutated in place by every one after it.
    //
    // Deliberately not built in the constructor. QueryAsync<T>'s box-free POCO path materializes straight
    // from the stream through TryMaterializeNextRow and never touches a slot, so constructing them eagerly
    // would allocate one permanently dead object per column on the driver's primary read API. Empty result
    // sets and readers opened only for their metadata likewise never pay for storage they do not use.
    //
    // Built once and never nulled again, so `hasCurrentRow` implies non-null. That is the whole safety
    // argument, and every value accessor establishes it by going through Slot(). GetValues is the one
    // exception — it indexes this array directly and carries its own copy of the guard, so if that guard is
    // ever "simplified" away it produces a NullReferenceException rather than the intended
    // InvalidOperationException.
    private ColumnSlot[] slots;
    private bool hasCurrentRow;

    private ClickHouseDataReader(HttpResponseMessage httpResponse, ExtendedBinaryReader reader, PooledReadBufferStream pooledReadBuffer, string[] names, ClickHouseType[] types, string[] rawTypeNames, PocoTypeRegistry pocoRegistry, ExceptionTagAwareStream exceptionTagStream = null, IReadValueConverter readValueConverter = null, Stream decompressor = null)
    {
        this.httpResponse = httpResponse ?? throw new ArgumentNullException(nameof(httpResponse));
        this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
        this.pooledReadBuffer = pooledReadBuffer;
        this.decompressor = decompressor;
        this.exceptionTagStream = exceptionTagStream;
        this.readValueConverter = readValueConverter;
        // pocoRegistry may be null when the reader is used purely for ADO.NET-style access
        // (GetValue / typed accessors) — MapTo<T> guards against null and surfaces the standard
        // "not registered" error.
        this.pocoRegistry = pocoRegistry;
        RawTypes = types;
        FieldNames = names;

        if (readValueConverter != null)
            columnTypeNames = rawTypeNames;
    }

    internal static Task<ClickHouseDataReader> FromHttpResponseAsync(HttpResponseMessage httpResponse, TypeSettings settings)
        => FromHttpResponseAsync(httpResponse, settings, pocoRegistry: null);

    internal static async Task<ClickHouseDataReader> FromHttpResponseAsync(HttpResponseMessage httpResponse, TypeSettings settings, PocoTypeRegistry pocoRegistry, int readBufferSize = DefaultBufferSize, IReadValueConverter readValueConverter = null)
    {
        if (httpResponse is null) throw new ArgumentNullException(nameof(httpResponse));
        if (readBufferSize < 1) throw new ArgumentOutOfRangeException(nameof(readBufferSize), readBufferSize, "Read buffer size must be greater than zero");

        // Extract exception tag from header if present
        string exceptionTag = null;
        if (httpResponse.Headers.TryGetValues(ExceptionTagHeader, out var tagValues))
            exceptionTag = System.Linq.Enumerable.FirstOrDefault(tagValues);

        ExtendedBinaryReader reader = null;
        ExceptionTagAwareStream exceptionStream = null;
        PooledReadBufferStream buffered = null;
        Stream decompressingStream = null;
        try
        {
            var rawStream = await httpResponse.Content.ReadAsStreamAsync().ConfigureAwait(false);

            // Decompression sits INNERMOST, directly on the transport stream, so that everything layered
            // above it — the mid-stream exception scanner and the pooled read buffer alike — sees
            // PLAINTEXT. Driven by the response's Content-Encoding, never by what we asked for.
            // leaveOpen: true because httpResponse owns rawStream and disposes it.
            var plaintext = ResponseDecompression.Wrap(rawStream, httpResponse, leaveOpen: true);

            // Reference-equal means the body needed no decoding, so there is no extra stream to own.
            decompressingStream = ReferenceEquals(plaintext, rawStream) ? null : plaintext;

            // Conditionally record recent bytes for mid-stream exception detection. This sits *below*
            // the buffering stream on purpose: there it observes one read per buffer refill instead of
            // one per scalar the reader decodes, which is the difference between recording a few bytes
            // millions of times and recording a 64 KiB block a few times. The ring buffer then holds the
            // last bytes received rather than the last bytes consumed — equivalent for this purpose,
            // because TryExtractMidStreamException is only consulted once a read has failed with an
            // IOException, by which point the transport is drained and the server's trailing marker has
            // been recorded.
            // It must stay *above* the decompressor, though: the server writes its exception marker into
            // the response body, so the marker only exists in the decoded plaintext.
            // leaveOpen: true because the stream below is owned by either httpResponse (uncompressed) or
            // this reader via decompressingStream (compressed).
            Stream bufferedInner = plaintext;
            if (!string.IsNullOrEmpty(exceptionTag))
            {
                exceptionStream = new ExceptionTagAwareStream(plaintext, exceptionTag, leaveOpen: true);
                bufferedInner = exceptionStream;
            }

            // Buffer reads through a pooled buffer (rented from ArrayPool) rather than BufferedStream's
            // fresh per-query array. leaveOpen: true because the streams below are owned elsewhere;
            // the reader disposes this wrapper explicitly to return the buffer (the BinaryReader ->
            // PeekableStreamWrapper chain does not propagate Dispose to inner streams).
            // Its Read may return fewer bytes than requested; the ExtendedBinaryReader below loops to
            // satisfy exact-count reads.
            buffered = new PooledReadBufferStream(bufferedInner, readBufferSize, leaveOpen: true);

            reader = new ExtendedBinaryReader(buffered);
            var (names, types, rawTypeNames) = ReadHeaders(reader, settings, readValueConverter != null);
            return new ClickHouseDataReader(httpResponse, reader, buffered, names, types, rawTypeNames, pocoRegistry, exceptionStream, readValueConverter, decompressingStream);
        }
        catch (Exception)
        {
            httpResponse?.Dispose();
            reader?.Dispose();
            buffered?.Dispose(); // returns the rented buffer if we failed before handing it to the reader
            decompressingStream?.Dispose();
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

    protected string[] FieldNames { get; set; }

    private protected ClickHouseType[] RawTypes { get; set; }

    /// <summary>
    /// Shared body for the strict typed accessors — every one of which was <c>(T)GetValue(ordinal)</c> before
    /// column slots, and keeps exactly that meaning here. This is the path compiled ORM mappers drive
    /// (linq2db registers <c>GetInt64</c>/<c>GetDouble</c>/<c>GetDateTime</c>/… and inlines them per column
    /// per row), so it is where the boxing elimination is worth the most.
    /// </summary>
    /// <remarks>
    /// With an <see cref="IReadValueConverter"/> configured the accessor keeps routing through
    /// <see cref="GetValue"/>, so it still calls <c>ConvertValue(object, …)</c> exactly as it did before.
    /// De-boxing here would mean calling <c>ConvertValue&lt;T&gt;</c> instead, which is observable to a
    /// converter whose two overloads disagree — the generic one matching on <c>typeof(T)</c>, the object one
    /// on the value's runtime type. Not worth a silent semantic change for the rare converter case;
    /// everyone else gets the fast path. <see cref="GetFieldValue{T}"/> keeps its own routing and stays on
    /// <c>ConvertValue&lt;T&gt;</c>, which is the overload it already called — so between the two paths every
    /// converter overload is still reached, exactly as before.
    /// </remarks>
    private T GetTypedValue<T>(int ordinal)
        => readValueConverter == null ? GetSlotValue<T>(ordinal) : (T)GetValue(ordinal);

    // Unlike its neighbours this one coerces rather than casts, so only an exact Bool column can take the
    // fast path; anything else keeps Convert.ToBoolean's widening (and its exception messages). A NULL cell
    // falls through as well, so Convert.ToBoolean(DBNull.Value) still throws exactly as it did.
    public override bool GetBoolean(int ordinal)
    {
        if (readValueConverter == null)
        {
            var slot = Slot(ordinal);
            if (slot is ValueSlot<bool> boolSlot)
                return boolSlot.Value;
            if (slot is NullableSlot<bool> nullableSlot && nullableSlot.HasValue)
                return nullableSlot.Value;
        }

        return Convert.ToBoolean(GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    public override byte GetByte(int ordinal) => GetTypedValue<byte>(ordinal);

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

    // No ClickHouse type reads as char, so there is no slot to hit — left on the boxed cast.
    public override char GetChar(int ordinal) => (char)GetValue(ordinal);

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

    public override string GetDataTypeName(int ordinal) => GetClickHouseType(ordinal).ToString();

    public override DateTime GetDateTime(int ordinal) => GetTypedValue<DateTime>(ordinal);

    // Box-free by construction once GetDateTime is: CoerceToDateTimeOffset has a DateTime overload.
    public virtual DateTimeOffset GetDateTimeOffset(int ordinal) => GetEffectiveClickHouseType(ordinal) is AbstractDateTimeType adt ?
        adt.CoerceToDateTimeOffset(GetDateTime(ordinal)) : throw new InvalidCastException();

    public override decimal GetDecimal(int ordinal)
    {
        if (readValueConverter == null)
        {
            // Which of these two representations a Decimal column resolves to is the UseBigDecimal setting's
            // doing; both reach the same decimal without a box, nullable or not. A NULL cell falls through to
            // the boxed path below, where casting DBNull.Value throws exactly as it did.
            var slot = Slot(ordinal);
            if (slot is ValueSlot<decimal> decimalSlot)
                return decimalSlot.Value;
            if (slot is NullableSlot<decimal> nullableDecimalSlot && nullableDecimalSlot.HasValue)
                return nullableDecimalSlot.Value;
            if (slot is ValueSlot<ClickHouseDecimal> bigDecimalSlot)
                return bigDecimalSlot.Value.ToDecimal(CultureInfo.InvariantCulture);
            if (slot is NullableSlot<ClickHouseDecimal> nullableBigDecimalSlot && nullableBigDecimalSlot.HasValue)
                return nullableBigDecimalSlot.Value.ToDecimal(CultureInfo.InvariantCulture);
        }

        var value = GetValue(ordinal);
        return value is ClickHouseDecimal clickHouseDecimal ? clickHouseDecimal.ToDecimal(CultureInfo.InvariantCulture) : (decimal)value;
    }

    public override double GetDouble(int ordinal) => GetTypedValue<double>(ordinal);

    public override Type GetFieldType(int ordinal)
    {
        var rawType = RawTypes[ordinal];
        return rawType is NullableType nt ? nt.UnderlyingType.FrameworkType : rawType.FrameworkType;
    }

    public override float GetFloat(int ordinal) => GetTypedValue<float>(ordinal);

    public override Guid GetGuid(int ordinal) => GetTypedValue<Guid>(ordinal);

    public override short GetInt16(int ordinal) => GetTypedValue<short>(ordinal);

    public override int GetInt32(int ordinal) => GetTypedValue<int>(ordinal);

    public override long GetInt64(int ordinal) => GetTypedValue<long>(ordinal);

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

    // Deliberately narrower than the other accessors: only a non-nullable String column short-circuits. Every
    // other shape keeps ToString()'s coercion, including the quirk that a NULL cell yields "" rather than
    // null, because DBNull.Value.ToString() is the empty string.
    public override string GetString(int ordinal)
        => readValueConverter == null && Slot(ordinal) is ValueSlot<string> stringSlot
            ? stringSlot.Value
            : GetValue(ordinal)?.ToString();

    /// <summary>
    /// The one boxing entry point on the read path. Boxes lazily, per call, so a query that projects ten
    /// columns and reads two pays for two — where the old <c>object[]</c> buffer boxed all ten during
    /// <see cref="Read"/> regardless.
    /// </summary>
    /// <remarks>
    /// Consequence of boxing per call rather than once per row: two <c>GetValue(i)</c> calls on the same
    /// value-type cell now return two distinct boxes. They compare equal by <see cref="object.Equals(object)"/>
    /// (the ADO.NET-relevant comparison) but no longer by <see cref="object.ReferenceEquals"/>.
    /// </remarks>
    public override object GetValue(int ordinal)
    {
        var value = Slot(ordinal).GetBoxed();
        return readValueConverter == null
            ? value
            : readValueConverter.ConvertValue(value, FieldNames[ordinal], columnTypeNames[ordinal]);
    }

    public override int GetValues(object[] values)
    {
        if (!hasCurrentRow)
            ThrowNoCurrentRow();

        var count = Math.Min(slots.Length, values.Length);

        if (readValueConverter != null)
        {
            for (var i = 0; i < count; i++)
                values[i] = readValueConverter.ConvertValue(slots[i].GetBoxed(), FieldNames[i], columnTypeNames[i]);
        }
        else
        {
            for (var i = 0; i < count; i++)
                values[i] = slots[i].GetBoxed();
        }

        return count;
    }

    public override bool IsDBNull(int ordinal)
        // Asks the slot directly rather than going through GetValue, for two reasons: a configured
        // IReadValueConverter must not run during a null check (it could throw, do expensive work, or
        // change the nullness of the result), and a null check has no business materializing a box.
        => Slot(ordinal).IsNull;

    /// <summary>
    /// The single gate every value accessor passes through. Column metadata (<see cref="FieldCount"/>,
    /// <see cref="GetName"/>, <see cref="GetFieldType"/>, <see cref="GetSchemaTable"/>, …) is available
    /// without a current row and is deliberately not gated.
    /// </summary>
    /// <remarks>
    /// Slots hold typed storage, so before the first <see cref="Read"/> a non-nullable value column would
    /// otherwise read back as a perfectly plausible <c>0</c> / <c>false</c> / <c>Guid.Empty</c> rather than
    /// as nothing. Answering a question the reader cannot yet answer, with a value indistinguishable from
    /// real data, is the one failure mode worth spending a branch to prevent — so this reports the mistake
    /// instead, matching what <c>SqlClient</c> and the rest of ADO.NET do.
    /// </remarks>
    private ColumnSlot Slot(int ordinal)
    {
        if (!hasCurrentRow)
            ThrowNoCurrentRow();

        return slots[ordinal];
    }

    // Separate and non-inlined so the check above stays small enough for the JIT to inline into the hot
    // accessors.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowNoCurrentRow()
        => throw new InvalidOperationException(
            "The reader has no current row. Call Read() and check that it returned true before reading " +
            "column values. Column metadata (FieldCount, GetName, GetFieldType, GetSchemaTable) is " +
            "available without a current row.");

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

        var value = GetSlotValue<T>(ordinal);
        if (readValueConverter != null)
            return readValueConverter.ConvertValue<T>(value, FieldNames[ordinal], columnTypeNames[ordinal]);
        return value;
    }

    /// <summary>
    /// Extracts the current row's column value as <typeparamref name="T"/>, without boxing where the slot
    /// already holds exactly that type.
    /// </summary>
    /// <remarks>
    /// <para>The two sealed-class checks are ordered by cost. For a value-typed <typeparamref name="T"/> the
    /// runtime JITs a dedicated instantiation, so each is a plain <c>isinst</c> against a known method table —
    /// measured at 1.5–2.4x the cost of the unbox it replaces, against roughly 10 ns per column of decode.
    /// A generic <c>IValueGetter&lt;T&gt;</c> interface implemented twice would let one slot serve both
    /// <c>long</c> and <c>long?</c>, but it goes through the shared-generics dictionary and measured
    /// 3.5–5.5x instead — so <c>T = U?</c> is deliberately left to the boxed fallback.</para>
    ///
    /// <para>The fallback is the pre-slot expression verbatim, which is what preserves the exact-type
    /// strictness callers depend on: <c>GetFieldValue&lt;long&gt;</c> over an <c>Int32</c> column throws, it
    /// does not widen, and reading a NULL as a non-nullable <typeparamref name="T"/> throws the runtime's
    /// own "cannot cast DBNull" <see cref="InvalidCastException"/> exactly as before.</para>
    /// </remarks>
    private T GetSlotValue<T>(int ordinal)
    {
        var slot = Slot(ordinal);

        if (slot is ValueSlot<T> valueSlot)
            return valueSlot.Value;

        // A Nullable(T) column holds the underlying T, so this also serves GetFieldValue<long> over
        // Nullable(Int64) — and when the cell is null it falls through to the box, which throws.
        if (slot is NullableSlot<T> nullableSlot && nullableSlot.HasValue)
            return nullableSlot.Value;

        return (T)slot.GetBoxed();
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
    public ushort GetUInt16(int ordinal) => GetTypedValue<ushort>(ordinal);

    // Custom extension
    public uint GetUInt32(int ordinal) => GetTypedValue<uint>(ordinal);

    // Custom extension
    public ulong GetUInt64(int ordinal) => GetTypedValue<ulong>(ordinal);

    // Custom extension
    public IPAddress GetIPAddress(int ordinal) => GetTypedValue<IPAddress>(ordinal);

    // Custom extension. Tuple columns have no typed slot, so this stays on the boxed cast.
    public ITuple GetTuple(int ordinal) => (ITuple)GetValue(ordinal);

    // Custom extension
    public sbyte GetSByte(int ordinal) => GetTypedValue<sbyte>(ordinal);

    // Custom extension
    public BigInteger GetBigInteger(int ordinal) => GetTypedValue<BigInteger>(ordinal);

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

            // Reading through GetValue makes sure we also go through the value converter (if it exists)
            var value = GetValue(col);

            if (value is null || value is DBNull)
            {
                if (binding.PropInfo.CanAssignNull)
                {
                    binding.Setter(instance, null);
                    continue;
                }

                throw new InvalidOperationException(PocoColumnAssignment.BuildAssignmentErrorMessage(
                    typeof(T), binding.PropInfo, FieldNames[col], RawTypes[col].ToString(), null));
            }

            try
            {
                binding.Setter(instance, value);
            }
            catch (InvalidCastException)
            {
                throw new InvalidOperationException(PocoColumnAssignment.BuildAssignmentErrorMessage(
                    typeof(T), binding.PropInfo, FieldNames[col], RawTypes[col].ToString(), value.GetType()));
            }
        }

        return instance;
    }

    /// <summary>
    /// If <typeparamref name="T"/> is registered for read and the current result shape has a full box-free
    /// fast path, returns the per-wire-column materializer delegates and the constructor; otherwise returns
    /// false and the caller should use the <see cref="Read"/> + <see cref="MapTo{T}"/> loop. Disabled when a
    /// read value converter is present, since the boxed path routes every value through it (see
    /// <see cref="MapTo{T}"/>), and that conversion must not be bypassed.
    /// </summary>
    internal bool TryGetRowMaterializer<T>(out Action<ExtendedBinaryReader, T>[] materializers, out Func<T> constructor)
        where T : class
    {
        materializers = null;
        constructor = null;

        if (readValueConverter != null || pocoRegistry == null)
            return false;

        var mapping = pocoRegistry.GetReadMapping<T>();
        if (mapping == null)
            return false;

        var built = pocoRegistry.GetOrBuildRowReaders<T>(FieldNames, RawTypes, mapping);
        if (built == null)
            return false;

        materializers = built;
        constructor = mapping.Constructor;
        return true;
    }

    /// <summary>
    /// Reads and materializes the next row straight from the stream via the fast-path delegates, bypassing
    /// the reader's column slots entirely — they are never even allocated on this path. Returns false at end
    /// of stream.
    /// Mirrors <see cref="Read"/>'s mid-stream server-exception handling. The delegates consume every wire
    /// column in order, so the stream stays aligned even for columns the POCO does not map.
    /// </summary>
    internal bool TryMaterializeNextRow<T>(Action<ExtendedBinaryReader, T>[] materializers, Func<T> constructor, out T value)
        where T : class
    {
        if (reader.PeekChar() == -1)
        {
            hasCurrentRow = false;
            value = null;
            return false;
        }

        try
        {
            var instance = constructor();
            for (var i = 0; i < materializers.Length; i++)
                materializers[i](reader, instance);
            value = instance;
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
        if (!PocoColumnAssignment.IsAssignable(propInfo, colType))
        {
            var colFrameworkType = colType.FrameworkType;
            var unwrappedColFrameworkType = Nullable.GetUnderlyingType(colFrameworkType) ?? colFrameworkType;
            throw new InvalidOperationException(PocoColumnAssignment.BuildAssignmentErrorMessage(
                pocoType, propInfo, FieldNames[columnOrdinal], colType.ToString(), unwrappedColFrameworkType));
        }
    }

    public override bool Read()
    {
        // Clear before the per-column loop so a mid-row throw cannot leave a stale
        // row visible to MapTo<T> if the caller catches and continues.
        hasCurrentRow = false;
        try
        {
            // PeekChar is inside the try: a mid-stream failure truncates the body at a row
            // boundary too, so the end-of-stream probe can itself hit the truncation.
            if (reader.PeekChar() == -1)
                return false; // End of stream reached

            // Built on the first row rather than in the ctor: the POCO fast path materializes straight
            // from the stream and never touches a slot, so eager construction would allocate a
            // permanently dead object per column on the primary read API. An empty result never gets here.
            var columns = slots ??= CreateSlots();
            for (var i = 0; i < columns.Length; i++)
            {
                columns[i].Read(reader);
            }
            hasCurrentRow = true;
            return true;
        }
        // A mid-stream server failure truncates the HTTP body; reading past the truncation surfaces
        // as an IOException — EndOfStreamException for a buffered body, but HttpIOException
        // ("response ended prematurely") for a live streamed response. Both derive from IOException.
        // When the server tagged the response (exceptionTagStream != null) the in-band exception block
        // is captured in the ring buffer, so convert it to the real server error; otherwise re-throw.
        catch (IOException) when (exceptionTagStream != null)
        {
            var serverEx = exceptionTagStream.TryExtractMidStreamException();
            if (serverEx != null)
                throw serverEx;
            throw;
        }
    }

    // Runs at most once per reader, so it is kept out of Read() to leave that method small enough for the
    // JIT to treat the slot loop as the hot path it is.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ColumnSlot[] CreateSlots()
    {
        var created = new ColumnSlot[RawTypes.Length];
        for (var i = 0; i < created.Length; i++)
            created[i] = ColumnSlotFactory.Create(RawTypes[i]);
        return created;
    }

#pragma warning disable CA2215 // Dispose methods should call base class dispose
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            httpResponse?.Dispose();
            reader?.Dispose();
            // Explicit: the BinaryReader -> PeekableStreamWrapper chain does not propagate Dispose to the
            // inner buffering stream, so return its pooled buffer here (idempotent if already disposed).
            pooledReadBuffer?.Dispose();
            // Innermost wrapper, disposed last. Null unless the body was transport-compressed; created
            // with leaveOpen: true, so this releases the codec's own state without touching the
            // transport stream that httpResponse owns.
            decompressor?.Dispose();
        }
    }
#pragma warning restore CA2215 // Dispose methods should call base class dispose

    private static (string[], ClickHouseType[], string[]) ReadHeaders(ExtendedBinaryReader reader, TypeSettings settings, bool captureRawTypeNames)
    {
        if (reader.PeekChar() == -1)
        {
            return ([], [], []);
        }

        // No magic-byte sniffing here: a still-compressed body is detected up front from the response's
        // Content-Encoding header and either decoded by ResponseDecompression or rejected with an
        // actionable error naming the codec — instead of being misread as a column count and surfacing
        // as a bogus type-parse failure.
        var count = reader.Read7BitEncodedInt();

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
