using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.ADO;

[TestFixture]
public class ReadValueConverterTests : AbstractConnectionTestFixture
{
    // ==========================================
    // GetValue (boxed) tests
    // ==========================================

    [Test]
    public async Task GetValue_WithConverter_ShouldTransformDateTime()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00')");
        ClassicAssert.IsTrue(reader.Read());

        var value = (DateTime)reader.GetValue(0);
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(value.Hour, Is.EqualTo(12));
    }

    [Test]
    public async Task GetValue_WithConverter_ShouldNotAffectNonTargetedTypes()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toInt32(42), 'hello'");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetValue(0), Is.EqualTo(42));
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));
    }

    // ==========================================
    // GetFieldValue<T> (generic) tests
    // ==========================================

    [Test]
    public async Task GetFieldValue_WithConverter_ShouldTransformDateTime()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00')");
        ClassicAssert.IsTrue(reader.Read());

        var value = reader.GetFieldValue<DateTime>(0);
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    [Test]
    public async Task GetFieldValue_NullableVsPlainTarget_DispatchByExactTypeOfT()
    {
        // The generic GetFieldValue<T> path dispatches on the exact typeof(T). A converter keyed on
        // typeof(DateTime) (DateTimeKindConverter) therefore fires for GetFieldValue<DateTime> but NOT
        // for GetFieldValue<DateTime?> (typeof(DateTime?) != typeof(DateTime)). The boxed GetValue path,
        // which dispatches on the runtime type, still applies it — so the two paths differ on Nullable(T).
        // This is the documented contract on IReadValueConverter.ConvertValue<T>; converters wanting
        // consistency must handle Nullable<U> themselves (DictionaryReadValueConverter does).
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT CAST('2025-01-15 12:00:00' AS Nullable(DateTime))");
        ClassicAssert.IsTrue(reader.Read());

        // Plain target: typeof(T) == typeof(DateTime) → converter fires.
        Assert.That(reader.GetFieldValue<DateTime>(0).Kind, Is.EqualTo(DateTimeKind.Utc));
        // Nullable target: typeof(T) == typeof(DateTime?) → converter is skipped on the generic path.
        Assert.That(reader.GetFieldValue<DateTime?>(0).Value.Kind, Is.Not.EqualTo(DateTimeKind.Utc));
        // Boxed path dispatches on the runtime type and still converts, confirming the paths diverge.
        Assert.That(((DateTime)reader.GetValue(0)).Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    [Test]
    public async Task GetFieldValue_WithConverter_ShouldNotAffectNonTargetedTypes()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toInt32(42)");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetFieldValue<int>(0), Is.EqualTo(42));
    }

    [Test]
    public async Task GetFieldValueByName_WithConverter_ShouldTransformValue()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-01-15 12:00:00') AS dt, toInt32(42) AS i");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetFieldValue<DateTime>("dt").Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(reader.GetFieldValue<int>("i"), Is.EqualTo(42));
    }

    // ==========================================
    // GetValues consistency
    // ==========================================

    [Test]
    public async Task GetValues_WithConverter_ShouldApplyConverterToAllColumns()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00'), toInt32(42)");
        ClassicAssert.IsTrue(reader.Read());

        var values = new object[2];
        reader.GetValues(values);

        var dt = (DateTime)values[0];
        Assert.That(dt.Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(values[1], Is.EqualTo(42));
    }

    // ==========================================
    // Null/DBNull handling
    // ==========================================

    [Test]
    public async Task GetValue_WithConverter_ShouldPassNullValues()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new NullTrackingConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST(NULL AS Nullable(Int32))");
        ClassicAssert.IsTrue(reader.Read());

        ClassicAssert.IsTrue(reader.IsDBNull(0));
    }

    // ==========================================
    // Per-query converter via QueryOptions
    // ==========================================

    [Test]
    public async Task QueryOptions_ReadValueConverter_ShouldOverrideClientLevel()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Local),
        };
        using var client = new ClickHouseClient(settings);

        var options = new QueryOptions
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-01-15 12:00:00')", options: options);
        ClassicAssert.IsTrue(reader.Read());

        var value = (DateTime)reader.GetValue(0);
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    [Test]
    public async Task QueryOptions_NullConverter_ShouldFallBackToClientLevel()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        // QueryOptions with null converter should fall back to client-level
        var options = new QueryOptions();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-01-15 12:00:00')", options: options);
        ClassicAssert.IsTrue(reader.Read());

        var value = (DateTime)reader.GetValue(0);
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    [Test]
    public async Task QueryOptions_ReadValueConverter_ShouldApplyWhenClientHasNone()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        // No client-level converter
        using var client = new ClickHouseClient(settings);

        var options = new QueryOptions
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-01-15 12:00:00')", options: options);
        ClassicAssert.IsTrue(reader.Read());

        var value = (DateTime)reader.GetValue(0);
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));

        // Same client, no QueryOptions — no converter should apply
        using var reader2 = await client.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00')");
        ClassicAssert.IsTrue(reader2.Read());

        var value2 = (DateTime)reader2.GetValue(0);
        Assert.That(value2.Kind, Is.EqualTo(DateTimeKind.Unspecified),
            "Second query without options should not see the per-query converter from the first call");
    }

    // ==========================================
    // ADO.NET path (ClickHouseConnection)
    // ==========================================

    [Test]
    public async Task AdoNet_WithConverter_ShouldTransformValues()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var conn = new ClickHouseConnection(settings);

        using var reader = await conn.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00')");
        ClassicAssert.IsTrue(reader.Read());

        var value = (DateTime)reader.GetValue(0);
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    // ==========================================
    // Multiple types with a single converter
    // ==========================================

    [Test]
    public async Task GetValue_WithMultiTypeConverter_ShouldTransformAllTargetedTypes()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new MultiTypeConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toInt32(42), 'hello', toFloat64(3.14)");
        ClassicAssert.IsTrue(reader.Read());

        // Int32 should be doubled
        Assert.That(reader.GetValue(0), Is.EqualTo(84));
        // String should be uppercased
        Assert.That(reader.GetValue(1), Is.EqualTo("HELLO"));
        // Float64 should be unchanged (not targeted)
        Assert.That((double)reader.GetValue(2), Is.EqualTo(3.14).Within(0.001));
    }

    // ==========================================
    // Typed accessor methods (GetInt32, GetDateTime, etc.)
    // ==========================================

    [Test]
    public async Task GetDateTime_WithConverter_ShouldReturnConvertedValue()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00')");
        ClassicAssert.IsTrue(reader.Read());

        var value = reader.GetDateTime(0);
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    // ==========================================
    // Enum columns: integer accessors expose the raw wire ordinal, converter-independent
    // ==========================================

    [Test]
    public async Task GetInt32_EnumColumnWithLabelMutatingConverter_ReturnsRawOrdinalWithoutKeyNotFound()
    {
        // The converter rewrites the enum label to a string that is NOT a valid enum member. The
        // integer accessors must still return the raw wire ordinal (resolved from the unconverted
        // label) and agree with GetFieldValue<int> — never surfacing KeyNotFoundException — while
        // the string projections apply the converter.
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new LabelMutatingConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(
            "SELECT CAST('Active', 'Enum8(''Active'' = 1, ''Inactive'' = 2)') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            // Integer accessors expose the raw wire ordinal, independent of the converter, and agree.
            Assert.That(reader.GetInt32(0), Is.EqualTo(1));
            Assert.That(reader.GetByte(0), Is.EqualTo(1));
            Assert.That(reader.GetSByte(0), Is.EqualTo(1));
            Assert.That(reader.GetInt64(0), Is.EqualTo(1));
            Assert.That(reader.GetUInt32(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldValue<int>(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldValue<byte>(0), Is.EqualTo(1));
            // The string projections still apply the converter.
            Assert.That(reader.GetString(0), Is.EqualTo("Active_converted"));
            Assert.That(reader.GetValue(0), Is.EqualTo("Active_converted"));
            Assert.That(reader.GetFieldValue<string>(0), Is.EqualTo("Active_converted"));
        });
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task GetInt32_EnumColumnWithRelabelingConverter_ReturnsRawWireOrdinalNotConvertedLabelOrdinal()
    {
        // The converter remaps one valid enum label to ANOTHER valid label ("Active" -> "Inactive").
        // Integer accessors must return the ordinal of the RAW wire label (1), not the converted
        // label's ordinal (2), and GetInt32 must agree with GetFieldValue<int>; the string
        // projections see the converted label.
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new LabelRemappingConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = (ClickHouseDataReader)await client.ExecuteReaderAsync(
            "SELECT CAST('Active', 'Enum8(''Active'' = 1, ''Inactive'' = 2)') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetInt32(0), Is.EqualTo(1));
            Assert.That(reader.GetByte(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldValue<int>(0), Is.EqualTo(1));
            // The string projections see the converted label.
            Assert.That(reader.GetString(0), Is.EqualTo("Inactive"));
            Assert.That(reader.GetFieldValue<string>(0), Is.EqualTo("Inactive"));
        });
        ClassicAssert.IsFalse(reader.Read());
    }

    // ==========================================
    // Indexer access
    // ==========================================

    [Test]
    public async Task Indexer_WithConverter_ShouldReturnConvertedValue()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00') as dt");
        ClassicAssert.IsTrue(reader.Read());

        var value = (DateTime)reader[0];
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));

        var valueByName = (DateTime)reader["dt"];
        Assert.That(valueByName.Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    // ==========================================
    // Multiple rows
    // ==========================================

    [Test]
    public async Task GetValue_WithConverter_ShouldApplyToEveryRow()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime(18942+number,'UTC') FROM system.numbers LIMIT 100");

        var count = 0;
        while (reader.Read())
        {
            var value = (DateTime)reader.GetValue(0);
            Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
            count++;
        }

        Assert.That(count, Is.EqualTo(100));
    }

    // ==========================================
    // ExecuteScalarAsync
    // ==========================================

    [Test]
    public async Task ExecuteScalarAsync_WithConverter_ShouldTransformValue()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new DateTimeKindConverter(DateTimeKind.Utc),
        };
        using var client = new ClickHouseClient(settings);

        var result = await client.ExecuteScalarAsync("SELECT toDateTime('2025-01-15 12:00:00')");

        var value = (DateTime)result;
        Assert.That(value.Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    // ==========================================
    // Converter that throws
    // ==========================================

    [Test]
    public async Task IsDBNull_WithThrowingConverter_ShouldNotInvokeConverter()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new ThrowingConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT CAST(NULL AS Nullable(Int32)), toInt32(42)");
        ClassicAssert.IsTrue(reader.Read());

        // IsDBNull must not run the converter — it inspects the raw value.
        ClassicAssert.IsTrue(reader.IsDBNull(0));
        ClassicAssert.IsFalse(reader.IsDBNull(1));
    }

    [Test]
    public async Task IsDBNull_WithConverter_ShouldReflectRawDbValueNotConvertedOutput()
    {
        // A converter that turns 42 into null would have masked a non-null cell as null
        // if IsDBNull had run the converter.
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new NullReturningConverter(),
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toInt32(42)");
        ClassicAssert.IsTrue(reader.Read());

        ClassicAssert.IsFalse(reader.IsDBNull(0),
            "IsDBNull must reflect the raw cell, not the converter's output");
    }

    [Test]
    public void GetValue_WithThrowingConverter_ShouldPropagateException()
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = new ThrowingConverter(),
        };
        using var client = new ClickHouseClient(settings);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            using var reader = await client.ExecuteReaderAsync("SELECT toInt32(42)");
            reader.Read();
            _ = reader.GetValue(0);
        });
    }

    [Test]
    public async Task Converter_ShouldReceiveRawServerTypeStrings()
    {
        var capturing = new CapturingConverter();
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = capturing,
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(@"
            SELECT
                toInt32(42) AS i,
                'hi' AS s,
                toDateTime64(now(), 3, 'UTC') AS dt,
                CAST(NULL AS LowCardinality(Nullable(String))) AS nlc,
                [1, 2, 3] AS arr");
        ClassicAssert.IsTrue(reader.Read());

        // Force the converter to fire on every column
        var values = new object[5];
        reader.GetValues(values);

        Assert.That(capturing.Captures, Has.Count.EqualTo(5));
        Assert.That(capturing.Captures[0], Is.EqualTo(("i", "Int32")));
        Assert.That(capturing.Captures[1], Is.EqualTo(("s", "String")));
        Assert.That(capturing.Captures[2], Is.EqualTo(("dt", "DateTime64(3, 'UTC')")));
        Assert.That(capturing.Captures[3], Is.EqualTo(("nlc", "LowCardinality(Nullable(String))")));
        Assert.That(capturing.Captures[4], Is.EqualTo(("arr", "Array(UInt8)")));
    }

    [Test]
    public async Task Converter_ShouldReceiveRawServerTypeStrings_ViaGetFieldValue()
    {
        var capturing = new CapturingConverter();
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings)
        {
            ReadValueConverter = capturing,
        };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime64(now(), 6, 'Europe/Berlin') AS dt");
        ClassicAssert.IsTrue(reader.Read());

        _ = reader.GetFieldValue<DateTime>(0);

        Assert.That(capturing.Captures, Has.Count.EqualTo(1));
        Assert.That(capturing.Captures[0], Is.EqualTo(("dt", "DateTime64(6, 'Europe/Berlin')")));
    }

    [Test]
    public async Task DictionaryReadValueConverter_GetValue_ShouldApplyRegisteredTransform()
    {
        var converter = new DictionaryReadValueConverter()
            .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc));

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-01-15 12:00:00'), toInt32(42)");
        ClassicAssert.IsTrue(reader.Read());

        var dt = (DateTime)reader.GetValue(0);
        Assert.That(dt.Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(reader.GetValue(1), Is.EqualTo(42));
    }

    [Test]
    public async Task DictionaryReadValueConverter_GetFieldValue_ShouldApplyRegisteredTransform()
    {
        var converter = new DictionaryReadValueConverter()
            .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc));

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-01-15 12:00:00'), toInt32(42)");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetFieldValue<DateTime>(0).Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(reader.GetFieldValue<int>(1), Is.EqualTo(42));
    }

    [Test]
    public async Task DictionaryReadValueConverter_FluentChain_ShouldRegisterMultipleTypes()
    {
        var converter = new DictionaryReadValueConverter()
            .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            .For<string>(s => s.Trim());

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toDateTime('2025-01-15 12:00:00'), '  hello  '");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(((DateTime)reader.GetValue(0)).Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));
    }

    [Test]
    public async Task DictionaryReadValueConverter_DbNullValue_ShouldPassThrough()
    {
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => i * 2);

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST(NULL AS Nullable(Int32))");
        ClassicAssert.IsTrue(reader.Read());

        ClassicAssert.IsTrue(reader.IsDBNull(0));
        Assert.That(reader.GetValue(0), Is.EqualTo(DBNull.Value));
    }

    [Test]
    public void DictionaryReadValueConverter_For_NullConverter_ShouldThrow()
    {
        var converter = new DictionaryReadValueConverter();
        Assert.Throws<ArgumentNullException>(() => converter.For<DateTime>(null));
    }

    [Test]
    public void DictionaryReadValueConverter_ConvertValue_UnregisteredType_ShouldPassThrough()
    {
        var converter = new DictionaryReadValueConverter()
            .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc));

        Assert.That(converter.ConvertValue(42, "c", "Int32"), Is.EqualTo(42));
        Assert.That(converter.ConvertValue("hi", "c", "String"), Is.EqualTo("hi"));
        Assert.That(converter.ConvertValue<int>(42, "c", "Int32"), Is.EqualTo(42));
    }

    [Test]
    public void DictionaryReadValueConverter_ConvertValue_NullAndDbNull_ShouldPassThrough()
    {
        var converter = new DictionaryReadValueConverter()
            .For<string>(s => s.ToUpperInvariant());

        Assert.That(converter.ConvertValue(null, "c", "Nullable(String)"), Is.Null);
        Assert.That(converter.ConvertValue(DBNull.Value, "c", "Nullable(String)"), Is.EqualTo(DBNull.Value));
        Assert.That(converter.ConvertValue<string>(null, "c", "Nullable(String)"), Is.Null);
    }

    [Test]
    public void DictionaryReadValueConverter_For_Reregistration_ShouldReplacePrevious()
    {
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => i + 1)
            .For<int>(i => i * 10);

        Assert.That(converter.ConvertValue(5, "c", "Int32"), Is.EqualTo(50));
        Assert.That(converter.ConvertValue<int>(5, "c", "Int32"), Is.EqualTo(50));
    }

    [Test]
    public async Task DictionaryReadValueConverter_GetFieldValueOfObject_ShouldDispatchByRuntimeType()
    {
        var converter = new DictionaryReadValueConverter()
            .For<DateTime>(dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc));

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT toDateTime('2025-01-15 12:00:00')");
        ClassicAssert.IsTrue(reader.Read());

        var asObject = (DateTime)reader.GetFieldValue<object>(0);
        var asValue = (DateTime)reader.GetValue(0);

        Assert.That(asObject.Kind, Is.EqualTo(DateTimeKind.Utc),
            "GetFieldValue<object> should apply the converter registered for the runtime type");
        Assert.That(asObject.Kind, Is.EqualTo(asValue.Kind),
            "GetFieldValue<object> and GetValue must agree for the same column");
    }

    [Test]
    public void DictionaryReadValueConverter_ConvertValueOfObject_DispatchesByRuntimeType()
    {
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => i * 10);

        // T = object but the actual value is an int — should dispatch via the boxed wrapper
        Assert.That(converter.ConvertValue<object>(5, "c", "Int32"), Is.EqualTo(50));
        // And still passes through when runtime type isn't registered
        Assert.That(converter.ConvertValue<object>("hi", "c", "String"), Is.EqualTo("hi"));
    }

    // The converter fires once per column at the GetValue/GetFieldValue<T> boundary.
    // For composite types (Array, Tuple, Map, Nested) it sees the entire deserialized
    // container — it does NOT recurse into elements. Users who want per-element
    // transforms must do them inside their own converter.

    [Test]
    public async Task DictionaryReadValueConverter_ArrayColumn_DoesNotFireOnElementType()
    {
        var elementFired = false;
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => { elementFired = true; return i * 10; });

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST([1, 2, 3] AS Array(Int32))");
        ClassicAssert.IsTrue(reader.Read());

        var arr = (int[])reader.GetValue(0);
        Assert.That(arr, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(elementFired, Is.False, "Element-type converter must not fire for Array columns");
    }

    [Test]
    public async Task DictionaryReadValueConverter_ArrayColumn_FiresOnContainerType()
    {
        var converter = new DictionaryReadValueConverter()
            .For<int[]>(arr => arr.Select(x => x * 10).ToArray());

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST([1, 2, 3] AS Array(Int32))");
        ClassicAssert.IsTrue(reader.Read());

        var arr = (int[])reader.GetValue(0);
        Assert.That(arr, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task DictionaryReadValueConverter_NullableColumn_WithValue_FiresOnUnderlyingType()
    {
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => i * 10);

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST(7 AS Nullable(Int32))");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetValue(0), Is.EqualTo(70));
    }

    [Test]
    public async Task DictionaryReadValueConverter_GetFieldValueNullable_FiresOnUnderlyingType()
    {
        // A For<int>() registration must also fire for GetFieldValue<int?> on a Nullable(Int32)
        // column, matching GetValue — non-null cells surface as the underlying type.
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => i * 10);

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST(7 AS Nullable(Int32))");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetFieldValue<int?>(0), Is.EqualTo(70));
        Assert.That(reader.GetValue(0), Is.EqualTo(70),
            "GetFieldValue<int?> and GetValue must agree for a nullable column");
    }

    [Test]
    public void DictionaryReadValueConverter_ConvertValueNullable_FiresOnUnderlyingType()
    {
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => i * 10);

        Assert.That(converter.ConvertValue<int?>(7, "c", "Nullable(Int32)"), Is.EqualTo(70));
        // null still passes through untouched
        Assert.That(converter.ConvertValue<int?>(null, "c", "Nullable(Int32)"), Is.Null);
        // an unregistered underlying type is left alone
        Assert.That(converter.ConvertValue<long?>(7L, "c", "Nullable(Int64)"), Is.EqualTo(7L));
    }

    [Test]
    public async Task DictionaryReadValueConverter_LowCardinalityString_FiresOnString()
    {
        var converter = new DictionaryReadValueConverter()
            .For<string>(s => s.ToUpperInvariant());

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT CAST('hello' AS LowCardinality(String))");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetValue(0), Is.EqualTo("HELLO"));
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task DictionaryReadValueConverter_DynamicColumn_FiresOnUnderlyingType()
    {
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => i * 10)
            .For<string>(s => s.ToUpperInvariant());

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync(
            "SELECT CAST(toInt32(7) AS Dynamic), CAST('hi' AS Dynamic)");
        ClassicAssert.IsTrue(reader.Read());

        Assert.That(reader.GetValue(0), Is.EqualTo(70));
        Assert.That(reader.GetValue(1), Is.EqualTo("HI"));
    }

    [Test]
    public async Task DictionaryReadValueConverter_TupleColumn_DoesNotFireOnElementTypes()
    {
        var elementFired = false;
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => { elementFired = true; return i * 10; })
            .For<string>(s => { elementFired = true; return s.ToUpperInvariant(); });

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT tuple(toInt32(1), 'hello')");
        ClassicAssert.IsTrue(reader.Read());

        var tuple = (System.Runtime.CompilerServices.ITuple)reader.GetValue(0);
        Assert.That(tuple[0], Is.EqualTo(1));
        Assert.That(tuple[1], Is.EqualTo("hello"));
        Assert.That(elementFired, Is.False, "Element-type converters must not fire for Tuple columns");
    }

    [Test]
    public async Task DictionaryReadValueConverter_MapColumn_DoesNotFireOnEntryTypes()
    {
        var entryFired = false;
        var converter = new DictionaryReadValueConverter()
            .For<int>(i => { entryFired = true; return i * 10; })
            .For<string>(s => { entryFired = true; return s.ToUpperInvariant(); });

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST(map('a', 1, 'b', 2) AS Map(String, Int32))");
        ClassicAssert.IsTrue(reader.Read());

        var map = reader.GetValue(0);
        Assert.That(map, Is.Not.Null);
        Assert.That(entryFired, Is.False, "Element-type converters must not fire for Map columns");
    }

    [Test]
    public async Task DictionaryReadValueConverter_NestedArray_DoesNotFireOnInnerArrays()
    {
        var innerFired = false;
        var converter = new DictionaryReadValueConverter()
            .For<int[]>(arr => { innerFired = true; return arr.Select(x => x * 10).ToArray(); });

        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings = new ClickHouseClientSettings(settings) { ReadValueConverter = converter };
        using var client = new ClickHouseClient(settings);

        using var reader = await client.ExecuteReaderAsync("SELECT CAST([[1, 2], [3, 4]] AS Array(Array(Int32)))");
        ClassicAssert.IsTrue(reader.Read());

        var outer = (int[][])reader.GetValue(0);
        Assert.That(outer[0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(outer[1], Is.EqualTo(new[] { 3, 4 }));
        Assert.That(innerFired, Is.False, "Inner-array converter must not fire for nested Array columns");
    }

    private sealed class DateTimeKindConverter : IReadValueConverter
    {
        private readonly DateTimeKind kind;

        public DateTimeKindConverter(DateTimeKind kind) => this.kind = kind;

        public object ConvertValue(object value, string columnName, string clickHouseType)
        {
            if (value is DateTime dt)
                return DateTime.SpecifyKind(dt, kind);
            return value;
        }

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            if (typeof(T) == typeof(DateTime) && value is DateTime dt)
                return (T)(object)DateTime.SpecifyKind(dt, kind);
            return value;
        }
    }

    private sealed class NullTrackingConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType) => value;
        public T ConvertValue<T>(T value, string columnName, string clickHouseType) => value;
    }

    // Rewrites string values (including enum labels) to a non-label string, and would also mutate
    // ints. Used to prove enum integer accessors expose the raw ordinal regardless of the converter,
    // while the string projection still applies it.
    private sealed class LabelMutatingConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
            => value is string s ? s + "_converted" : value;

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            if (value is string s)
                return (T)(object)(s + "_converted");
            if (value is int i)
                return (T)(object)(i + 100);
            return value;
        }
    }

    // Remaps one valid enum label to another valid label. Used to prove enum integer accessors
    // expose the raw wire ordinal, not the ordinal of the converted label.
    private sealed class LabelRemappingConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
            => value is string s && s == "Active" ? "Inactive" : value;

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
            => value is string s && s == "Active" ? (T)(object)"Inactive" : value;
    }

    private sealed class ThrowingConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
            => throw new InvalidOperationException("Converter failed");

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
            => throw new InvalidOperationException("Converter failed");
    }

    private sealed class NullReturningConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType) => null;
        public T ConvertValue<T>(T value, string columnName, string clickHouseType) => default;
    }

    private sealed class MultiTypeConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
        {
            if (value is int i)
                return i * 2;
            if (value is string s)
                return s.ToUpperInvariant();
            return value;
        }

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            var converted = ConvertValue((object)value, columnName, clickHouseType);
            return (T)converted;
        }
    }

    private sealed class CapturingConverter : IReadValueConverter
    {
        public System.Collections.Generic.List<(string Column, string ClickHouseType)> Captures { get; } = new();

        public object ConvertValue(object value, string columnName, string clickHouseType)
        {
            Captures.Add((columnName, clickHouseType));
            return value;
        }

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
        {
            Captures.Add((columnName, clickHouseType));
            return value;
        }
    }
}
