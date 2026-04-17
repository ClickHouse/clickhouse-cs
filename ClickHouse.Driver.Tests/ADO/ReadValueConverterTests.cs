using System;
using System.Data;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
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

    // ==========================================
    // ClickHouse type string forwarded to the converter
    // ==========================================

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

    // ==========================================
    // Test converter implementations
    // ==========================================

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

    private sealed class ThrowingConverter : IReadValueConverter
    {
        public object ConvertValue(object value, string columnName, string clickHouseType)
            => throw new InvalidOperationException("Converter failed");

        public T ConvertValue<T>(T value, string columnName, string clickHouseType)
            => throw new InvalidOperationException("Converter failed");
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
