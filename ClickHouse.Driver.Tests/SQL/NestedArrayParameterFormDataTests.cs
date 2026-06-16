using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

/// <summary>
/// Mirrors the URL-query parameter coverage in <see cref="NestedArrayParameterTests"/> for the
/// multipart/form-data parameter request encoding (<c>useFormDataParameters: true</c>).
/// Form-data uses a different request body construction path, so nested-array formatting must
/// be exercised there independently.
/// </summary>
[TestFixture(true)]
[TestFixture(false)]
public class NestedArrayParameterFormDataTests
{
    private readonly ClickHouseConnection connection;

    public NestedArrayParameterFormDataTests(bool useCompression)
    {
        connection = TestUtilities.GetTestClickHouseConnection(useCompression, useFormDataParameters: true);
        connection.Open();
    }

    [Test]
    [RequiredFeature(Feature.ParamsInMultipartFormData)]
    public async Task ExecuteReaderAsync_JaggedByteArray_RoundTripsViaFormData()
    {
        var input = new byte[][] { new byte[] { 1, 2 }, new byte[] { 3, 4, 5 } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(UInt8))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = (byte[][])reader.GetValue(0);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    [RequiredFeature(Feature.ParamsInMultipartFormData)]
    public async Task ExecuteReaderAsync_Multidim2DByteArray_RoundTripsViaFormData()
    {
        var input = new byte[,] { { 1, 2, 3 }, { 4, 5, 6 } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(UInt8))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = (byte[][])reader.GetValue(0);
        Assert.That(result[0], Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(result[1], Is.EqualTo(new byte[] { 4, 5, 6 }));
    }

    [Test]
    [RequiredFeature(Feature.ParamsInMultipartFormData)]
    public async Task ExecuteReaderAsync_Multidim3DInt32Array_RoundTripsViaFormData()
    {
        var input = new int[2, 2, 2] { { { 1, 2 }, { 3, 4 } }, { { 5, 6 }, { 7, 8 } } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Array(Int32)))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = (int[][][])reader.GetValue(0);
        Assert.That(result[0][0], Is.EqualTo(new[] { 1, 2 }));
        Assert.That(result[0][1], Is.EqualTo(new[] { 3, 4 }));
        Assert.That(result[1][0], Is.EqualTo(new[] { 5, 6 }));
        Assert.That(result[1][1], Is.EqualTo(new[] { 7, 8 }));
    }

    [Test]
    [RequiredFeature(Feature.ParamsInMultipartFormData)]
    public async Task ExecuteReaderAsync_JaggedStringArrayWithEscapes_PreservesQuotesAndBackslashes()
    {
        // String escaping rules differ between URL-query and form-data bodies; this test guards
        // against regressions in either path.
        var input = new string[][] { new[] { "a'b", "c\\d", "with\nnewline" }, new[] { "ok" } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(String))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = (string[][])reader.GetValue(0);
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    [RequiredFeature(Feature.ParamsInMultipartFormData)]
    public async Task ExecuteReaderAsync_NullableInnerElements_RoundTripsViaFormData()
    {
        var input = new int?[][] { new int?[] { 1, null, 3 }, new int?[] { null } };
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {p:Array(Array(Nullable(Int32)))} as result";
        command.AddParameter("p", input);
        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = (int?[][])reader.GetValue(0);
        Assert.That(result, Is.EqualTo(input));
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => connection?.Dispose();
}
