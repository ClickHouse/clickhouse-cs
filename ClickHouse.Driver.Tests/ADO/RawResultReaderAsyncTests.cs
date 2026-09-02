using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[Category("Cloud")]
public class RawResultReaderAsyncTests : AbstractConnectionTestFixture
{
    private async Task<ClickHouseRawResult> ExecuteRawResultAsync()
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1,2,3 FORMAT TSV";
        return await command.ExecuteRawResultAsync(CancellationToken.None);
    }
    
    [Test]
    public async Task ReadAsStreamAsync_WithoutCancellationToken_ReturnsStream()
    {
        using var result = await ExecuteRawResultAsync();
        using var stream = await result.ReadAsStreamAsync();
        using var reader = new StreamReader(stream);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("1\t2\t3\n"));
    }
    
    [Test]
    public async Task ReadAsStreamAsync_WithCancellationToken_ReturnsStream()
    {
        using var cts = new CancellationTokenSource();
        using var result = await ExecuteRawResultAsync();
        using var stream = await result.ReadAsStreamAsync(cts.Token);
        using var reader = new StreamReader(stream);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("1\t2\t3\n"));
    }

    [Test]
    public async Task CopyToAsync_WithoutCancellationToken_ShouldCopyToStream()
    {
        using var result = await ExecuteRawResultAsync();
        using var stream = new MemoryStream();
        await result.CopyToAsync(stream);

        stream.Seek(0, SeekOrigin.Begin);

        using var reader = new StreamReader(stream);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("1\t2\t3\n"));
    }
    
    [Test]
    public async Task CopyToAsync_WithCancellationToken_ShouldCopyToStream()
    {
        using var cts = new CancellationTokenSource();
        using var result = await ExecuteRawResultAsync();
        using var stream = new MemoryStream();
        await result.CopyToAsync(stream, cts.Token);

        stream.Seek(0, SeekOrigin.Begin);

        using var reader = new StreamReader(stream);
        Assert.That(reader.ReadToEnd(), Is.EqualTo("1\t2\t3\n"));
    }
    
    [Test]
    public async Task CopyToAsync_WithCanceledToken_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        using var result = await ExecuteRawResultAsync();
        using var stream = new MemoryStream();
        Assert.CatchAsync<OperationCanceledException>(() => result.CopyToAsync(stream, cts.Token));
    }
    
    [Test]
    public async Task ReadAsByteArrayAsync_WithoutCancellationToken_ReturnsByteArray()
    {
        using var result = await ExecuteRawResultAsync();
        var array = await result.ReadAsByteArrayAsync();
        Assert.That(array, Is.EqualTo(Encoding.UTF8.GetBytes("1\t2\t3\n")));
    }
    
    [Test]
    public async Task ReadAsByteArrayAsync_WithCancellationToken_ReturnsByteArray()
    {
        using var cts = new CancellationTokenSource();
        using var result = await ExecuteRawResultAsync();
        var array = await result.ReadAsByteArrayAsync(cts.Token);
        Assert.That(array, Is.EqualTo(Encoding.UTF8.GetBytes("1\t2\t3\n")));
    }
    
    [Test]
    public async Task ReadAsByteArrayAsync_WithCanceledToken_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        using var result = await ExecuteRawResultAsync();
        Assert.CatchAsync<OperationCanceledException>(() => result.ReadAsByteArrayAsync(cts.Token));
    }

    [Test]
    public async Task ReadAsStringAsync_WithoutCancellationToken_ReturnsStrung()
    {
        using var result = await ExecuteRawResultAsync();
        var @string = await result.ReadAsStringAsync();
        Assert.That(@string, Is.EqualTo("1\t2\t3\n"));
    }
    
    [Test]
    public async Task ReadAsStringAsync_WithCancellationToken_ReturnsStrung()
    {
        using var cts = new CancellationTokenSource();
        using var result = await ExecuteRawResultAsync();
        var @string = await result.ReadAsStringAsync(cts.Token);
        Assert.That(@string, Is.EqualTo("1\t2\t3\n"));
    }
    
    [Test]
    public async Task ReadAsStringAsync_WithCanceledToken_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        
        using var result = await ExecuteRawResultAsync();
        Assert.CatchAsync<OperationCanceledException>(() => result.ReadAsStringAsync(cts.Token));
    }
}
