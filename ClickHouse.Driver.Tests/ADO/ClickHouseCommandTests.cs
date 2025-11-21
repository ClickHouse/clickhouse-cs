using System;
using ClickHouse.Driver.ADO;
namespace ClickHouse.Driver.Tests.ADO;

public class ClickHouseCommandTests
{
    [Test]
    public void ExecuteReaderAsync_WithoutConnection_ThrowsInvalidOperationException()
    {
        // Arrange
        var command = new ClickHouseCommand();
        command.CommandText = "SELECT 1";

        // Act & Assert
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            using var reader = await command.ExecuteReaderAsync();
        });

        Assert.That(ex.Message, Is.EqualTo("Connection is not set"));
    }

    [Test]
    public void ExecuteNonQueryAsync_WithoutConnection_ThrowsInvalidOperationException()
    {
        // Arrange
        var command = new ClickHouseCommand();
        command.CommandText = "INSERT INTO test VALUES (1)";

        // Act & Assert
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await command.ExecuteNonQueryAsync();
        });

        Assert.That(ex.Message, Is.EqualTo("Connection is not set"));
    }

    [Test]
    public void ExecuteScalarAsync_WithoutConnection_ThrowsInvalidOperationException()
    {
        // Arrange
        var command = new ClickHouseCommand();
        command.CommandText = "SELECT 1";

        // Act & Assert
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await command.ExecuteScalarAsync();
        });

        Assert.That(ex.Message, Is.EqualTo("Connection is not set"));
    }

    [Test]
    public void ExecuteRawResultAsync_WithoutConnection_ThrowsInvalidOperationException()
    {
        // Arrange
        var command = new ClickHouseCommand();
        command.CommandText = "SELECT 1 FORMAT JSON";

        // Act & Assert
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await command.ExecuteRawResultAsync(default);
        });

        Assert.That(ex.Message, Is.EqualTo("Connection is not set"));
    }
}
