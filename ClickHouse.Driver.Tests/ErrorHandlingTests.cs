﻿using System.Threading.Tasks;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

public static class ErrorHandlingTests
{
    [Test]
    public static async Task ExceptionHandlerShouldParseErrorCode()
    {
        using var connection = TestUtilities.GetTestClickHouseConnection(true);
        try
        {
            var result = await connection.ExecuteScalarAsync("SELECT A");
        }
        catch (ClickHouseServerException ex)
        {
            Assert.That(ex.ErrorCode, Is.EqualTo(47));
        }
    }

    [Test]
    public static void UnknownTypeShouldThrowException()
    {
        using var connection = TestUtilities.GetTestClickHouseConnection(true);
        Assert.ThrowsAsync<System.ArgumentException>(async () => await connection.ExecuteScalarAsync("SELECT INTERVAL 4 DAY"));
    }
}
