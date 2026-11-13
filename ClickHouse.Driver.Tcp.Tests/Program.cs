using System;
using ClickHouse.Driver.Tcp.Tests.Poco;

namespace ClickHouse.Driver.Tcp.Tests;

/// <summary>
/// The entry point of the test assembly. The test runner does not call it. <see cref="PocoReadNoDynamicCodeTests"/> and
/// <see cref="PocoWriteNoDynamicCodeTests"/> start the assembly in a child process with dynamic code off and give it their
/// argument.
/// </summary>
internal static class Program
{
    /// <summary>
    /// Reads the POCO rows of <see cref="PocoReadNoDynamicCodeTests"/> or writes the rows of
    /// <see cref="PocoWriteNoDynamicCodeTests"/> when asked, else does nothing.
    /// </summary>
    /// <param name="args">The arguments of the process.</param>
    /// <returns>The exit code.</returns>
    public static int Main(string[] args) => args switch
    {
        [PocoReadNoDynamicCodeTests.Argument] => PocoReadNoDynamicCodeTests.Read(Console.Out),
        [PocoWriteNoDynamicCodeTests.Argument] => PocoWriteNoDynamicCodeTests.Write(Console.Out),
        _ => 0,
    };
}
