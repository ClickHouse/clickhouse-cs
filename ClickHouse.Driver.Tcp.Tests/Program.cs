using System;
using ClickHouse.Driver.Tcp.Tests.Poco;

namespace ClickHouse.Driver.Tcp.Tests;

/// <summary>
/// The entry point of the test assembly. The test runner does not call it. <see cref="PocoReadNoDynamicCodeTests"/>
/// starts the assembly in a child process with dynamic code off and gives it <see cref="PocoReadNoDynamicCodeTests.Argument"/>.
/// </summary>
internal static class Program
{
    /// <summary>Reads the POCO rows of <see cref="PocoReadNoDynamicCodeTests"/> when asked, else does nothing.</summary>
    /// <param name="args">The arguments of the process.</param>
    /// <returns>The exit code.</returns>
    public static int Main(string[] args)
        => args.Length == 1 && args[0] == PocoReadNoDynamicCodeTests.Argument ? PocoReadNoDynamicCodeTests.Read(Console.Out) : 0;
}
