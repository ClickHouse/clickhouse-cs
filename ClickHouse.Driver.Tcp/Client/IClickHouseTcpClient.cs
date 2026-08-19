using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The contract of a ClickHouse client that speaks the native TCP protocol. It runs every
/// <see cref="IClickHouseTcpOperations"/> member over a pool, so consecutive operations need not land on the same
/// connection. <see cref="ClickHouseTcpClient"/> is the implementation; code against this interface to substitute
/// a test double.
///
/// <para>
/// This type is experimental: its surface may change in a future release. Suppress diagnostic
/// <c>CHTCP0001</c> to acknowledge that.
/// </para>
/// </summary>
[Experimental("CHTCP0001")]
public interface IClickHouseTcpClient : IClickHouseTcpOperations
{
}
