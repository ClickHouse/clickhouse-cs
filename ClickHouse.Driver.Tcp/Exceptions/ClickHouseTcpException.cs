using System;
using System.Data.Common;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The base class for the failures the native-protocol client raises about the database or the connection
/// to it: <see cref="ClickHouseTcpServerException"/>, <see cref="ClickHouseTcpProtocolException"/> and
/// <see cref="ClickHouseTcpTransportException"/>.
/// </summary>
/// <remarks>
/// <para>
/// Catch this to handle anything that went wrong between the client and the server. It does not cover
/// mistakes in the calling code, which keep the usual framework types — <see cref="ArgumentException"/>
/// and friends for bad arguments, <see cref="InvalidOperationException"/> for a misused object,
/// <see cref="ObjectDisposedException"/> after disposal, and <see cref="OperationCanceledException"/>
/// when the caller cancels.
/// </para>
/// <para>
/// The hierarchy is closed: the constructors are not visible outside this assembly, so a caught
/// <see cref="ClickHouseTcpException"/> is always one of the three types above.
/// </para>
/// </remarks>
public abstract class ClickHouseTcpException : DbException
{
    private protected ClickHouseTcpException(string message)
        : base(message)
    {
    }

    private protected ClickHouseTcpException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
