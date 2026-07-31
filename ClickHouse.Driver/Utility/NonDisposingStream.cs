using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Utility;

/// <summary>
/// Pass-through stream wrapper whose disposal does not propagate to the wrapped stream.
///
/// <para>
/// The driver hands caller-supplied streams to <see cref="System.Net.Http.StreamContent"/>, and both
/// <see cref="System.Net.Http.StreamContent"/> and the <see cref="System.Net.Http.HttpRequestMessage"/>
/// that carries it dispose what they were given. Wrapping the caller's stream keeps that internal
/// disposal from closing a stream the caller still owns.
/// </para>
/// </summary>
internal sealed class NonDisposingStream : Stream
{
    private readonly Stream inner;

    public NonDisposingStream(Stream inner) => this.inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public override bool CanRead => inner.CanRead;

    public override bool CanSeek => inner.CanSeek;

    public override bool CanWrite => inner.CanWrite;

    public override bool CanTimeout => inner.CanTimeout;

    public override long Length => inner.Length;

    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    public override int ReadTimeout
    {
        get => inner.ReadTimeout;
        set => inner.ReadTimeout = value;
    }

    public override int WriteTimeout
    {
        get => inner.WriteTimeout;
        set => inner.WriteTimeout = value;
    }

    public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

    public override int Read(Span<byte> buffer) => inner.Read(buffer);

    public override int ReadByte() => inner.ReadByte();

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => inner.ReadAsync(buffer, offset, count, cancellationToken);

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        => inner.ReadAsync(buffer, cancellationToken);

    public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);

    public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);

    public override void WriteByte(byte value) => inner.WriteByte(value);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => inner.WriteAsync(buffer, offset, count, cancellationToken);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        => inner.WriteAsync(buffer, cancellationToken);

    public override void CopyTo(Stream destination, int bufferSize) => inner.CopyTo(destination, bufferSize);

    public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        => inner.CopyToAsync(destination, bufferSize, cancellationToken);

    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

    public override void SetLength(long value) => inner.SetLength(value);

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    // Deliberately does not touch the wrapped stream: it belongs to the caller. Close() and
    // DisposeAsync() both route through here, so neither reaches the wrapped stream either.
    protected override void Dispose(bool disposing)
    {
    }
}
