using System;
using System.IO;
using System.Text;

namespace ClickHouse.Driver.Formats;

internal class ExtendedBinaryReader : BinaryReader
{
    private readonly PeekableStreamWrapper streamWrapper;

    public ExtendedBinaryReader(Stream stream)
        : base(new PeekableStreamWrapper(stream), Encoding.UTF8, false)
    {
        streamWrapper = (PeekableStreamWrapper)BaseStream;
    }

    public new int Read7BitEncodedInt() => base.Read7BitEncodedInt();

    /// <summary>
    /// Performs guaranteed read of requested number of bytes, or throws an exception
    /// </summary>
    /// <param name="count">number of bytes to read</param>
    /// <returns>number of bytes read, always equals to count</returns>
    /// <exception cref="EndOfStreamException">thrown if requested number of bytes is not available</exception>
    public override byte[] ReadBytes(int count)
    {
        var buffer = new byte[count];
        Read(buffer, 0, count);
        return buffer;
    }

    /// <summary>
    /// Performs guaranteed read of requested number of bytes, or throws an exception
    /// </summary>
    /// <param name="buffer">buffer array</param>
    /// <param name="index">index to write to in the buffer</param>
    /// <param name="count">number of bytes to read</param>
    /// <returns>number of bytes read, always equals to count</returns>
    /// <exception cref="EndOfStreamException">thrown if requested number of bytes is not available</exception>
    public override int Read(byte[] buffer, int index, int count)
    {
        int bytesRead = 0;
        do
        {
            int read = base.Read(buffer, index + bytesRead, count - bytesRead);
            bytesRead += read;
            if (read == 0 && bytesRead < count)
            {
                throw new EndOfStreamException($"Expected to read {count} bytes, got {bytesRead}");
            }
        }
        while (bytesRead < count);

        return bytesRead;
    }

    /// <summary>
    /// Performs guaranteed read of enough bytes to fill <paramref name="buffer"/>, or throws.
    /// Lets callers read fixed-size values into a stack-allocated span without heap allocation.
    /// </summary>
    /// <param name="buffer">destination span to fill completely</param>
    /// <exception cref="EndOfStreamException">thrown if the stream ends before the span is filled</exception>
    public void ReadBytes(Span<byte> buffer)
    {
        int bytesRead = 0;
        while (bytesRead < buffer.Length)
        {
            int read = base.Read(buffer.Slice(bytesRead));
            if (read == 0)
            {
                throw new EndOfStreamException($"Expected to read {buffer.Length} bytes, got {bytesRead}");
            }

            bytesRead += read;
        }
    }

    public override int PeekChar() => streamWrapper.Peek();
}
