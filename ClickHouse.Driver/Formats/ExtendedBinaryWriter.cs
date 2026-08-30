using System.IO;
using System.Text;

namespace ClickHouse.Driver.Formats;

public class ExtendedBinaryWriter : BinaryWriter
{
    public ExtendedBinaryWriter(Stream stream)
        : base(stream, Encoding.UTF8, false) { }

    public ExtendedBinaryWriter(Stream stream, bool leaveOpen)
        : base(stream, Encoding.UTF8, leaveOpen) { }

    /// <summary>
    /// The stream this writer writes to, without the flush <see cref="BinaryWriter.BaseStream"/>
    /// performs in its getter.
    /// </summary>
    /// <remarks>
    /// Use this to copy a value's bytes straight into the output. This writer leaves nothing pending -
    /// each of its writes, string encoding included, reaches the stream within the call - so writing
    /// to the stream directly cannot reorder or lose anything. Flushing instead costs a compression
    /// block per value, which makes an insert scale with the value count rather than the byte count.
    /// </remarks>
    internal Stream RawStream => OutStream;

    public new void Write7BitEncodedInt(int i) => base.Write7BitEncodedInt(i);
}
