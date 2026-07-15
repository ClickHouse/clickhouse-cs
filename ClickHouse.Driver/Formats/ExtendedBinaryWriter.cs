using System.IO;
using System.Text;

namespace ClickHouse.Driver.Formats;

public class ExtendedBinaryWriter : BinaryWriter
{
    public ExtendedBinaryWriter(Stream stream)
        : base(stream, Encoding.UTF8, false) { }

    public ExtendedBinaryWriter(Stream stream, bool leaveOpen)
        : base(stream, Encoding.UTF8, leaveOpen) { }

    public new void Write7BitEncodedInt(int i) => base.Write7BitEncodedInt(i);
}
