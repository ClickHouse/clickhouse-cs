using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// ClickHouse's server-side <c>Identifier</c> query-parameter pseudo-type, e.g. <c>{name:Identifier}</c>.
/// Used to bind a database, table or column name. The value is sent verbatim as <c>param_&lt;name&gt;</c>;
/// the server substitutes it as a bare SQL identifier and applies its own backtick quoting/escaping.
/// <para>
/// This is not a column data type — it can appear only as the type of a query parameter, so it is never
/// read from, or written to, a result set. <see cref="HttpParameterFormatter"/> emits the value unquoted
/// and unescaped because the server owns identifier escaping.
/// </para>
/// </summary>
internal class IdentifierType : ClickHouseType
{
    public override Type FrameworkType => typeof(string);

    public override object Read(ExtendedBinaryReader reader) =>
        throw new NotSupportedException("Identifier is a query-parameter-only type and is never read from a result set");

    public override void Write(ExtendedBinaryWriter writer, object value) =>
        throw new NotSupportedException("Identifier is a query-parameter-only type and is never written in binary form");

    public override string ToString() => "Identifier";
}
