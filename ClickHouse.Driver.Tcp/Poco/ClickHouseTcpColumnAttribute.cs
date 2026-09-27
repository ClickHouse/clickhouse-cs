using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Maps a property to a differently-named ClickHouse column on the native-TCP client's POCO paths.
///
/// <para>
/// Without this attribute a property is matched against the column of the same name, then case-insensitively,
/// then ignoring underscores — so <c>UserId</c> already finds a <c>user_id</c> column. Use the attribute only when
/// the names differ by more than that.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ClickHouseTcpColumnAttribute : Attribute
{
    /// <summary>
    /// The ClickHouse column name this property maps to. Leave unset to match on the property name.
    /// </summary>
    public string Name { get; set; }
}
