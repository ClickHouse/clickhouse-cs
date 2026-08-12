namespace ClickHouse.Driver;

/// <summary>
/// Specifies how Map columns are returned when reading from ClickHouse.
/// </summary>
public enum MapReadMode
{
    /// <summary>
    /// Maps are returned as Dictionary&lt;TKey, TValue&gt;. A map holding several entries with the
    /// same key keeps only the last of them, because a dictionary cannot hold duplicate keys.
    /// </summary>
    Dictionary = 0,

    /// <summary>
    /// Maps are returned as List&lt;KeyValuePair&lt;TKey, TValue&gt;&gt;, in the order the server sent
    /// them. Every key-value pair is preserved, including entries which repeat a key.
    /// </summary>
    KeyValuePairs = 1,
}
