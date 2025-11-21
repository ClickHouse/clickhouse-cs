using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Represents a tuple with named fields from ClickHouse.
/// Supports both indexed access (tuple[0]) and named access (tuple["fieldName"]).
/// </summary>
public class NamedTuple
#if !NET462
    : ITuple
#endif
{
    private readonly object[] values;
    private readonly string[] names;
    private readonly Dictionary<string, int> nameIndex;

    internal ClickHouseType[] UnderlyingTypes { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="NamedTuple"/> class.
    /// </summary>
    /// <param name="names">Field names</param>
    /// <param name="values">Field values</param>
    public NamedTuple(string[] names, object[] values)
    {
        if (names == null)
            throw new ArgumentNullException(nameof(names));
        if (values == null)
            throw new ArgumentNullException(nameof(values));
        if (names.Length != values.Length)
            throw new ArgumentException("Number of names must match number of values");

        this.names = names;
        this.values = values;
        this.nameIndex = names.Select((n, i) => (n, i))
                             .ToDictionary(x => x.n, x => x.i);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="NamedTuple"/> class from an array of name-value pairs.
    /// </summary>
    /// <param name="fields">Array where each element is [name, value]</param>
    public NamedTuple(object[][] fields)
    {
        if (fields == null)
            throw new ArgumentNullException(nameof(fields));

        var length = fields.Length;
        names = new string[length];
        values = new object[length];

        for (int i = 0; i < length; i++)
        {
            if (fields[i] == null || fields[i].Length != 2)
                throw new ArgumentException($"Field at index {i} must be an array of length 2 [name, value]");

            if (fields[i][0] is not string name)
                throw new ArgumentException($"Field name at index {i} must be a string");

            names[i] = name;
            values[i] = fields[i][1];
        }

        nameIndex = names.Select((n, i) => (n, i))
                         .ToDictionary(x => x.n, x => x.i);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="NamedTuple"/> class from a dictionary.
    /// </summary>
    /// <param name="dictionary">Dictionary of field names to values</param>
    public NamedTuple(Dictionary<string, object> dictionary)
    {
        if (dictionary == null)
            throw new ArgumentNullException(nameof(dictionary));

        var length = dictionary.Count;
        names = new string[length];
        values = new object[length];

        int i = 0;
        foreach (var kvp in dictionary)
        {
            names[i] = kvp.Key;
            values[i] = kvp.Value;
            i++;
        }

        nameIndex = names.Select((n, idx) => (n, idx))
                         .ToDictionary(x => x.n, x => x.idx);
    }

    /// <summary>
    /// Gets the value at the specified index.
    /// </summary>
    public object this[int index]
    {
        get
        {
            if (index < 0 || index >= values.Length)
                throw new ArgumentOutOfRangeException(nameof(index));
            return values[index];
        }
    }

    /// <summary>
    /// Gets the value with the specified field name.
    /// </summary>
    public object this[string name]
    {
        get
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));
            if (!nameIndex.TryGetValue(name, out var index))
                throw new KeyNotFoundException($"Field '{name}' not found in named tuple");
            return values[index];
        }
    }

    /// <summary>
    /// Gets the number of fields in the tuple.
    /// </summary>
    public int Length => values.Length;

    /// <summary>
    /// Gets the field names.
    /// </summary>
    public IReadOnlyList<string> Names => names;

    /// <summary>
    /// Attempts to get the value with the specified field name.
    /// </summary>
    public bool TryGetValue(string name, out object value)
    {
        if (nameIndex.TryGetValue(name, out var index))
        {
            value = values[index];
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Checks if the tuple contains a field with the specified name.
    /// </summary>
    public bool ContainsName(string name) => nameIndex.ContainsKey(name);

    /// <summary>
    /// Returns a string representation of the named tuple.
    /// </summary>
    public override string ToString()
    {
        var fields = names.Select((n, i) => $"{n}: {values[i]}");
        return $"({string.Join(", ", fields)})";
    }

    /// <summary>
    /// Gets the hash code for this named tuple.
    /// </summary>
    public override int GetHashCode()
    {
        var hash = 17;
        foreach (var value in values)
        {
            hash = hash * 31 + (value?.GetHashCode() ?? 0);
        }
        return hash;
    }

    /// <summary>
    /// Determines whether this named tuple equals another object.
    /// </summary>
    public override bool Equals(object obj)
    {
        if (obj is not NamedTuple other)
            return false;

        if (Length != other.Length)
            return false;

        for (int i = 0; i < Length; i++)
        {
            if (names[i] != other.names[i])
                return false;
            if (!Equals(values[i], other.values[i]))
                return false;
        }

        return true;
    }
}
