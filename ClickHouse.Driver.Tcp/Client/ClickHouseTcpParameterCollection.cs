using System;
using System.Collections;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The parameters bound to one query, keyed by name and kept in the order they were added.
/// </summary>
/// <remarks>
/// Names are matched with ordinal comparison, because ClickHouse parameter names are case-sensitive. A name may
/// appear only once: the wire format is a name/value list, so a repeated name has no defined meaning.
/// <para>
/// This type is mutable and is not thread-safe. A <see cref="ClickHouseTcpClient"/> is meant to be shared, so
/// build a collection and then leave it alone: adding to one while an operation reads it is a data race. To
/// vary the values per operation, build a collection per operation.
/// </para>
/// </remarks>
public sealed class ClickHouseTcpParameterCollection : IEnumerable<ClickHouseTcpParameter>
{
    private readonly List<ClickHouseTcpParameter> parameters = [];
    private readonly Dictionary<string, int> indexByName = new(StringComparer.Ordinal);

    /// <summary>Initializes a new empty instance of the <see cref="ClickHouseTcpParameterCollection"/> class.</summary>
    public ClickHouseTcpParameterCollection()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ClickHouseTcpParameterCollection"/> class with the given parameters.</summary>
    /// <param name="parameters">The parameters to add, in order.</param>
    /// <exception cref="ArgumentNullException"><paramref name="parameters"/> is null.</exception>
    /// <exception cref="ArgumentException">A parameter name is null or empty, or repeats an earlier one.</exception>
    public ClickHouseTcpParameterCollection(IEnumerable<ClickHouseTcpParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        foreach (ClickHouseTcpParameter parameter in parameters)
        {
            Add(parameter);
        }
    }

    /// <summary>The number of parameters.</summary>
    public int Count => parameters.Count;

    /// <summary>Gets the parameter with the given name.</summary>
    /// <param name="name">The parameter name.</param>
    /// <returns>The parameter.</returns>
    /// <exception cref="KeyNotFoundException">No parameter has that name.</exception>
    public ClickHouseTcpParameter this[string name]
        => indexByName.TryGetValue(name ?? string.Empty, out int index)
            ? parameters[index]
            : throw new KeyNotFoundException($"No parameter named '{name}' is bound to this query.");

    /// <summary>Adds a parameter whose type comes from the query's <c>{name:Type}</c> placeholder.</summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The value.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is null or empty, or already added.</exception>
    public void Add(string name, object value) => Add(new ClickHouseTcpParameter(name, value));

    /// <summary>Adds a parameter, overriding the type the value is formatted as.</summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The value.</param>
    /// <param name="clickHouseType">The ClickHouse type to format the value as.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is null or empty, or already added.</exception>
    public void Add(string name, object value, string clickHouseType)
        => Add(new ClickHouseTcpParameter(name, value, clickHouseType));

    /// <summary>Adds a parameter.</summary>
    /// <param name="parameter">The parameter.</param>
    /// <exception cref="ArgumentNullException"><paramref name="parameter"/> is null.</exception>
    /// <exception cref="ArgumentException">The name is null or empty, or already added.</exception>
    public void Add(ClickHouseTcpParameter parameter)
    {
        ArgumentNullException.ThrowIfNull(parameter);

        // An empty name would collide with the empty key that terminates the wire parameter list.
        if (string.IsNullOrEmpty(parameter.Name))
        {
            throw new ArgumentException("A query parameter name must not be null or empty.", nameof(parameter));
        }

        if (indexByName.ContainsKey(parameter.Name))
        {
            throw new ArgumentException($"A parameter named '{parameter.Name}' is already bound to this query.", nameof(parameter));
        }

        indexByName[parameter.Name] = parameters.Count;
        parameters.Add(parameter);
    }

    /// <summary>Reports whether a parameter with the given name is bound.</summary>
    /// <param name="name">The parameter name.</param>
    /// <returns>True when the name is bound.</returns>
    public bool Contains(string name) => indexByName.ContainsKey(name ?? string.Empty);

    /// <summary>Gets the parameter with the given name, if it is bound.</summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="parameter">The parameter, or null when the name is not bound.</param>
    /// <returns>True when the name is bound.</returns>
    public bool TryGetValue(string name, out ClickHouseTcpParameter parameter)
    {
        if (indexByName.TryGetValue(name ?? string.Empty, out int index))
        {
            parameter = parameters[index];
            return true;
        }

        parameter = null;
        return false;
    }

    /// <inheritdoc/>
    public IEnumerator<ClickHouseTcpParameter> GetEnumerator() => parameters.GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
