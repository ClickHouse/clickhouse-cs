using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.ADO.Parameters;

public class ClickHouseParameterCollection : DbParameterCollection
{
    private const int MinimumIndexedCount = 64;
    private readonly List<ClickHouseDbParameter> parameters = new();
    private NameIndex nameIndex;
    private bool scanNames;

    public override int Count => parameters.Count;

    public override object SyncRoot { get; }

    public override int Add(object value)
    {
        var parameter = (ClickHouseDbParameter)value;
        var index = parameters.Count;
        parameters.Add(parameter);
        if (nameIndex?.IsValid == true && !nameIndex.TryAdd(parameter, index))
            ResetNameIndex(scan: true);
        return index;
    }

    public override void AddRange(Array values)
    {
        ResetNameIndex();
        parameters.AddRange(values.Cast<ClickHouseDbParameter>());
    }

    public override void Clear()
    {
        ResetNameIndex();
        parameters.Clear();
    }

    public override bool Contains(object value) => parameters.Contains(value as ClickHouseDbParameter);

    public override bool Contains(string value) =>
        GetNameIndex(value)?.IndexOf(value) is int index ? index >= 0 : ContainsNameByScan(value);

    private bool ContainsNameByScan(string value) => parameters.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index)
    {
        for (int i = 0; i < parameters.Count; i++)
        {
            array.SetValue(parameters[i].Value, index + i);
        }
    }

    public override IEnumerator GetEnumerator() => parameters.GetEnumerator();

    public override int IndexOf(object value) => parameters.IndexOf(value as ClickHouseDbParameter);

    public override int IndexOf(string parameterName) =>
        GetNameIndex(parameterName)?.IndexOf(parameterName) ?? IndexOfNameByScan(parameterName);

    private int IndexOfNameByScan(string parameterName) =>
        parameters.FindIndex(x => x.ParameterName == parameterName);

    public override void Insert(int index, object value)
    {
        ResetNameIndex();
        parameters.Insert(index, (ClickHouseDbParameter)value);
    }

    public override void Remove(object value)
    {
        if (parameters.Remove(value as ClickHouseDbParameter))
            ResetNameIndex();
    }

    public override void RemoveAt(int index)
    {
        ResetNameIndex();
        parameters.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
        ResetNameIndex();
        parameters.RemoveAll(p => p.ParameterName == parameterName);
    }

    protected override DbParameter GetParameter(int index) => parameters[index];

    protected override DbParameter GetParameter(string parameterName) => parameters[IndexOf(parameterName)];

    protected override void SetParameter(int index, DbParameter value)
    {
        ResetNameIndex();
        parameters[index] = (ClickHouseDbParameter)value;
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index < 0)
            Add(value);
        else
            SetParameter(index, value);
    }

    public override string ToString() => string.Join(";", parameters);

    private NameIndex GetNameIndex(string name)
    {
        if (name is null || scanNames || parameters.Count < MinimumIndexedCount)
            return null;
        if (nameIndex?.IsValid == true)
            return nameIndex;

        nameIndex = new NameIndex(parameters.Count);
        for (var index = 0; index < parameters.Count; index++)
        {
            if (!nameIndex.TryAdd(parameters[index], index))
            {
                ResetNameIndex(scan: true);
                return null;
            }
        }
        return nameIndex;
    }

    private void ResetNameIndex(bool scan = false)
    {
        nameIndex?.Invalidate();
        nameIndex = null;
        scanNames = scan;
    }

    internal sealed class NameIndex
    {
        private readonly Dictionary<string, int> positions;
        private readonly WeakReference<NameIndex> observer;
        private volatile bool isValid = true;

        internal NameIndex(int capacity)
        {
            positions = new Dictionary<string, int>(capacity, StringComparer.Ordinal);
            observer = new WeakReference<NameIndex>(this);
        }

        internal bool IsValid => isValid;

        internal void Invalidate() => isValid = false;

        internal int IndexOf(string name) => positions.TryGetValue(name, out var index) ? index : -1;

        internal bool TryAdd(ClickHouseDbParameter parameter, int index)
        {
            // A subclass can override the getter without calling the base setter.
            if (parameter?.GetType() != typeof(ClickHouseDbParameter) || !parameter.TryObserveName(observer))
                return false;

            if (parameter.ParameterName is { } name)
                positions.TryAdd(name, index);
            return true;
        }
    }

    /// <summary>
    /// Resolves the ClickHouse type name for each parameter exactly once.
    /// The returned dictionary is used for both SQL placeholder generation and HTTP value formatting,
    /// ensuring consistency.
    /// </summary>
    /// <param name="sqlQuery">The SQL query, used to extract {name:Type} hints.</param>
    /// <param name="resolver">Custom resolver from settings, or null.</param>
    internal IReadOnlyDictionary<string, string> ResolveTypeNames(string sqlQuery, IParameterTypeResolver resolver)
    {
        var typeHints = SqlParameterTypeExtractor.ExtractTypeHints(sqlQuery);
        var resolved = new Dictionary<string, string>(parameters.Count);

        foreach (var parameter in parameters)
        {
            typeHints.TryGetValue(parameter.ParameterName, out var sqlTypeHint);
            // TryAdd: parameter collection can in theory contain duplicate names
            resolved.TryAdd(
                parameter.ParameterName,
                ParameterTypeResolution.ResolveTypeName(parameter, sqlTypeHint, resolver));
        }

        return resolved;
    }

    /// <summary>
    /// Replaces @-style parameter placeholders with {name:Type} ClickHouse native syntax
    /// using pre-resolved type names. Placeholders inside string literals, quoted identifiers,
    /// heredocs and comments are left as they are.
    /// </summary>
    /// <param name="sqlQuery">The SQL query with @-style placeholders.</param>
    /// <param name="resolvedTypeNames">Pre-resolved type names per parameter (keyed by parameter name).</param>
    internal string ReplacePlaceholders(string sqlQuery, IReadOnlyDictionary<string, string> resolvedTypeNames)
    {
        if (FeatureSwitch.DisableReplacingParameters || parameters.Count == 0)
            return sqlQuery;

        var replacements = new Dictionary<string, string>();
        // Using foreach+TryAdd as parameter collection can in theory contain duplicate names
        foreach (var p in parameters)
        {
            resolvedTypeNames.TryGetValue(p.ParameterName, out var typeName);
            replacements.TryAdd("@" + p.ParameterName, $"{{{p.ParameterName}:{typeName}}}");
        }

        return SqlPlaceholderRewriter.ReplacePlaceholders(sqlQuery, replacements);
    }
}
