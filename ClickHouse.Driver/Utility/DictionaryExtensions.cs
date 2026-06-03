using System.Collections.Generic;

namespace ClickHouse.Driver.Utility;

internal static class DictionaryExtensions
{
    public static bool TryAdd<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
    {
        if (dictionary.ContainsKey(key))
            return false;
        dictionary.Add(key, value);
        return true;
    }

    public static void Set<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
    {
        if (dictionary.ContainsKey(key))
            dictionary[key] = value;
        else
            dictionary.Add(key, value);
    }

    public static void SetOrRemove(this IDictionary<string, string> dictionary, string key, string value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            dictionary.Set(key, value);
        }
        else
        {
            dictionary.Remove(key);
        }
    }

    /// <summary>
    /// Compares two read-only dictionaries entry by entry. Treats <c>null</c> and an empty
    /// dictionary as equivalent. Value comparison uses
    /// <see cref="EqualityComparer{TValue}.Default"/> — not deep: nested collections or
    /// objects are compared via their own <c>Equals</c> implementation (reference equality
    /// for reference types unless overridden).
    /// </summary>
    public static bool EntriesEqual<TKey, TValue>(
        this IReadOnlyDictionary<TKey, TValue> a,
        IReadOnlyDictionary<TKey, TValue> b)
    {
        var countA = a?.Count ?? 0;
        var countB = b?.Count ?? 0;
        if (countA != countB) return false;
        if (countA == 0) return true; // both null/empty

        foreach (var kvp in a)
        {
            if (!b.TryGetValue(kvp.Key, out var v) || !EqualityComparer<TValue>.Default.Equals(kvp.Value, v))
                return false;
        }
        return true;
    }
}
