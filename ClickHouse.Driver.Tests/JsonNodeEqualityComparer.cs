using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace ClickHouse.Driver.Tests;

internal class JsonNodeEqualityComparer : IComparer<JsonObject>
{
    public int Compare(JsonObject x, JsonObject y)
    {
#if NET6_0
        return DeepCompareJsonNodes(x, y) ? 0 : 1;
#else
        return JsonNode.DeepEquals(x, y) ? 0 : 1;
#endif
    }

#if NET6_0
    private static bool DeepCompareJsonNodes(JsonNode x, JsonNode y)
    {
        if (x == null && y == null) return true;
        if (x == null || y == null) return false;

        if (x is JsonObject xObject && y is JsonObject yObject)
        {
            if (xObject.Count != yObject.Count)
                return false;
            
            foreach (var property in xObject)
            {
                if (!yObject.TryGetPropertyValue(property.Key, out var yValue))
                    return false;
                
                if (!DeepCompareJsonNodes(property.Value, yValue))
                    return false;
            }

            return true;
        }

        if (x is JsonArray xArray && y is JsonArray yArray)
        {
            if (xArray.Count != yArray.Count)
                return false;
            
            for (var i = 0; i < xArray.Count; i++)
            {
                if (!DeepCompareJsonNodes(xArray[i], yArray[i]))
                    return false;
            }

            return true;
        }

        if (x is JsonValue xVal && y is JsonValue yVal)
            return xVal.ToJsonString() == yVal.ToJsonString();

        return false;
    }
#endif
}
