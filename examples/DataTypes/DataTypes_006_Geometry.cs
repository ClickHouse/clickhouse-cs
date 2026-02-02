using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates working with ClickHouse geometry types and geo functions.
/// Covers:
/// - Basic geometry types (Point, Polygon)
/// - WKT (Well-Known Text) parsing
/// - H3 geospatial indexing
/// - Point-in-polygon containment checks
/// - Great circle distance calculations
/// </summary>
public static class GeometryTypes
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("Geometry Types Examples\n");

        // Example 1: Basic geometry types
        Console.WriteLine("1. Basic Geometry Types (Point, Polygon):");
        await Example1_BasicGeometryTypes(connection);

        // Example 2: WKT support
        Console.WriteLine("\n2. WKT (Well-Known Text) Support:");
        await Example2_WktSupport(connection);

        // Example 3: H3 indexing
        Console.WriteLine("\n3. H3 Geospatial Indexing:");
        await Example3_H3Indexing(connection);

        // Example 4: Point in polygon
        Console.WriteLine("\n4. Point-in-Polygon Containment:");
        await Example4_PointInPolygon(connection);

        // Example 5: Great circle distance
        Console.WriteLine("\n5. Great Circle Distance:");
        await Example5_GreatCircleDistance(connection);

        Console.WriteLine("\nAll geometry examples completed!");
    }

    /// <summary>
    /// Demonstrates basic Point and Polygon types with table creation, insertion, and reading.
    /// Points are represented as Tuple&lt;double, double&gt; (x, y).
    /// Polygons are arrays of rings, where each ring is an array of points.
    /// </summary>
    private static async Task Example1_BasicGeometryTypes(ClickHouseConnection connection)
    {
        var tableName = "example_geometry_basic";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                location Point,
                boundary Polygon
            )
            ENGINE = MergeTree()
            ORDER BY id
        ");

        Console.WriteLine($"   Created table '{tableName}' with Point and Polygon columns");

        // Insert data with geometry types
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                INSERT INTO {tableName} (id, location, boundary)
                VALUES ({{id:UInt32}}, {{location:Point}}, {{boundary:Polygon}})
            ";

            // Point is represented as Tuple<double, double> (x, y) or (longitude, latitude)
            command.AddParameter("id", 1);
            command.AddParameter("location", Tuple.Create(4.9041, 52.3676)); // Amsterdam coordinates (lon, lat)

            // Polygon is an array of rings. First ring is outer boundary, subsequent rings are holes.
            // Each ring is an array of points, with the last point equal to the first (closed ring).
            // This polygon roughly covers central Amsterdam
            var outerRing = new[]
            {
                Tuple.Create(4.85, 52.35),
                Tuple.Create(4.95, 52.35),
                Tuple.Create(4.95, 52.40),
                Tuple.Create(4.85, 52.40),
                Tuple.Create(4.85, 52.35) // Close the ring
            };
            command.AddParameter("boundary", new[] { outerRing });

            await command.ExecuteNonQueryAsync();
        }

        Console.WriteLine("   Inserted row with Point and Polygon data");

        // Read back the geometry data
        using (var reader = await connection.ExecuteReaderAsync($"SELECT id, location, boundary FROM {tableName}"))
        {
            Console.WriteLine("\n   Reading geometry data:");
            while (reader.Read())
            {
                var id = reader.GetFieldValue<uint>(0);
                var location = reader.GetFieldValue<Tuple<double, double>>(1);
                var boundary = reader.GetFieldValue<Tuple<double, double>[][]>(2);

                Console.WriteLine($"\n   ID: {id}");
                Console.WriteLine($"     Location (Point): ({location.Item1}, {location.Item2})");
                Console.WriteLine($"     Boundary (Polygon): {boundary.Length} ring(s), outer ring has {boundary[0].Length} points");
            }
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Demonstrates WKT (Well-Known Text) parsing functions for creating geometry from text.
    /// WKT is a standard text format for representing geometry objects.
    /// </summary>
    private static async Task Example2_WktSupport(ClickHouseConnection connection)
    {
        // Parse Point from WKT
        Console.WriteLine("   Parsing Point from WKT:");
        using (var reader = await connection.ExecuteReaderAsync("SELECT readWKTPoint('POINT (37.6173 55.7558)')"))
        {
            if (reader.Read())
            {
                var point = reader.GetFieldValue<Tuple<double, double>>(0);
                Console.WriteLine($"     'POINT (37.6173 55.7558)' -> ({point.Item1}, {point.Item2})");
            }
        }

        // Parse Polygon from WKT
        Console.WriteLine("\n   Parsing Polygon from WKT:");
        using (var reader = await connection.ExecuteReaderAsync(@"
            SELECT readWKTPolygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')
        "))
        {
            if (reader.Read())
            {
                var polygon = reader.GetFieldValue<Tuple<double, double>[][]>(0);
                Console.WriteLine($"     Parsed polygon with {polygon.Length} ring(s)");
                Console.WriteLine($"     Outer ring points: {string.Join(", ", polygon[0].Select(p => $"({p.Item1},{p.Item2})"))}");
            }
        }

        // Parse Ring from WKT - Ring uses POLYGON format
        Console.WriteLine("\n   Parsing Ring from WKT:");
        using (var reader = await connection.ExecuteReaderAsync(@"
            SELECT readWKTRing('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')
        "))
        {
            if (reader.Read())
            {
                var ring = reader.GetFieldValue<Tuple<double, double>[]>(0);
                Console.WriteLine($"     Parsed ring with {ring.Length} points");
            }
        }

        // Parse MultiPolygon from WKT
        Console.WriteLine("\n   Parsing MultiPolygon from WKT:");
        using (var reader = await connection.ExecuteReaderAsync(@"
            SELECT readWKTMultiPolygon('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))')
        "))
        {
            if (reader.Read())
            {
                var multiPolygon = reader.GetFieldValue<Tuple<double, double>[][][]>(0);
                Console.WriteLine($"     Parsed MultiPolygon with {multiPolygon.Length} polygon(s)");
            }
        }
    }

    /// <summary>
    /// Demonstrates H3 geospatial indexing functions.
    /// H3 is a hierarchical hexagonal grid system for indexing geographic coordinates.
    /// Note: As of ClickHouse 25.5, geoToH3() uses (lat, lon) order.
    /// </summary>
    private static async Task Example3_H3Indexing(ClickHouseConnection connection)
    {
        // Basic geoToH3 conversion
        Console.WriteLine("   Converting coordinates to H3 index:");

        // Amsterdam coordinates
        var lat = 52.33676;
        var lon = 4.9041;

        using (var command = connection.CreateCommand())
        {
            // geoToH3(lat, lon, resolution) - note: lat, lon order as of ClickHouse 25.5
            command.CommandText = @"
                SELECT
                    geoToH3({lat:Float64}, {lon:Float64}, 5) AS h3_res5,
                    geoToH3({lat:Float64}, {lon:Float64}, 10) AS h3_res10,
                    geoToH3({lat:Float64}, {lon:Float64}, 15) AS h3_res15
            ";
            command.AddParameter("lat", lat);
            command.AddParameter("lon", lon);

            using (var reader = await command.ExecuteReaderAsync())
            {
                if (reader.Read())
                {
                    var h3Res5 = reader.GetFieldValue<ulong>(0);
                    var h3Res10 = reader.GetFieldValue<ulong>(1);
                    var h3Res15 = reader.GetFieldValue<ulong>(2);

                    Console.WriteLine($"     Coordinates: ({lat}, {lon}) - Amsterdam");
                    Console.WriteLine($"     H3 Resolution 5:  {h3Res5}");
                    Console.WriteLine($"     H3 Resolution 10: {h3Res10}");
                    Console.WriteLine($"     H3 Resolution 15: {h3Res15}");
                }
            }
        }

        // Convert H3 index back to coordinates
        Console.WriteLine("\n   Converting H3 index back to coordinates:");
        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT h3ToGeo(geoToH3({lat}, {lon}, 10)) AS center_point
        "))
        {
            if (reader.Read())
            {
                var centerPoint = reader.GetFieldValue<Tuple<double, double>>(0);
                Console.WriteLine($"     H3 cell center: ({centerPoint.Item1}, {centerPoint.Item2})");
            }
        }

        // Get H3 cell boundary as polygon
        Console.WriteLine("\n   Getting H3 cell boundary:");
        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT h3ToGeoBoundary(geoToH3({lat}, {lon}, 8)) AS boundary
        "))
        {
            if (reader.Read())
            {
                var boundary = reader.GetFieldValue<Tuple<double, double>[]>(0);
                Console.WriteLine($"     H3 cell boundary has {boundary.Length} vertices (hexagon)");
            }
        }

        // Practical example: Spatial aggregation using H3
        Console.WriteLine("\n   Practical: Grouping locations by H3 cell:");
        var tableName = "example_h3_locations";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                name String,
                lat Float64,
                lon Float64
            )
            ENGINE = MergeTree()
            ORDER BY name
        ");

        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} (name, lat, lon)
            VALUES
                ('Location A', 52.3731, 4.8936),
                ('Location B', 52.3735, 4.8940),
                ('Location C', 52.3750, 4.8950),
                ('Location D', 52.3900, 4.9100)
        ");

        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT
                geoToH3(lat, lon, 7) AS h3_cell,
                count() AS location_count,
                groupArray(name) AS locations
            FROM {tableName}
            GROUP BY h3_cell
            ORDER BY location_count DESC
        "))
        {
            Console.WriteLine("     H3 Cell (res 7)     | Count | Locations");
            Console.WriteLine("     --------------------|-------|----------");
            while (reader.Read())
            {
                var h3Cell = reader.GetFieldValue<ulong>(0);
                var count = reader.GetFieldValue<ulong>(1);
                var locations = reader.GetFieldValue<string[]>(2);
                Console.WriteLine($"     {h3Cell,-19} | {count,-5} | [{string.Join(", ", locations)}]");
            }
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Demonstrates point-in-polygon containment checks.
    /// Useful for geofencing, area-based filtering, and spatial queries.
    /// </summary>
    private static async Task Example4_PointInPolygon(ClickHouseConnection connection)
    {
        // Define a simple rectangular polygon (e.g., a geofence)
        Console.WriteLine("   Basic point-in-polygon check:");

        // Polygon vertices (clockwise or counter-clockwise)
        // This represents a rectangular area
        using (var reader = await connection.ExecuteReaderAsync(@"
            SELECT
                pointInPolygon((5, 5), [(0, 0), (10, 0), (10, 10), (0, 10)]) AS inside,
                pointInPolygon((15, 15), [(0, 0), (10, 0), (10, 10), (0, 10)]) AS outside
        "))
        {
            if (reader.Read())
            {
                var inside = reader.GetFieldValue<byte>(0);
                var outside = reader.GetFieldValue<byte>(1);
                Console.WriteLine($"     Point (5,5) in rectangle [0-10, 0-10]: {(inside == 1 ? "INSIDE" : "OUTSIDE")}");
                Console.WriteLine($"     Point (15,15) in rectangle [0-10, 0-10]: {(outside == 1 ? "INSIDE" : "OUTSIDE")}");
            }
        }

        // Practical example: Filtering locations within a geofence
        Console.WriteLine("\n   Practical: Filtering locations within a geofence:");
        var tableName = "example_geofence";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                device_id String,
                lat Float64,
                lon Float64
            )
            ENGINE = MergeTree()
            ORDER BY device_id
        ");

        // Insert some device locations around Amsterdam
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} (device_id, lat, lon)
            VALUES
                ('device_1', 52.3731, 4.8936),
                ('device_2', 52.3752, 4.8840),
                ('device_3', 52.3105, 4.7683),
                ('device_4', 52.3600, 4.8852)
        ");

        // Define a geofence polygon around central Amsterdam
        // Check which devices are inside the geofence
        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT
                device_id,
                lat,
                lon,
                pointInPolygon(
                    (lon, lat),
                    [(4.85, 52.35), (4.92, 52.35), (4.92, 52.40), (4.85, 52.40)]
                ) AS in_geofence
            FROM {tableName}
            ORDER BY device_id
        "))
        {
            Console.WriteLine("     Device     | Lat      | Lon      | In Geofence");
            Console.WriteLine("     -----------|----------|----------|------------");
            while (reader.Read())
            {
                var deviceId = reader.GetString(0);
                var deviceLat = reader.GetFieldValue<double>(1);
                var deviceLon = reader.GetFieldValue<double>(2);
                var inGeofence = reader.GetFieldValue<byte>(3);
                Console.WriteLine($"     {deviceId,-10} | {deviceLat:F4} | {deviceLon:F4} | {(inGeofence == 1 ? "YES" : "NO")}");
            }
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Demonstrates great circle distance calculations between geographic coordinates.
    /// Uses the haversine formula to calculate the shortest distance over the Earth's surface.
    /// </summary>
    private static async Task Example5_GreatCircleDistance(ClickHouseConnection connection)
    {
        // Calculate distance between two cities
        Console.WriteLine("   Distance between cities:");

        // City coordinates (lon, lat)
        var cities = new[]
        {
            ("Amsterdam", 4.9041, 52.3676),
            ("London", -0.1276, 51.5074),
            ("New York", -74.006, 40.7128),
            ("Tokyo", 139.6917, 35.6895),
            ("Sydney", 151.2093, -33.8688)
        };

        // Calculate distances from Amsterdam to other cities
        Console.WriteLine("     From Amsterdam to:");
        foreach (var (name, lon, lat) in cities.Skip(1))
        {
            using (var reader = await connection.ExecuteReaderAsync($@"
                SELECT greatCircleDistance(4.9041, 52.3676, {lon}, {lat}) AS distance_meters
            "))
            {
                if (reader.Read())
                {
                    var distanceMeters = reader.GetFieldValue<double>(0);
                    var distanceKm = distanceMeters / 1000;
                    var distanceMiles = distanceKm * 0.621371;
                    Console.WriteLine($"       {name,-12}: {distanceKm:N0} km ({distanceMiles:N0} miles)");
                }
            }
        }

        // Practical example: Find nearby locations
        Console.WriteLine("\n   Practical: Finding nearby locations:");
        var tableName = "example_locations_distance";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                name String,
                lon Float64,
                lat Float64
            )
            ENGINE = MergeTree()
            ORDER BY name
        ");

        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} (name, lon, lat)
            VALUES
                ('Dam Square', 4.8936, 52.3731),
                ('Anne Frank House', 4.8840, 52.3752),
                ('Rijksmuseum', 4.8852, 52.3600),
                ('Amsterdam Centraal', 4.9003, 52.3791),
                ('Schiphol Airport', 4.7683, 52.3105)
        ");

        // Find locations within 5km of Dam Square
        var centerLon = 4.8936;
        var centerLat = 52.3731;
        var radiusMeters = 5000;

        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT
                name,
                greatCircleDistance({centerLon}, {centerLat}, lon, lat) AS distance
            FROM {tableName}
            WHERE greatCircleDistance({centerLon}, {centerLat}, lon, lat) <= {radiusMeters}
            ORDER BY distance
        "))
        {
            Console.WriteLine($"     Locations within {radiusMeters / 1000}km of Dam Square:");
            while (reader.Read())
            {
                var name = reader.GetString(0);
                var distance = reader.GetFieldValue<double>(1);
                Console.WriteLine($"       {name}: {distance:N0} meters");
            }
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }
}
