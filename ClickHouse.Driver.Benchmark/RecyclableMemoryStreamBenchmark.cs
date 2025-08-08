using System;
using System.IO;
using System.Net;
using System.Net.Http;
using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Benchmark.References;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Benchmark;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MarkdownExporter]
public class RecyclableMemoryStreamBenchmark
{
    private byte[] smallResponseData;   // 10 KB
    private byte[] mediumResponseData;  // 500 KB  
    private byte[] largeResponseData;   // 5 MB
    private TypeSettings typeSettings;

    [GlobalSetup]
    public void Setup()
    {
        typeSettings = new TypeSettings();
        
        // Create mock responses of different sizes
        smallResponseData = CreateMockBinaryResponse(100, 10);      // 100 rows, 10 columns ~ 10KB
        mediumResponseData = CreateMockBinaryResponse(5000, 10);    // 5000 rows, 10 columns ~ 500KB
        largeResponseData = CreateMockBinaryResponse(50000, 10);    // 50000 rows, 10 columns ~ 5MB
    }

    [Benchmark(Baseline = true)]
    [Arguments(100)]    // Small response
    [Arguments(5000)]   // Medium response
    [Arguments(50000)]  // Large response
    public long ReadAllData_BufferedStream(int rowCount)
    {
        var data = rowCount switch
        {
            100 => smallResponseData,
            5000 => mediumResponseData,
            50000 => largeResponseData,
            _ => throw new ArgumentException()
        };

        using var reader = BufferedStreamClickHouseDataReader.FromHttpResponse(
            CreateMockHttpResponse(data), typeSettings);
        
        long sum = 0;
        var rowsRead = 0;
        while (reader.Read() && rowsRead < rowCount)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetFieldType(i) == typeof(int))
                    sum += reader.GetInt32(i);
            }
            rowsRead++;
        }
        
        return sum;
    }

    [Benchmark]
    [Arguments(100)]    // Small response
    [Arguments(5000)]   // Medium response
    [Arguments(50000)]  // Large response
    public long ReadAllData_RecyclableMemoryStream(int rowCount)
    {
        var data = rowCount switch
        {
            100 => smallResponseData,
            5000 => mediumResponseData,
            50000 => largeResponseData,
            _ => throw new ArgumentException()
        };

        using var reader = ClickHouseDataReader.FromHttpResponse(
            CreateMockHttpResponse(data), typeSettings);
        
        long sum = 0;
        var rowsRead = 0;
        while (reader.Read() && rowsRead < rowCount)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetFieldType(i) == typeof(int))
                    sum += reader.GetInt32(i);
            }
            rowsRead++;
        }
        
        return sum;
    }

    [Benchmark(Baseline = true)]
    public void MultipleSmallQueries_BufferedStream()
    {
        // Simulate multiple small queries - BufferedStream allocates new buffer each time
        for (var i = 0; i < 100; i++)
        {
            using var reader = BufferedStreamClickHouseDataReader.FromHttpResponse(
                CreateMockHttpResponse(smallResponseData), typeSettings);
            
            while (reader.Read())
            {
                _ = reader.GetValue(0);
            }
        }
    }

    [Benchmark]
    public void MultipleSmallQueries_RecyclableStream()
    {
        // Simulate multiple small queries that would benefit from buffer pooling
        for (var i = 0; i < 100; i++)
        {
            using var reader = ClickHouseDataReader.FromHttpResponse(
                CreateMockHttpResponse(smallResponseData), typeSettings);
            
            while (reader.Read())
            {
                _ = reader.GetValue(0);
            }
        }
    }

    private static HttpResponseMessage CreateMockHttpResponse(byte[] data)
    {
        var content = new ByteArrayContent(data);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        
        return new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = content
        };
    }

    private static byte[] CreateMockBinaryResponse(int rows, int columns)
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        
        writer.Write7BitEncodedInt(columns);
        
        for (var i = 0; i < columns; i++)
        {
            writer.Write($"column{i}");
        }
        
        for (var i = 0; i < columns; i++)
        {
            switch (i % 4)
            {
                case 0:
                    writer.Write("Int32");
                    break;
                case 1:
                    writer.Write("String");
                    break;
                case 2:
                    writer.Write("Float64");
                    break;
                case 3:
                    writer.Write("UInt64");
                    break;
            }
        }
        
        for (var row = 0; row < rows; row++)
        {
            for (var col = 0; col < columns; col++)
            {
                switch (col % 4)
                {
                    case 0: // Int32
                        writer.Write(row);
                        break;
                    case 1: // String
                        writer.Write($"row{row}_col{col}"); 
                        break;
                    case 2: // Float64
                        writer.Write(row / 100.0); 
                        break;
                    case 3: // UInt64
                        writer.Write((ulong)row * 1000); 
                        break;
                }
            }
        }
        
        return stream.ToArray();
    }
}
