using System.Globalization;
using System.Text;

namespace Onebrc;

public static class Challenge
{
    public static async Task Do()
    {
        var measurements = await GetMeasurements();
        var dictionary = new Dictionary<string, (double min, double max, double summ, int count)>();

        foreach (var measurement in measurements)
        {
            if (dictionary.TryGetValue(measurement.Town, out var value))
            {
                value.min = Math.Min(value.min, measurement.Measurement);
                value.max = Math.Max(value.max, measurement.Measurement);
                value.summ += measurement.Measurement;
                value.count++;
            }
            else
            {
                dictionary[measurement.Town] = (measurement.Measurement, measurement.Measurement,
                    measurement.Measurement, 1);
            }
        }

        using (var outFile = File.Create(CreateMeasurements.OutputFileName))
        {
            await outFile.WriteAsync(Encoding.UTF8.GetBytes("{"));
            
            foreach (var town in dictionary.Keys.OrderBy(x => x).ToArray())
            {
                var pair = dictionary[town];
                var min = Math.Round(pair.min, 1).ToString(CultureInfo.InvariantCulture);
                var max = Math.Round(pair.max, 1).ToString(CultureInfo.InvariantCulture);
                var mean = Math.Round(pair.summ / pair.count, 1).ToString(CultureInfo.InvariantCulture);
                
                await outFile.WriteAsync(Encoding.UTF8.GetBytes($"{town}={min}/{mean}/{max}, "));
            }
            
            await outFile.WriteAsync(Encoding.UTF8.GetBytes("}"));
        }
    }

    public static async Task<IEnumerable<TownMeasurement>> GetMeasurements()
    {
        var measurements = new List<TownMeasurement>();
        
        using (var reader = File.OpenRead(CreateMeasurements.InputFileName))
        {
            int index = 0,
                batchSize = 4096;
            var buffer = new byte[batchSize];

            while (await reader.ReadAsync(buffer, index, batchSize - index) != 0)
            {
                var row = ParseBatch(buffer);
                measurements.AddRange(row.measurements);
                if (row.bytesRead != batchSize)
                {
                    Array.Copy(buffer, row.bytesRead, buffer, 0, batchSize - row.bytesRead);
                    Array.Clear(buffer, batchSize - row.bytesRead, row.bytesRead);
                    index = batchSize - row.bytesRead;
                }
                else
                {
                    index = 0;
                }
            }
        }

        return measurements;
    }

    public static (IEnumerable<TownMeasurement> measurements, int bytesRead) ParseBatch(byte[] batch)
    {
        int index = 0;
        int lastNewLineIndex = 0;
        var measurements = new List<TownMeasurement>(100);
        
        for (int i = 0; i < batch.Length; i++)
        {
            if (batch[i] == 0x0A)
            {
                var townMeasurement = ParseRow(new Span<byte>(batch, lastNewLineIndex, i - lastNewLineIndex));
                measurements.Add(townMeasurement);
                lastNewLineIndex = i + 1;
            }
        }
        return (measurements, lastNewLineIndex);
    }

    public record TownMeasurement(string Town, double Measurement);
    
    public static TownMeasurement ParseRow(Span<byte> span)
    {
        var separatorIndex = span.IndexOf((byte) 0x3B);
        var town = Encoding.UTF8.GetString(span.Slice(0, separatorIndex));
        var measure = ParseDoubleFromBytes(span.Slice(separatorIndex + 1, span.Length - separatorIndex - 1));
        return new TownMeasurement(town, measure);
    }

    private static double ParseDoubleFromBytes(Span<byte> span)
    {
        double result = 0;
        int index = 0;
        double negativeScale = 0.1;
        bool positive = true;
        bool sign = true;
        
        while (index < span.Length)
        {
            if (span[index] == 0x2D)
                sign = false;
            
            if (span[index] != 0x2E)
            {
                if(positive)
                    result = result * 10 + (span[index] - 0x30);
                else
                {
                    result += negativeScale * (span[index] - 0x30);
                    negativeScale /= 10;
                }
            }

            if (span[index] == 0x2E)
            {
                positive = false;
            }

            index++;
        }

        if (!sign)
            result = -result;

        return result;
    }
}