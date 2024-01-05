using System.Globalization;
using System.Text;

namespace Onebrc;

public static class Challenge
{
    public static async Task Do()
    {
        var dictionary = await GetMeasurements();

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

    private static void AddMeasure(Dictionary<string, (double min, double max, double summ, int count)> dictionary,
        (string Town, double Measurement) measurement)
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

    public static async Task<Dictionary<string, (double min, double max, double summ, int count)>> GetMeasurements()
    {
        var dictionary = new Dictionary<string, (double min, double max, double summ, int count)>();
        using (var reader = File.OpenRead(CreateMeasurements.InputFileName))
        {
            int index = 0,
                batchSize = 4096 * 128;
            var buffer = new byte[batchSize];

            while (await reader.ReadAsync(buffer, index, batchSize - index) != 0)
            {
                var bytesRead = ParseBatch(buffer, dictionary);
                if (bytesRead != batchSize)
                {
                    Array.Copy(buffer, bytesRead, buffer, 0, batchSize - bytesRead);
                    Array.Clear(buffer, batchSize - bytesRead, bytesRead);
                    index = batchSize - bytesRead;
                }
                else
                {
                    index = 0;
                }
            }
        }

        return dictionary;
    }

    public static int ParseBatch(byte[] batch, Dictionary<string, (double min, double max, double summ, int count)> valueTuples)
    {
        int lastNewLineIndex = 0;

        for (int i = 0; i < batch.Length; i++)
        {
            if (batch[i] == 0x0A)
            {
                var townMeasurement = ParseRow(new Span<byte>(batch, lastNewLineIndex, i - lastNewLineIndex));
                AddMeasure(valueTuples, townMeasurement);
                lastNewLineIndex = i + 1;
            }
        }
        return lastNewLineIndex;
    }

    public record TownMeasurement(string Town, double Measurement);
    
    public static (string, double) ParseRow(Span<byte> span)
    {
        var separatorIndex = span.IndexOf((byte) 0x3B);
        var town = Encoding.UTF8.GetString(span.Slice(0, separatorIndex));
        var measure = ParseDoubleFromBytes(span.Slice(separatorIndex + 1, span.Length - separatorIndex - 1));
        return new ValueTuple<string, double>(town, measure);
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