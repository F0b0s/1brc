using System.Globalization;
using System.Text;

namespace Onebrc;

public static class Challenge
{
    public static async Task Do()
    {
        var measurements = await GetMeasurements();
        var dictionary = new SortedDictionary<string, List<double>>();
        
        foreach (var measurement in measurements)
        {
            if (dictionary.ContainsKey(measurement.Town))
            {
                dictionary[measurement.Town].Add(measurement.Measurement);
            }
            else
            {
                dictionary[measurement.Town] = new List<double>() {measurement.Measurement};
            }
        }

        using (var outFile = File.Create(CreateMeasurements.OutputFileName))
        {
            await outFile.WriteAsync(Encoding.UTF8.GetBytes("{"));
            
            foreach (var pair in dictionary)
            {
                var min = Math.Round(pair.Value.Min(), 1).ToString(CultureInfo.InvariantCulture);
                var max = Math.Round(pair.Value.Max(), 1).ToString(CultureInfo.InvariantCulture);
                var mean = Math.Round(pair.Value.Sum() / pair.Value.Count, 1).ToString(CultureInfo.InvariantCulture);
                
                await outFile.WriteAsync(Encoding.UTF8.GetBytes($"{pair.Key}={min}/{mean}/{max}, "));
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
        var measurements = new List<TownMeasurement>();
        
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
        var measure = Double.Parse(
            Encoding.UTF8.GetString(span.Slice(separatorIndex + 1, span.Length - separatorIndex - 1)),
            CultureInfo.InvariantCulture
            );
        return new TownMeasurement(town, measure);
    }
}