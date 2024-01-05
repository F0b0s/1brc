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
        string Town, double Measurement)
    {
        if (dictionary.TryGetValue(Town, out var value))
        {
            value.min = Math.Min(value.min, Measurement);
            value.max = Math.Max(value.max, Measurement);
            value.summ += Measurement;
            value.count++;
        }
        else
        {
            dictionary[Town] = (Measurement, Measurement, Measurement, 1);
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
                ParseRow(batch, lastNewLineIndex, i - lastNewLineIndex, valueTuples);

                lastNewLineIndex = i + 1;
            }
        }
        return lastNewLineIndex;
    }
    
    public static void ParseRow(byte[] batch, int startIndex, int count,
        Dictionary<string, (double min, double max, double summ, int count)> valueTuples)
    {
        var separatorIndexArray = Array.IndexOf(batch,(byte) 0x3B, startIndex);
        var town = Encoding.UTF8.GetString(batch, startIndex, separatorIndexArray - startIndex);
        var measure = ParseDoubleFromBytes(batch, separatorIndexArray + 1, count - (separatorIndexArray - startIndex) - 1); // 4 sec
        AddMeasure(valueTuples, town, measure);
    }

    private static double ParseDoubleFromBytes(byte[] batch, int startIndex, int count)
    {
        double result = 0;
        int index = 0;
        double negativeScale = 0.1;
        bool positive = true;
        bool sign = true;
        
        while (index < count)
        {
            var b = batch[startIndex + index];
            if (b == 0x2D)
            {
                sign = false;
            }
            else if (b != 0x2E)
            {
                if (positive)
                {
                    result = result * 10 + (b - 0x30);
                }
                else
                {
                    result += negativeScale * (b - 0x30);
                    negativeScale /= 10;
                }
            } else
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