using System.Globalization;
using System.Text;

namespace Onebrc;

public static class Challenge
{
    public class Town
    {
        public double Min;
        public double Max;
        public double Sum;
        public int Count;
    }
    
    public static async Task Do()
    {
        var dictionary = await GetMeasurements();

        using (var outFile = File.Create(CreateMeasurements.OutputFileName))
        {
            await outFile.WriteAsync(Encoding.UTF8.GetBytes("{"));
            
            foreach (var town in dictionary.Keys.OrderBy(x => x).ToArray())
            {
                var pair = dictionary[town];
                var min = Math.Round(pair.Min, 1).ToString(CultureInfo.InvariantCulture);
                var max = Math.Round(pair.Max, 1).ToString(CultureInfo.InvariantCulture);
                var mean = Math.Round(pair.Sum / pair.Count, 1).ToString(CultureInfo.InvariantCulture);
                
                await outFile.WriteAsync(Encoding.UTF8.GetBytes($"{town}={min}/{mean}/{max}, "));
            }
            
            await outFile.WriteAsync(Encoding.UTF8.GetBytes("}"));
        }
    }

    private static void AddMeasure(Dictionary<string, Town> dictionary,
        string Town, double Measurement)
    {
        if (dictionary.TryGetValue(Town, out var value))
        {
            if (Measurement < value.Min)
                value.Min = Measurement;

            if (Measurement > value.Max)
                value.Max = Measurement;
            
            value.Sum += Measurement;
            value.Count++;
            dictionary[Town] = value;
        }
        else
        {
            var town = new Town();
            town.Min = town.Max = town.Sum = Measurement;
            town.Count = 1;
            dictionary[Town] = town;
        }
    }

    public static async Task<Dictionary<string, Town>> GetMeasurements()
    {
        var dictionary = new Dictionary<string, Town>();
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

    public static int ParseBatch(byte[] batch, Dictionary<string, Town> valueTuples)
    {
        int lastNewLineIndex = 0;
        
        while (lastNewLineIndex < batch.Length)
        {
            var index = Array.IndexOf(batch, (byte)0x0A, lastNewLineIndex);
            if (index == -1)
                return lastNewLineIndex;
            
            ParseRow(batch, lastNewLineIndex, valueTuples);
            lastNewLineIndex = index + 1;
        }
        
        return lastNewLineIndex;
    }
    
    public static void ParseRow(byte[] batch, int startIndex,
        Dictionary<string, Town> valueTuples)
    {
        var separatorIndexArray = Array.IndexOf(batch,(byte) 0x3B, startIndex); 
        var town = Encoding.UTF8.GetString(batch, startIndex, separatorIndexArray - startIndex);
        var measure = ParseDoubleFromBytes(batch, separatorIndexArray + 1);
        AddMeasure(valueTuples, town, measure);
    }

    public static double ParseDoubleFromBytes(byte[] batch, int startIndex)
    {
        int pointIndex = Array.IndexOf(batch,(byte) 0x2E, startIndex);
        if (batch[startIndex] == 0x2D)
        {
            if (pointIndex - startIndex > 2)
            {
                // negative 11.1
                return  ((batch[pointIndex - 2] - 0x30) * 10 + (batch[pointIndex - 1] - 0x30) + (batch[pointIndex + 1] - 0x30) * 0.1) * -1;
            }
            else
            {
                // negative 1.1
                return  ((batch[pointIndex - 1] - 0x30) + (batch[pointIndex + 1] - 0x30) * 0.1) * -1;
            }
        }
        else
        {
            if (pointIndex - startIndex > 1)
            {
                // positive 11.1
                return  (batch[pointIndex - 2] - 0x30) * 10 + (batch[pointIndex - 1] - 0x30) + (batch[pointIndex + 1] - 0x30) * 0.1;
            }
            else
            {
                // positive 1.1
                return  (batch[pointIndex - 1] - 0x30) + (batch[pointIndex + 1] - 0x30) * 0.1;
            }
        }
    }
}