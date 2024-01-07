using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Text;
using Microsoft.Win32.SafeHandles;

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
        var stopWatch = Stopwatch.StartNew();
        var consumers = StartConsumers();   
        //await GetMeasurements(0, new System.IO.FileInfo(CreateMeasurements.InputFileName).Length);
        var readers = StartReaders();
        await Task.WhenAll(readers);
        
        _piecesOfWorks.CompleteAdding();
        
        await Task.WhenAll(consumers);
        Console.WriteLine("Done parsing: " + DateTime.UtcNow);
        
        var dictionary = Aggregate(consumers.Select(x => x.Result).ToList());

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

    public static async Task GetMeasurements(long startIndex, long count)
    {
        using (var reader = File.OpenRead(CreateMeasurements.InputFileName))
        {
            int index = 0,
                batchSize = 8192;
            var buffer = new byte[batchSize];
            reader.Seek(startIndex, SeekOrigin.Begin);
            while (count >= 0)
            {
                var read = await reader.ReadAsync(buffer, index, batchSize - index);
                if (read == 0)
                    break;
                count -= read;
                
                var bytesRead = ParseBatch(buffer);
                var temp = new byte[batchSize];
                
                if (bytesRead != batchSize)
                {
                    Array.Copy(buffer, bytesRead, temp, 0, batchSize - bytesRead);
                    index = batchSize - bytesRead;
                }
                else
                {
                    index = 0;
                }

                buffer = temp;
            }
        }
    }

    public record PieceOfWork(byte[] array, int startIndex);
    public static BlockingCollection<List<PieceOfWork>> _piecesOfWorks = 
        new BlockingCollection<List<PieceOfWork>>(new ConcurrentBag<List<PieceOfWork>>());
    
    public static int ParseBatch(byte[] batch)
    {
        int lastNewLineIndex = 0;
        var list = new List<PieceOfWork>(400);
        
        while (lastNewLineIndex < batch.Length)
        {
            var index = Array.IndexOf(batch, (byte)0x0A, lastNewLineIndex);
            if (index == -1)
                return lastNewLineIndex;
            
            list.Add(new PieceOfWork(batch, lastNewLineIndex));
            lastNewLineIndex = index + 1;
        }
        
        //Console.WriteLine("Batch size: " + _piecesOfWorks.Count);
        _piecesOfWorks.Add(list);
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

    public static Dictionary<string, Town> Consumer()
    {
        var dictionary = new Dictionary<string, Town>();
        
        while (!_piecesOfWorks.IsCompleted)
        {

            List<PieceOfWork> data = null;
            try
            {
                data = _piecesOfWorks.Take();
            }
            catch (InvalidOperationException) { }

            if (data != null)
            {
                foreach (var pieceOfWork in data)
                {
                    ParseRow(pieceOfWork.array, pieceOfWork.startIndex, dictionary);                    
                }
            }
        }

        return dictionary;
    }

    public static List<Task> StartReaders()
    {
        var fileSize = new System.IO.FileInfo(CreateMeasurements.InputFileName).Length;
        var middle = fileSize / 3;
        long index = 0;
        using (var reader = File.OpenRead(CreateMeasurements.InputFileName))
        {
            
            reader.Seek(middle, SeekOrigin.Begin);
            var buffer = new byte[1024];
            var _ = reader.Read(buffer, 0, 1024);

            index = Array.IndexOf(buffer, (byte)0x0A);
        }

        var task1 = Task.Run(() => GetMeasurements(0, middle + index - 1));
        var task2 = Task.Run(() => GetMeasurements(middle + index + 1, fileSize - (middle - 1)));

        return new List<Task>() {task1, task2};
    }
    
    public static List<Task<Dictionary<string, Town>>> StartConsumers()
    {
        var tasks = new List<Task<Dictionary<string, Town>>>();

        for (int i = 0; i < Environment.ProcessorCount - 2; i++)
        {
            var task = Task.Run(Consumer);
            tasks.Add(task);
        }

        return tasks;
    }

    public static Dictionary<string, Town> Aggregate(List<Dictionary<string, Town>> dictionaries)
    {
        var result = dictionaries.First();

        for (int i = 1; i < dictionaries.Count; i++)
        {
            foreach (var key in result.Keys)
            {
                var resultValue = result[key];
                var anotherValue = dictionaries[i][key];

                if (resultValue.Min > anotherValue.Min)
                    resultValue.Min = anotherValue.Min;
                
                if (resultValue.Max < anotherValue.Max)
                    resultValue.Max = anotherValue.Max;

                resultValue.Sum += anotherValue.Sum;
                resultValue.Count += anotherValue.Count;
            }
        }

        return result;
    }
}