﻿// See https://aka.ms/new-console-template for more information


using System.Diagnostics;
using Onebrc;

if (args.Length < 1)
{
    Console.WriteLine("Should be called with parameters");
    return;
}

if (args[0] == "generate")
{
    if (args.Length != 2)
    {
        Console.WriteLine("Enter number of records to create");
        return;
    }
    
    int size = 0;
    try 
    {
        size = int.Parse(args[1]);
    }
    catch (Exception) {
        Console.WriteLine("Invalid value for <number of records to create>");
        return;
    }

    await CreateMeasurements.Generate(size);
}
else if (args[0] == "challenge")
{
    var stopwatch = Stopwatch.StartNew();
    await Challenge.Do();
    Console.WriteLine($"Challenged in {stopwatch}. Memory consumption is: {GC.GetTotalMemory(false)}");
}
else
{
    Console.WriteLine("Unknown command");
}