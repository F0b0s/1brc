# 1️⃣🐝🏎️ The One Billion Row Challenge - .NET Edition

> The One Billion Row Challenge (1BRC [Original Java Challenge](https://github.com/gunnarmorling/1brc)) is a fun exploration of how far modern .NET can be pushed for aggregating one billion rows from a text file.
> Grab all your (virtual) threads, reach out to SIMD, optimize your GC, or pull any other trick, and create the fastest implementation for solving this task!

My result was mentioned at [Official .NET 1BRC implementation](https://github.com/praeclarum/1brc)

# Commands
Run 

```bash
dotnet onebrc.dll generate 100
```

to generate `measurements.txt` with 100 measurements

```bash
dotnet onebrc.dll challenge
```

to process `measurements.txt` in bin directory

## Results
Tested on macOS Monterey 12.5.1 16GB
Apple M1 2.40GHz, 1 CPU, 8 logical and 8 physical cores
.NET SDK=8.0.100

| # | Result (m:s.ms) |    Date   |  Details     |
|---|-----------------|-----------|---------------
| 1.|        01:43.999| 06.01.2024| Single threaded implementation, file is read by chunnks from disk |
| 2.|        00:31.000| 06.01.2024| File is read by chunnks from disk, parsing done with parallelisation |

