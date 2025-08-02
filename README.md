# SparkInsight

Spark auto performance tuning and failure analysis tool.

SparkInsight is a command-line tool for analyzing and tuning Spark applications. It helps identify performance bottlenecks, suggests optimizations, and provides a detailed summary of application metrics.

## Features

*   **Performance Analysis:** Identifies performance bottlenecks in Spark jobs.
*   **Failure Analysis:** Helps to debug and identify the root cause of job failures.
*   **Metrics Summary:** Provides a comprehensive summary of key metrics for Spark applications.
*   **Command-Line Interface:** Easy to use from the command line.

## Usage

To analyze a Spark application, run the following command:

```bash
./bin/spark-insight <app_id>
```

Replace `<app_id>` with your Spark application ID.

## Summarized metrics for all of the jobs:

╔══════════════════════════════╤═══════════╤═════════════════════════════════════════════════════════════════════════════════════════╗
║ Metric name                  │ Value     │ Metric description                                                                      ║
╠══════════════════════════════╪═══════════╪═════════════════════════════════════════════════════════════════════════════════════════╣
║ Disk Spill Size              │ 26.58     │ Total data spilled to disk (in GB)                                                      ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Executor CPU Time            │ 2307      │ Total active CPU time spent by the executor running the main task thread (in minutes)   ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Executor Runtime             │ 4100      │ Total elapsed time spent by the executor running tasks (in minutes)                     ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Executor Runtime w/o Shuffle │ 3669      │ Executor run time excluding shuffle time (in minutes)                                   ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Input Records                │ 108666742 │ Total number of records consumed by tasks (in thousands)                                ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Input Size                   │ 1123      │ Total input data consumed by tasks (in GB)                                              ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ JVM GC Time                  │ 41.99     │ Total time JVM spent in garbage collection (in minutes)                                 ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Memory Spill Size            │ 205       │ Total data spilled to memory (in GB)                                                    ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Net I/O Time                 │ 1320      │ Total time spent accessing external storage (in minutes)                                ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Output Records               │ 0.02      │ Total number of records produced by tasks (in thousands)                                ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────────────────────────── _║
║ Output Size                  │ 0         │ Total output data produced by tasks (in GB)                                             ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Shuffle Read Records         │ 92997219  │ Total number of shuffle records consumed by tasks (in thousands)                        ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Shuffle Read Size            │ 885       │ Total shuffle data consumed by tasks (in GB)                                            ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Shuffle Read Wait Time       │ 359       │ Total time during which tasks were blocked waiting for remote shuffle data (in minutes) ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Shuffle Write Records        │ 86070887  │ Total number of shuffle records produced by tasks (in thousands)                        ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Shuffle Write Size           │ 842       │ Total shuffle data produced by tasks (in GB)                                            ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Shuffle Write Time           │ 72.08     │ Total Shuffle write time spent by tasks (in minutes)                                    ║
╟──────────────────────────────┼───────────┼─────────────────────────────────────────────────────────────────────────────────────────╢
║ Total Runtime                │ 40.74     │ Total elapsed running time (in minutes)                                                 ║
╚══════════════════════════════╧═══════════╧═════════════════════════════════════════════════════════════════════════════════════════╝

## Building from Source

To build the project from source, use Maven:

```bash
mvn clean package
```

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.