# SparkInsight

Spark auto performance tuning and failure analysis tool.

SparkInsight is a command-line tool for analyzing and tuning Spark applications. It helps identify performance bottlenecks, suggests optimizations, and provides a detailed summary of application metrics.

## Features

*   **Performance Analysis:** Identifies performance bottlenecks in Spark jobs.
*   **Failure Analysis:** Helps to debug and identify the root cause of job failures.
*   **Metrics Summary:** Provides a comprehensive summary of key metrics for Spark applications.
*   **Diff Analysis:** Compares two Spark applications to identify performance regressions.
*   **Auto-Scaling Recommendations:** Provides auto-scaling recommendations based on application workload.
*   **Executor Analysis:** Shows the number of running executors in one-minute intervals.
*   **Command-Line Interface:** Easy to use from the command line.

## Usage

To analyze a single Spark application, run the following command:

```bash
./bin/spark-insight run --url1 <app_id_or_url>
```

To compare the two most recent Spark applications with a given name, run the following command:

```bash
./bin/spark-insight run --app-name <app_name>
```

To compare two specific Spark applications, run the following command:

```bash
./bin/spark-insight run --url1 <app_id_or_url_1> --url2 <app_id_or_url_2>
```

Replace `<app_id_or_url>` with your Spark application ID or the full URL to the application in the Spark History Server.

## Example output:

### Single Application Analysis

```text
Auto-Scaling Analysis for application_1693333993908_0001 -
Provides recommendations for auto-scaling configuration based on application workload.
The suggested values are calculated as follows:
- Initial Executors: Based on the stages running in the first 2 minutes of the application.
- Max Executors: Based on the maximum number of concurrent stages running throughout the application.
The calculation aims to complete each stage in 2 minutes.


╔═══════════════════╤═════════╤═══════════╤═══════════════════════════════════════════════════════════╗
║ Configuration     │ Current │ Suggested │ Description                                               ║
╠═══════════════════╪═════════╪═══════════╪═══════════════════════════════════════════════════════════╣
║ Initial Executors │ N/A     │ 113       │ Recommended number of initial executors to provision.     ║
╟───────────────────┼─────────┼───────────┼───────────────────────────────────────────────────────────╢
║ Max Executors     │ 10      │ 113       │ Recommended maximum number of executors for auto-scaling. ║
╚═══════════════════╧═════════╧═══════════╧═══════════════════════════════════════════════════════════╝



Spark Application Performance Report for applicationId: application_1693333993908_0001 -

╔══════════════════════════════╤═══════════╤═══════════════════════════════════════════════════════════════════╗
║ Metric name                  │ Value     │ Metric description                                                ║
╠══════════════════════════════╪═══════════╪═══════════════════════════════════════════════════════════════════╣
║ Time                         │           │                                                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║                              │           │                                                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Total Runtime                │ 404       │ Total elapsed running time (minutes)                              ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Total Executor Time          │ 4450      │ Total time across all executors (minutes)                         ║
⟟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────⟢
║ Successful Executor Runtime  │ 11480     │ Total executor running time for successful stages (minutes)       ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Failed Executor Runtime      │ 3860      │ Total executor running time for failed stages (minutes)           ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Executor Runtime w/o Shuffle │ 12580     │ Executor run time excluding shuffle time (minutes)                ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Executor CPU Time            │ 9014      │ Total executor CPU time on main task thread (minutes)             ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ JVM GC Time                  │ 48        │ Total JVM garbage collection time (minutes)                       ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Executor Utilization         │ 86.18%    │ Executor utilization percentage                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ I/O                          │           │                                                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║                              │           │                                                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Input Size                   │ 4166      │ Total input data consumed by tasks (GB)                           ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Output Size                  │ 0         │ Total output data produced by tasks (GB)                          ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Input Records                │ 346362347 │ Total records consumed by tasks (thousands)                       ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Output Records               │ 0         │ Total records produced by tasks (thousands)                       ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Net I/O Time                 │ 4166      │ Total time accessing external storage (minutes)                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle                      │           │                                                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║                              │           │                                                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Read Size            │ 4870      │ Total shuffle data consumed by tasks (GB)                         ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Write Size           │ 4803      │ Total shuffle data produced by tasks (GB)                         ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Read Records         │ 299157917 │ Total shuffle records consumed by tasks (thousands)               ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Write Records        │ 292430791 │ Total shuffle records produced by tasks (thousands)               ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Read Wait Time       │ 2760      │ Total task time blocked waiting for remote shuffle data (minutes) ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Write Time           │ 112877208 │ Total shuffle write time spent by tasks (minutes)                 ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Disk Spill Size              │ 1557      │ Total data spilled to disk (GB)                                   ║
╟──────────────────────────────┼───────────┼───────────────────────────────────────────────────────────────────╢
║ Memory Spill Size            │ 5684      │ Total data spilled to memory (GB)                                 ║
╚══════════════════════════════╧═══════════╧═══════════════════════════════════════════════════════════════════╝



Executor Analysis for application_1693333993908_0001 - Shows the number of running executors at one-minute intervals.

╔════════════════╤═══════════════════╗
║ Time (minutes) │ Running Executors ║
╠════════════════╪═══════════════════╣
║ 0              │ 0                 ║
╟────────────────┼───────────────────╢
║ 1-404          │ 11                ║
╚════════════════╧═══════════════════╝
```

### Diff Analysis

```text
Spark Application Diff Report for app-20250407085246-0000 and app-20250406090044-0000 -

╔════════════════════════╤════════════════════════════════╤════════════════════════════════╤═════════╤═══════════════════════════════════════════════════════════════════╗
║ Metric                 │ App1 (app-20250407085246-0000) │ App2 (app-20250406090044-0000) │ Diff    │ Metric Description                                                ║
╠════════════════════════╪════════════════════════════════╪════════════════════════════════╪═════════╪═══════════════════════════════════════════════════════════════════╣
║ Executor CPU Time      │ 694                            │ 535                            │ -23.01% │ Total executor CPU time on main task thread (minutes)             ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Executor Runtime       │ 1318                           │ 1357                           │ 2.92%   │ Total executor running time (minutes)                             ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Input Records          │ 3178788                        │ 2017966                        │ -36.52% │ Total records consumed by tasks (thousands)                       ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ JVM GC Time            │ 15                             │ 14                             │ -6.82%  │ Total JVM garbage collection time (minutes)                       ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Output Records         │ 986749                         │ 988522                         │ 0.18%   │ Total records produced by tasks (thousands)                       ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Output Size            │ 40                             │ 40                             │ 0.19%   │ Total output data produced by tasks (GB)                          ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Read Records   │ 3272898                        │ 3004204                        │ -8.21%  │ Total shuffle records consumed by tasks (thousands)               ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Read Size      │ 247                            │ 231                            │ -6.58%  │ Total shuffle data consumed by tasks (GB)                         ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Read Wait Time │ 39                             │ 179                            │ 353.26% │ Total task time blocked waiting for remote shuffle data (minutes) ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Write Records  │ 4288331                        │ 3004204                        │ -29.94% │ Total shuffle records produced by tasks (thousands)               ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Write Size     │ 283                            │ 231                            │ -18.27% │ Total shuffle data produced by tasks (GB)                         ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Shuffle Write Time     │ 9652359                        │ 6540095                        │ -32.24% │ Total shuffle write time spent by tasks (minutes)                 ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Total Executor Time    │ 5409                           │ 1289                           │ -76.17% │ Total time across all executors (minutes)                         ║
╟────────────────────────┼────────────────────────────────┼────────────────────────────────┼─────────┼───────────────────────────────────────────────────────────────────╢
║ Total Runtime          │ 46                             │ 8                              │ -80.85% │ Total elapsed running time (minutes)                              ║
╚════════════════════════╧════════════════════════════════╧════════════════════════════════╧═════════╧═══════════════════════════════════════════════════════════════════╝



Stage Level Diff Report for app-20250407085246-0000 and app-20250406090044-0000 - Compares the performance of common stages between two Spark applications.

╔══════════╤═══════════════════════════════════════════════════════════╤═════════════════╤════════════╤════════════════════╤═════════════════════╤═════════════════════╗
║ Stage ID │ Name                                                      │ Duration Diff   │ Input Diff │ Output Diff        │ Shuffle Read Diff   │ Shuffle Write Diff  ║
╠══════════╪═══════════════════════════════════════════════════════════╪═════════════════╪════════════╪════════════════════╪═════════════════════╪═════════════════════╣
║ 2        │ save at BigQueryWriteHelper.java:129                      │ -7min (-79.19%) │ 0MB (N/A)  │ 0MB (N/A)          │ 0MB (N/A)           │ 0MB (0.14%)         ║
╟──────────┼───────────────────────────────────────────────────────────┼─────────────────┼────────────┼────────────────────┼─────────────────────┼─────────────────────╢
║ 15       │ save at BigQueryWriteHelper.java:129                      │ -7min (-92.87%) │ 0MB (N/A)  │ 39568MB (1689.18%) │ 109481MB (1219.54%) │ 0MB (N/A)           ║
╟──────────┼───────────────────────────────────────────────────────────┼─────────────────┼────────────┼────────────────────┼─────────────────────┼─────────────────────╢
║ 9        │ save at BigQueryWriteHelper.java:129                      │ -7min (-90.94%) │ 0MB (N/A)  │ 0MB (N/A)          │ 109833MB (1234.16%) │ 109521MB (1225.41%) ║
╟──────────┼───────────────────────────────────────────────────────────┼─────────────────┼────────────┼────────────────────┼─────────────────────┼─────────────────────╢
║ 1        │ $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 │ -7min (-78.39%) │ 0MB (N/A)  │ 0MB (N/A)          │ 0MB (N/A)           │ 0MB (N/A)           ║
╟──────────┼───────────────────────────────────────────────────────────┼─────────────────┼────────────┼────────────────────┼─────────────────────┼─────────────────────╢
║ 0        │ save at BigQueryWriteHelper.java:129                      │ -7min (-77.52%) │ 0MB (N/A)  │ 0MB (N/A)          │ 0MB (N/A)           │ 225MB (0.95%)       ║
╚══════════╧═══════════════════════════════════════════════════════════╧═════════════════╧════════════╧════════════════════╧═════════════════════╧═════════════════════╝
```

## Building from Source

To build the project from source, use Maven:

```bash
mvn clean package
```

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.