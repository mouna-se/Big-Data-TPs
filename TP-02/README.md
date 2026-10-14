## 1. Dataset Source
**Dataset link:** [Bot-IoT Dataset on Kaggle](https://www.kaggle.com/datasets/vigneshvenkateswaran/bot-iot)


## 2. Dataset Preparation

The downloaded dataset contained multiple CSV files. we merged them into a single file, `all_merged.csv`, using Windows PowerShell.

```powershell
$first = $true
Get-ChildItem *.csv | ForEach-Object {
    if ($first) {
        Get-Content $_.FullName | Set-Content all_merged.csv
        $first = $false
    } else {
        Get-Content $_.FullName | Select-Object -Skip 1 | Add-Content all_merged.csv
    }
}
```
The resulting file has a total size of approximately **5.16 GB** and contains **27,000,000 rows** and 35 columns.
## 3. Task 1: Standard Pandas

### Results

- **Status:** MemoryError
- **Execution time:** 217.19 seconds
- **Process RAM before execution:** 78.25 MB
- **Process RAM after execution:** 131.85 MB
- **Total rows:** Not available because the operation failed

## 4. Task 2: Pandas Chunking

### Results

- **Status:** Success
- **Total rows processed:** 27,000,000
- **Chunk size:** 100,000 rows
- **Global sum:** \(8.318993685178456e+16)
- **Global mean:** 140,050,889.86
- **Execution time:** 276.93 seconds
- **Process RAM before execution:** 78.12 MB
- **Peak process RAM:** 184.25 MB

## 5. Task 3: Dask DataFrame

**Lazy Evaluation :**  
A technique that delays the execution of operations until their results are needed. In Dask, operations are first recorded in a computation graph and executed when a result is requested.

### Results

- **Status:** Success
- **Total rows processed:** 27,000,000
- **Block size:** 64 MiB
- **Number of partitions:** 82
- **Global mean:** 154,053,794.83
- **Execution time:** 152.97 seconds
- **Process RAM before execution:** 105.54 MB
- **Process RAM after execution:** 242.25 MB

## 6. Performance Benchmark

| Method | Max RAM Utilization | Execution Speed |
|---|---|---|
| Standard Pandas | Failed to load the dataset (`MemoryError`) | N/A (OOM Error) |
| Pandas Chunking | 184.25 MB  | Moderate (276.93 s) |
| Dask DataFrame |  1339.9 MB  | Fast (152.97 s) |

### Benchmark Analysis

- **Standard Pandas:** Failed to load the complete dataset because of a `MemoryError`.
- **Pandas Chunking:** Successfully processed the dataset in chunks, with a peak process RAM usage of 184.25 MB.
- **Dask DataFrame:** Successfully processed all 27 million rows in 152.97 seconds, making it the fastest successful method in this experiment.

Dask completed the data processing faster than Pandas Chunking, demonstrating better execution time performance. However, Pandas Chunking used significantly less RAM, making it more memory-efficient for processing large datasets.


## 7. Conclusion

In conclusion, Dask DataFrame provided the fastest execution time, while Pandas Chunking achieved better memory efficiency. Standard Pandas was unable to process the entire dataset due to a `MemoryError`. Therefore, Dask is preferable when speed is the priority, whereas Pandas Chunking is a better choice for systems with limited RAM.