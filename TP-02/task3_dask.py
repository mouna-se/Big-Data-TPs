import dask
import dask.dataframe as dd
import pandas as pd
import psutil
import time
import json

# File paths
file_path = r"D:\csv\all_merged.csv"
results_file = "task3_results.json"

process = psutil.Process()

ram_before = process.memory_info().rss / (1024 ** 2)
system_ram_before = psutil.virtual_memory().percent
start_time = time.time()

peak_ram = ram_before

results = {
    "Method": "Dask DataFrame",
    "Block Size": "64 MiB",
    "Total Rows": None,
    "Number of Partitions": None,
    "Global Mean": None,
    "Execution Time (s)": None,
    "Process RAM Before (MB)": round(ram_before, 2),
    "Process RAM Peak (MB)": None,
    "Process RAM After (MB)": None,
    "System RAM Before (%)": system_ram_before,
    "System RAM After (%)": None,
    "Status": None,
    "Error": None
}

try:
    # Load data using Dask
    df = dd.read_csv(
        file_path,
        header=None,
        blocksize="64 MiB",
        assume_missing=True,
        dtype={
            5: "object",
            7: "object"
        }
    )

    results["Number of Partitions"] = df.npartitions
    print("Number of partitions:", df.npartitions)

    # Identify numeric columns
    numeric_cols = [
        col for col in df.columns
        if pd.api.types.is_numeric_dtype(df.dtypes[col])
    ]

    if not numeric_cols:
        raise ValueError("No numeric columns were detected.")

    numeric_columns = df[numeric_cols]

    # Build the lazy computation graph
    total_sum_task = numeric_columns.sum().sum()
    total_count_task = numeric_columns.count().sum()
    total_rows_task = df.map_partitions(len).sum()

    # Execute computations using multiple threads
    total_sum, total_count, total_rows = dask.compute(
        total_sum_task,
        total_count_task,
        total_rows_task,
        scheduler="threads"
    )

    ram_after_compute = process.memory_info().rss / (1024 ** 2)
    peak_ram = max(peak_ram, ram_after_compute)

    # Calculate the global mean of numeric values
    global_mean = (
        float(total_sum / total_count)
        if total_count > 0 else None
    )

    results["Total Rows"] = int(total_rows)
    results["Global Mean"] = global_mean
    results["Status"] = "Success"

except KeyboardInterrupt:
    results["Status"] = "Interrupted"
    results["Error"] = "Execution was interrupted by the user."

except Exception as e:
    results["Status"] = "Error"
    results["Error"] = f"{type(e).__name__}: {e}"
    print("Error:", results["Error"])

finally:
    results["Execution Time (s)"] = round(
        time.time() - start_time, 2
    )

    ram_after = process.memory_info().rss / (1024 ** 2)
    peak_ram = max(peak_ram, ram_after)

    results["Process RAM Peak (MB)"] = round(peak_ram, 2)
    results["Process RAM After (MB)"] = round(ram_after, 2)
    results["System RAM After (%)"] = psutil.virtual_memory().percent

    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print("\nResults saved to", results_file)
    print(json.dumps(results, indent=4, ensure_ascii=False))