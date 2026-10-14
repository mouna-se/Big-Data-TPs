
import pandas as pd
import psutil
import time
import json

file_path = r"D:\csv\all_merged.csv"
results_file = "task1_results.json"

process = psutil.Process()

ram_before = process.memory_info().rss / (1024 ** 2)
system_ram_before = psutil.virtual_memory().percent
start_time = time.time()

results = {
    "Method": "Standard Pandas",
    "Total Rows": None,
    "Execution Time (s)": None,
    "Process RAM Before (MB)": round(ram_before, 2),
    "Process RAM After (MB)": None,
    "System RAM Before (%)": system_ram_before,
    "System RAM After (%)": None,
    "Status": None,
    "Error": None
}

try:
    # Read the entire file into memory at once
    df = pd.read_csv(file_path, header=None)

    results["Total Rows"] = len(df)
    results["Status"] = "Success"

    print("Total rows:", len(df))

except MemoryError:
    results["Status"] = "MemoryError"
    results["Error"] = "Insufficient memory to load the entire file."

except Exception as e:
    results["Status"] = "Error"
    results["Error"] = f"{type(e).__name__}: {e}"

finally:
    results["Execution Time (s)"] = round(
        time.time() - start_time, 2
    )

    results["Process RAM After (MB)"] = round(
        process.memory_info().rss / (1024 ** 2), 2
    )

    results["System RAM After (%)"] = psutil.virtual_memory().percent

    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print("Results saved to", results_file)
    print(json.dumps(results, indent=4, ensure_ascii=False))
