import pandas as pd
import psutil
import time
import json

file_path = r"D:\csv\all_merged.csv"
chunk_size = 100000

process = psutil.Process()

start_ram = process.memory_info().rss / (1024 ** 2)
system_ram_before = psutil.virtual_memory().percent

total_rows = 0
total_sum = 0
total_count = 0
peak_ram = start_ram

start_time = time.time()

for chunk in pd.read_csv(
    file_path,
    header=None, #عتبر الصف الأول بيانات، وليس أسماء للأعمدة
    chunksize=chunk_size,
    dtype={5: str, 7: str}  #قراءة العمودين 5 و 7 كنصوص 
):
    total_rows += len(chunk)

    #تحويل العمودين إلى أرقام
    chunk[5] = pd.to_numeric(chunk[5], errors='coerce')
    chunk[7] = pd.to_numeric(chunk[7], errors='coerce')

    # استخراج كافة الأعمدة الرقمية)
    numeric_data = chunk.select_dtypes(include="number")

    total_sum += numeric_data.sum().sum()
    total_count += numeric_data.count().sum() #عدد القيم غير المفقودة في كل عمود

    current_ram = process.memory_info().rss / (1024 ** 2)
    peak_ram = max(peak_ram, current_ram)

global_mean = total_sum / total_count if total_count > 0 else 0
execution_time = round(time.time() - start_time, 2)

results = {
    "Method": "Pandas Chunking",
    "Chunk Size": chunk_size,
    "Total Rows": total_rows,
    "Global Sum": float(total_sum),
    "Global Mean": float(global_mean),
    "Execution Time (s)": execution_time,
    "Process RAM Before (MB)": round(start_ram, 2),
    "Peak Process RAM (MB)": round(peak_ram, 2),
    "System RAM Before (%)": system_ram_before,
    "System RAM After (%)": psutil.virtual_memory().percent,
    "Status": "Success"
}

with open("task2_results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)
