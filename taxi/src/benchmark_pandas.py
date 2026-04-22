import pandas as pd
import time
import psutil
import os

file = "yellow_tripdata_2023-01.parquet"

process = psutil.Process(os.getpid())

t0 = time.time()
df = pd.read_parquet(file)
t1 = time.time()

ram = process.memory_info().rss / (1024**2)

print("=== Benchmark Pandas ===")
print(f"Tempo de carregamento: {t1 - t0:.3f} s")
print(f"RAM utilizada: {ram:.2f} MB")
print(f"Média fare_amount: {df['fare_amount'].mean():.3f}")

