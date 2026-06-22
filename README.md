# Big Data — Spark NYC Taxi 2023

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-distributed-E25A1C?logo=apachespark)
![Parquet](https://img.shields.io/badge/Format-Parquet-50ABF1)
![License](https://img.shields.io/badge/License-Academic-lightgrey)

Distributed data pipeline for the NYC Yellow Taxi 2023 dataset: cleaning, benchmarking (Pandas vs Spark, CSV vs Parquet), exploratory analysis, and visualisations with Apache Spark.  
Built as a Big Data module project (Campus Numérique in the Alps — Data Engineer & AI, RNCP Level 7, 2026).

---

## Project Structure

```
bigdata-spark-taxi-2023/
├── pipeline_nettoyage_spark.py        # Cleaning pipeline (Iteration 2)
├── benchmark_pandas.py                # Pandas read + aggregation benchmark
├── benchmark_spark.py                 # Spark read + aggregation benchmark
├── csv_vs_parquet.py                  # Format comparison: CSV vs Parquet
├── parquet_vs_parquet.py              # Format comparison: Parquet vs partitioned Parquet
├── test_cluster.py                    # Basic Spark cluster validation
├── analyse_spark.py                   # Descriptive statistics and aggregations
├── visualisation_spark.py             # Visualisations (histograms, barplots, time series)
├── yellow_tripdata_2023-01.parquet    # Raw dataset (local — not versioned)
└── yellow_tripdata_2023-01-clean.parquet  # Cleaned output (local — not versioned)
```

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11 |
| Distributed processing | Apache Spark (PySpark) |
| Data format | Parquet (raw + partitioned) |
| Benchmarking | Pandas vs Spark (read + aggregation) |
| Visualisations | Matplotlib, Seaborn |
| Pipeline output summary | Rich |

---

## Installation

```bash
# Install dependencies
pip install pyspark matplotlib seaborn rich

# Run the cleaning pipeline
python3 pipeline_nettoyage_spark.py
```

---

## Cleaning Pipeline

`pipeline_nettoyage_spark.py` runs the following steps on the raw Parquet dataset:

1. Load raw dataset from Parquet
2. Drop null values
3. Filter out incoherent values (negative fares, zero-distance trips, etc.)
4. Save cleaned dataset to Parquet
5. Print pipeline summary with Rich

---

## Benchmarks

| Script | Comparison |
|--------|------------|
| `benchmark_pandas.py` | Pandas read time + mean computation |
| `benchmark_spark.py` | Spark read time + mean computation |
| `csv_vs_parquet.py` | Read performance: CSV vs Parquet |
| `parquet_vs_parquet.py` | Read performance: flat Parquet vs partitioned Parquet |

Results demonstrate the performance advantages of columnar storage and distributed processing at scale on the NYC Taxi dataset (~3M rows, January 2023).

---

## Exploratory Analysis

`analyse_spark.py` computes:

- Descriptive statistics (fare amount, distance, duration)
- Average fare by payment type
- Trip count by hour of day
- Average distance by pickup zone

---

## Visualisations

`visualisation_spark.py` generates:

- Fare amount distribution (histogram)
- Hourly trip volume (time series barplot)
- Average distance by pickup zone (barplot)

---

## Cluster Validation

`test_cluster.py` runs a basic Spark job to confirm the cluster is operational:

```python
spark.range(1_000_000).count()
```

---

## Dataset

NYC Yellow Taxi Trip Records — January 2023.  
Source: [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
Raw and cleaned Parquet files are not versioned in this repository (large binary files).

---

## Author

**Natália Helen Ferreira**  
PhD in Biological Chemistry | Data Engineer & AI (RNCP Level 7, in progress)  
[LinkedIn](https://linkedin.com/in/ferreiranh) · [GitHub](https://github.com/NatyFerreira)

---

## License

Academic project — free to use for educational purposes.
