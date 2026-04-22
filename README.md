# 📘 **README.md — Big Data Spark Taxi 2023**

```markdown
# 🚖 Big Data – Spark NYC Taxi (2023)
Projet réalisé dans le cadre du module **Big Data** : nettoyage, benchmarks, analyses et visualisations avec **Apache Spark**.

## 📂 Structure du projet
```
bigdata-spark-taxi-2023/
│
├── pipeline_nettoyage_spark.py        # Pipeline de nettoyage (Itération 2)
├── benchmark_pandas.py                # Benchmark Pandas
├── benchmark_spark.py                 # Benchmark Spark
├── csv_vs_parquet.py                  # Comparaison CSV vs Parquet
├── parquet_vs_parquet.py              # Comparaison Parquet vs Parquet partitionné
├── test_cluster.py                    # Test simple du cluster Spark
├── analyse_spark.py                   # Statistiques et agrégations
├── visualisation_spark.py             # Visualisations (histogrammes, barplots, etc.)
│
├── yellow_tripdata_2023-01.parquet    # Données brutes (local)
└── yellow_tripdata_2023-01-clean.parquet
```

---

## 🧹 **Itération 2 – Nettoyage & Benchmarks**

### ✔ Pipeline de nettoyage
- Chargement du dataset brut (Parquet)
- Suppression des valeurs nulles
- Filtrage des valeurs incohérentes
- Sauvegarde du dataset propre en Parquet
- Résumé du pipeline avec **Rich**

### ✔ Benchmarks
- **Pandas** : temps de lecture + moyenne
- **Spark** : temps de lecture + moyenne
- Comparaison CSV vs Parquet
- Comparaison Parquet vs Parquet partitionné

### ✔ Test du cluster
- Job simple `spark.range().count()`

### ✔ Analyses exploratoires
- Statistiques descriptives
- Montant moyen par type de paiement
- Nombre de courses par heure
- Distance moyenne par zone

### ✔ Visualisations
- Histogramme des montants
- Répartition horaire
- Distance moyenne par zone

---

## 👥 **Crédits & Collaboration**

Projet développé par **Natalia Ferreira**, avec contributions de :

- **Elena** — support dans le nettoyage et la validation des données  
- **Clément** — aide dans les tests du cluster et l’analyse exploratoire  

Merci pour la collaboration durant l’Itération 2.

---

## 🚀 **Exécution**

Lancer un script :

```bash
python3 pipeline_nettoyage_spark.py
```

Assurez-vous d’avoir :

- Python 3.x  
- PySpark  
- Matplotlib / Seaborn  
- Rich  

---

## 📌 **Objectif du projet**

Mettre en place un pipeline Big Data complet :

- ingestion  
- nettoyage  
- benchmarks  
- analyses  
- visualisations  

en utilisant **Apache Spark** sur un dataset réel NYC Taxi (2023).

---

## 📄 Licence
Projet académique — utilisation pédagogique uniquement.
```
