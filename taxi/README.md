🚖 Itération 2 — Spark Avancé : Cluster & Performances
🎯 Objectif
Cette itération explore les performances de Spark en mode local et en cluster, ainsi que l’impact du format de stockage, du partitionnement et de la montée en charge.

📁 Contenu du repository
Código
notebooks/       → Notebook principal propre et documenté  
src/             → Scripts Python (benchmarks, pipeline, cluster, etc.)  
images/          → Visualisations générées  
README.md        → Documentation du projet  
.gitignore       → Exclusion des fichiers lourds et temporaires  
1️⃣ CSV vs Parquet
Benchmark lecture complète

Benchmark projection (3 colonnes)

Ratio de taille (CSV 6–8× plus lourd)

Ratio de performance (Parquet 10–20× plus rapide)

Explication : stockage ligne vs colonne

2️⃣ Cluster Spark Standalone
Architecture : Driver / Master / Workers

Installation Spark 3.5.1

Configuration SPARK_LOCAL_IP

Démarrage du Master et des Workers

Connexion via .master("spark://<ip>:7077")

Exécution du pipeline taxi sur le cluster

Comparaison avec le mode local

3️⃣ spark-submit
Génération d’un script job_taxi.py

Soumission :

Código
spark-submit --master spark://<ip>:7077 job_taxi.py
4️⃣ Partitionnement
repartition() vs coalesce()

Analyse du DAG (Exchange / Shuffle)

Benchmarks : 1, 2, 4, 8, 16, 32 partitions

Optimum attendu : 8–12 partitions (2–3× nb de cœurs)

5️⃣ Montée en charge
Benchmarks : 1, 3, 6, 12 mois

Analyse : sub-linéaire / linéaire / supra-linéaire

Détection du spill disque

Optimisations proposées

✔ Livrables validés
Master UI avec Workers actifs

spark-submit fonctionnel

Benchmarks CSV vs Parquet

Partitionnement + DAG

Montée en charge + analyse

🧑‍💻 Auteur
Projet réalisé dans le cadre de l’Itération 2 — Big Data - Clément Brandel, Elena Sapede et Natália Ferreira (Campus Numérique in the Alps).