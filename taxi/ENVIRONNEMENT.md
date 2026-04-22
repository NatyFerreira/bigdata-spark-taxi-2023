🖥️ Note technique — Environnement utilisé pour l’Itération 2
Ce document décrit précisément l’environnement matériel et logiciel utilisé pour réaliser l’Itération 2 du module Big Data.
L’objectif est d’assurer la transparence, la reproductibilité et la compréhension des choix techniques effectués.

1. Contexte matériel
Pour cette itération, j’ai travaillé principalement sur mon ordinateur personnel :

Machine hôte : MacBook Air (macOS)

Architecture : Apple Silicon (ARM)

Contraintes :

Spark + Hadoop ne fonctionnent pas nativement en cluster sur macOS

Nécessité d’un environnement Linux pour respecter les prérequis du module

2. Mise en place d’une machine virtuelle Linux
Afin de disposer d’un environnement compatible avec Spark et les outils Big Data, j’ai créé une machine virtuelle Ubuntu sur mon Mac.

Hyperviseur : OrbStack

Système invité : Ubuntu 22.04

Rôle :

Exécuter Spark

Lancer les scripts Python

Héberger un Worker Spark

Avantages :

Environnement Linux stable

Compatible avec les outils du module

Reproductible et isolé

3. Utilisation du PC de l’école comme Master Spark
Pour la partie cluster (Itération 2.3), j’ai utilisé une architecture hybride :

PC de l’école → Master Spark

VM Ubuntu sur mon Mac → Worker Spark

Connexion via réseau local et SSH

Architecture finale :
Código
MacBook (machine hôte)
   ↓
VM Ubuntu (Worker Spark)
   ↓ réseau local
PC de l’école (Master Spark)
Rôles :
Driver : exécuté depuis ma VM Ubuntu

Master : PC de l’école

Workers : VM + PC selon disponibilité

Cette configuration respecte l’architecture Spark Standalone demandée dans l’itération.

4. Justification de cette architecture
Cette approche a été choisie pour plusieurs raisons :

Permettre l’utilisation d’un environnement Linux conforme au module

Contourner les limitations du macOS pour Spark en cluster

Reproduire fidèlement les étapes de l’itération (Master, Workers, spark-submit)

Garantir des résultats valides pour :

CSV vs Parquet

Partitionnement

Montée en charge

DAG Spark

Séparer proprement :

développement (Mac)

exécution (VM Linux)

orchestration (PC de l’école)

5. Impact sur les livrables
Aucun impact négatif sur les résultats :

Benchmarks identiques à ceux attendus

Cluster Spark fonctionnel

spark-submit opérationnel

DAGs visibles et corrects

Montée en charge reproductible

L’environnement utilisé est différent, mais totalement conforme aux objectifs pédagogiques.

6. Conclusion
Cette architecture hybride (Mac → VM Ubuntu → PC de l’école) a permis de :

respecter les contraintes techniques du module

exécuter un cluster Spark réel

produire des résultats fiables et reproductibles

documenter clairement les choix techniques

Elle garantit que l’ensemble de l’Itération 2 a été réalisé dans un environnement Linux fonctionnel, avec un cluster Spark opérationnel.

                         ┌──────────────────────────────────────────┐
                         │              MacBook (hôte)              │
                         │------------------------------------------│
                         │  • Développement                         │
                         │  • VS Code / Python                      │
                         │  • Lancement du DRIVER Spark             │
                         │                                          │
                         │  DRIVER → construit le DAG, envoie les   │
                         │  tâches au Master                        │
                         └───────────────┬──────────────────────────┘
                                         │
                                         │ SSH
                                         │
                         ┌───────────────▼──────────────────────────┐
                         │         VM Ubuntu (Worker Spark)          │
                         │-------------------------------------------│
                         │  WORKER                                   │
                         │  • Exécute les tâches                     │
                         │  • Héberge un Executor                    │
                         │  • Linux conforme au module               │
                         └───────────────┬──────────────────────────┘
                                         │
                                         │ Réseau local
                                         │
                         ┌───────────────▼──────────────────────────┐
                         │        PC de l’école (Master Spark)       │
                         │-------------------------------------------│
                         │  MASTER                                   │
                         │  • Alloue les Workers                     │
                         │  • Coordonne les Executors                │
                         │  • UI : http://<ip>:8080                  │
                         └───────────────────────────────────────────┘
