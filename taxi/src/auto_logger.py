#!/usr/bin/env python3
"""
===========================================================
AUTO LOGGER - Rapport automatique pour chaque commande
===========================================================

Ce script execute n'importe quelle commande passee en argument,
capture sa sortie, l'affiche dans le terminal, puis l'ajoute
automatiquement au fichier memo.log avec :

- horodatage
- commande executee
- repertoire courant
- paragraphe explicatif
- sortie complete
- separateurs propres

===========================================================
"""

import subprocess
import sys
import datetime
import os

# ----------------------------------------------------------
# 1) Verification : l'utilisateur doit fournir une commande
# ----------------------------------------------------------
if len(sys.argv) < 2:
    print("Usage : python auto_logger.py <commande>")
    sys.exit(1)

# ----------------------------------------------------------
# 2) Construction de la commande a executer
# ----------------------------------------------------------
commande = sys.argv[1:]
commande_str = " ".join(commande)

# ----------------------------------------------------------
# 3) Paragraphe explicatif automatique
# ----------------------------------------------------------
paragraphe = """
=== Explication du processus ===

L'analyse executee ci-dessous fait partie du pipeline Big Data.
Chaque commande est enregistree automatiquement afin de constituer
un rapport complet et tracable. Ce mecanisme permet de documenter
les etapes du nettoyage, de l'analyse, des benchmarks et des
aggregations Spark, garantissant une transparence totale dans la
construction du projet.

"""

# Affichage dans le terminal AVANT la commande
print(paragraphe)

# ----------------------------------------------------------
# 4) Preparation du fichier de log
# ----------------------------------------------------------
log_path = os.path.expanduser("~/bigdata/taxi/memo.log")

with open(log_path, "a") as log:

    log.write("\n===========================================================\n")
    log.write(f"Commande executee : {commande_str}\n")
    log.write(f"Horodatage : {datetime.datetime.now()}\n")
    log.write(f"Repertoire : {os.getcwd()}\n")
    log.write("-----------------------------------------------------------\n")

    log.write(paragraphe)
    log.write("-----------------------------------------------------------\n")
    log.write("Sortie :\n")

    try:
        result = subprocess.run(
            commande,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        print(result.stdout)
        log.write(result.stdout)

    except Exception as e:
        print(f"Erreur lors de l'execution : {e}")
        log.write(f"Erreur : {e}\n")

    log.write("===========================================================\n")

