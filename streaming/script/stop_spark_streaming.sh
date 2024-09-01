#!/bin/bash

# Arrêter tous les processus Spark (utilisez pkill pour les tuer tous)
pkill -f "spark-submit"

echo "Processus Spark arrêtés."