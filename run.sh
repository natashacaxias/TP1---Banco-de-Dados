#!/bin/bash

set -e  # parar em qualquer erro

echo "Subindo os contêineres com Docker Compose..."
docker compose up -d --build

echo "Aguardando PostgreSQL iniciar..."
sleep 5  # opcional, depende do seu PC

echo "Executando script de carga (tp1_3.2.py)..."
docker compose run --rm app python src/tp1_3.2.py \
  --db-host db --db-port 5432 --db-name ecommerce \
  --db-user postgres --db-pass postgres \
  --input /data/amazon-meta.txt

echo "Executando consultas do dashboard (tp1_3.3.py)..."
docker compose run --rm app python src/tp1_3.3.py \
  --db-host db --db-port 5432 --db-name ecommerce \
  --db-user postgres --db-pass postgres \
  --output /app/out

echo "Tudo pronto! Resultados salvos na pasta /out"
