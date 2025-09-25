# 1) Construir e subir os serviços
docker compose up -d --build

# 2) (Opcional) conferir saúde do PostgreSQL
docker compose ps

# 3) Criar esquema e carregar dados
docker compose run --rm app python src/tp1_3.2.py --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres -input data/amazon-meta.txt.gz

# 4) Executar o Dashboard (todas as consultas)
docker compose run --rm app python src/tp1_3.3.py --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres --product-asin 0738700797