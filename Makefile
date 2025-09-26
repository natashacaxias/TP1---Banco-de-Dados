# Variáveis
DB_HOST=db
DB_PORT=5432
DB_NAME=ecommerce
DB_USER=postgres
DB_PASS=postgres
INPUT=data/amazon-meta.txt.gz
ASIN=0738700797

# 1) Construir e subir os serviços
up:
	docker compose up -d --build

# 2) Conferir saude do PostgreSQL (opcional)
ps:
	docker compose ps

# 3) Criar esquema e carregar dados
load:
	docker compose run --rm app python src/tp1_3.2.py \
	  --db-host $(DB_HOST) --db-port $(DB_PORT) \
	  --db-name $(DB_NAME) --db-user $(DB_USER) --db-pass $(DB_PASS) \
	  --input $(INPUT)

# 4) Executar o Dashboard (todas as consultas)
dashboard:
	docker compose run --rm app python src/tp1_3.3.py \
	  --db-host $(DB_HOST) --db-port $(DB_PORT) \
	  --db-name $(DB_NAME) --db-user $(DB_USER) --db-pass $(DB_PASS) \
	  --product-asin $(ASIN)

# 5) Derrubar os servicos (containers + volumes)
down:
	docker compose down -v

# 6) Resetar o ambiente (derrubar + subir de novo)
reset: down up

# 7) Atalho para rodar tudo de ponta-a-ponta (up + load + dashboard)
all: up load dashboard
