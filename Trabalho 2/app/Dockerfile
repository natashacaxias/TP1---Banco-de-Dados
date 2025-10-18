FROM debian:bookworm-slim

# Atualiza pacotes e instala compiladores
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ make cmake \
    && rm -rf /var/lib/apt/lists/*

# Define diretório de trabalho
WORKDIR /app

# Copia o código-fonte
COPY . /app

# Compila os binários via Makefile
RUN make build

# Define diretório de dados persistente
VOLUME ["/data"]

# Variáveis de ambiente
ENV CSV_PATH=/data/input.csv \
    DATA_DIR=/data/db \
    LOG_LEVEL=info

# Mensagem padrão ao rodar o container
CMD ["bash", "-lc", "echo 'Use: docker run --rm -v $(pwd)/data:/data tp2 ./bin/upload /data/input.csv'"]
