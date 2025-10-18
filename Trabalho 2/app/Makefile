# Diretórios
SRC_DIR = ./src
BIN_DIR = ./bin
INCLUDE_DIR = ./include

# Compilador e flags
CXX = g++
CXXFLAGS = -Wall -O2 -std=c++17 -I$(INCLUDE_DIR)

# Programas do TP2
PROGS = upload findrec seek1 seek2

all: build

build: $(PROGS)

$(PROGS):
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(SRC_DIR)/$@.cpp -o $(BIN_DIR)/$@

clean:
	rm -rf $(BIN_DIR)/*

docker-build:
	docker build -t tp2 .

docker-run-upload:
	docker run --rm -v $(PWD)/data:/data tp2 ./bin/upload /data/input.csv

docker-run-findrec:
	docker run --rm -v $(PWD)/data:/data tp2 ./bin/findrec 123

docker-run-seek1:
	docker run --rm -v $(PWD)/data:/data tp2 ./bin/seek1 123

docker-run-seek2:
	docker run --rm -v $(PWD)/data:/data tp2 "./bin/seek2 Um Título Exato"
