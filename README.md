# TP1 – Banco de Dados (2025/02)

Alunos: Abel Severo Rocha, Ana Carla de Araujo Fernandes, Natasha Araujo Caxias

Trabalho Prático I da disciplina **Introdução a Bancos de Dados** – Universidade Federal do Amazonas (UFAM) – Instituto de Computação. 

O objetivo é projetar e implementar um banco de dados relacional sobre produtos vendidos em uma loja de comércio eletrônico, utilizando dados do **Amazon product co-purchasing network metadata (SNAP)**.  

O projeto inclui:
- **Criação do esquema relacional** no PostgreSQL  
- **Carga (ETL)** dos dados a partir do arquivo de entrada  
- **Dashboard (consultas SQL puras)** para geração de relatórios  

## Estrutura do Repositório

```
TP1---Banco-de-Dados/
├── docker-compose.yml # Orquestração dos serviços
├── Dockerfile # Build da aplicação Python
├── requirements.txt # Dependências Python
├── Makefile # Atalhos para execução
├── src/
│ ├── tp1_3.2.py # Script de carga (criação esquema + ETL)
│ └── tp1_3.3.py # Dashboard (consultas SQL)
├── sql/
│ ├── schema.sql #  DDL em SQL puro
│ └── queries.sql # Consultas em SQL puro
├── data/
│ └── amazon-meta.txt.gz # Arquivo de entrada (README de instruções de download)
├── out/ # Resultados em CSV (gerados automaticamente)
├── docs/
│ ├── tp1_3.1.pdf # Documentação do esquema
│ └── esquema.png # Diagrama do BD
└── README.md # Este arquivo
```

## Como executar 

##(Atenção: Carregar o arquivo de entrada no diretório /data)

### Usando `Makefile` (recomendado)

#### 1) Construir e subir os serviços
`make up` 

#### 2) Conferir se o banco está saudável
`make ps` 

#### 3) Criar esquema e carregar dados
`make ps` 

#### 4) Executar o Dashboard (consultas SQL)
`make dashboard` 

#### 5) Derrubar os serviços (containers + volumes)
`make down` 

#### 6) Projeto de Ponta a Ponta (subir → carregar → dashboard)
`make all` 

#### 7) Resetar o ambiente (derrubar + subir de novo)
`make reset` 



