import psycopg2
from psycopg2 import sql
import pandas as pd
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
PATH = BASE_DIR / "sql" / "queries.sql"

def conectar_bd():

    parser = argparse.ArgumentParser(description='Processar dados do Amazon Meta')
    parser.add_argument('--db-host', required=True, help='Host do PostgreSQL')
    parser.add_argument('--db-port', required=True, help='Porta do PostgreSQL')
    parser.add_argument('--db-name', required=True, help='Nome do banco de dados')
    parser.add_argument('--db-user', required=True, help='Usuário do PostgreSQL')
    parser.add_argument('--db-pass', required=True, help='Senha do PostgreSQL')
    
    args = parser.parse_args()

    DB_CONFIG = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_pass,
    }

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Banco de Dados conectado com sucesso!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao BD: {e}")
        exit(1)

def executar_consultas(conn, path):
    with open(path, 'r', encoding='utf-8') as arquivo_sql:
        consultas = arquivo_sql.read()
    
    queries = [q.strip() for q in consultas.split(';')]

    cursor = conn.cursor()

    try:
        for i, query in enumerate(queries, 1):
            if not query: # pula linha em branco
                continue 

            l, sep, query = query.partition("\n")
            if l.startswith('-- Q'):
                consulta_nome = l

            # executa consulta
            cursor.execute(query)

            # salva consulta
            if(query.strip().upper().startswith('SELECT')):
                colunas = [desc[0] for desc in cursor.description]
                dados = cursor.fetchall()

                df = pd.DataFrame(dados, columns=colunas)

            # imprime no terminal
            print(f"Resultados da consulta '{consulta_nome}':")
            print(f"Total de registros: {len(df)}")
            print(f"Colunas: {', '.join(colunas)}")
            print("\nPrimeiras 10 linhas:")
            print(df.head(10))

            # salva .csv
            df.to_csv(BASE_DIR / "out"/ consulta_nome+".csv", index = False)
    except Exception as e:
        print(f"❌ Erro na consulta {i}: {e}")
        print(f"Consulta problemática: {query[:100]}...")






if __name__ == "__main__":

    CONN = conectar_bd()
    executar_consultas(CONN, PATH)

