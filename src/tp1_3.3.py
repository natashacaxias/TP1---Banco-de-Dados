import psycopg2, argparse
from pathlib import Path
from psycopg2 import sql
import pandas as pd
from psycopg2.extras import DictCursor  # Adicionar esta importação


BASE_DIR = Path(__file__).parent.parent

def conectar_postgree():

    parser = argparse.ArgumentParser(description='Processar dados do Amazon Meta')
    parser.add_argument('--db-host', required=True, help='Host do PostgreSQL')
    parser.add_argument('--db-port', required=True, help='Porta do PostgreSQL')
    parser.add_argument('--db-name', required=True, help='Nome do banco de dados')
    parser.add_argument('--db-user', required=True, help='Usuário do PostgreSQL')
    parser.add_argument('--db-pass', required=True, help='Senha do PostgreSQL')
    
    args = parser.parse_args()

    # Configuração do PostgreSQL
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

def executar_consultas(conn, consultas):
    with open(consultas, 'r') as c:
        linhas = c.read().split(";")
        
        for l in linhas:
            if not l.strip():  # Pular linhas vazias
                continue
                
            aux = l.strip().split("\n")[0]
            print(aux)
            nome_arquivo = f"{aux.split()[1][:-1]}.csv"
            
            # Usar cursor com DictCursor para melhor formatação
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(l)
                resultados = cur.fetchall()
                
                # Converter para DataFrame
                colunas = [desc[0] for desc in cur.description]
                df = pd.DataFrame(resultados, columns=colunas)
            
            df.to_csv(BASE_DIR / "out" / nome_arquivo, index=False, encoding="utf-8")

            print(f"═" * 60)
            print(f"CONSULTA: {nome_arquivo[:-4]}")
            print(f"Arquivo salvo: {nome_arquivo}")
            print(f"Total de registros: {len(df)}")
            print(f"═" * 60)
            
            # Mostrar resultados de forma mais organizada
            if len(df) > 0:
                print(df.to_string(index=False))
            else:
                print("Nenhum resultado encontrado")
                
            print(f"\n")

def main():

    CONN = conectar_postgree()
    executar_consultas(CONN, BASE_DIR / "sql" / "queries.sql")

    print("Concluído")

if __name__ == "__main__":
    main()