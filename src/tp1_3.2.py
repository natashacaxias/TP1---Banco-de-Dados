import argparse
import psycopg

parser = argparse.ArgumentParser()
parser.add_argument('--db-host', required=True)
parser.add_argument('--db-port', required=True)
parser.add_argument('--db-name', required=True)
parser.add_argument('--db-user', required=True)
parser.add_argument('--db-pass', required=True)
parser.add_argument('--input', required=True)
args = parser.parse_args()

try:
    conn = psycopg.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_pass
    )
    print("✅ Conexão com o PostgreSQL estabelecida com sucesso.")
    conn.close()
except Exception as e:
    print("❌ Erro ao conectar ao PostgreSQL:", e)
    exit(1)
