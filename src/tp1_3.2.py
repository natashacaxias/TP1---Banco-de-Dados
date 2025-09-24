import re, psycopg2, gzip,argparse, time
from pathlib import Path
from psycopg2 import sql
from psycopg2.extras import execute_values

# Constantes e expressões regulares
BASE_DIR = Path(__file__).parent.parent
pattern_info = re.compile(r'\s*([^:]+):\s*(.*)').match
extract_asins = re.compile(r'\b(\w{10})\b').findall
regex_categorias = re.compile(r"\|([^\n|]+?)\s*\[(\d+)\]")
extract_downloaded_reviews = re.compile(r'\s*downloaded:\s*(\d+)').search
extract_total_reviews = re.compile(r'\s*total:\s*(\d+)').search
check_discontinued = re.compile(r'^\s*discontinued product\s*$').match
batch_size = 2000
product_count = 0
product_total = 0

current_product = {
    'id': None,
    'asin': None,
    'title': None,
    'grupo': None,
    'salesrank': None,
    'ativo': True,
}

# Batches globais para inserção em lote
produtos_batch = []
reviews_batch = []
categoria_batch = set()
categoria_produto_batch = set()
similares_all = set()
categoria_pai = {}

def inserir_batch():

    global product_count, product_total

    produtos_batch.append((
        current_product['id'],
        current_product['asin'],
        current_product['title'],
        current_product['grupo'],
        current_product['salesrank'],
        current_product['ativo'],
    ))
    product_count += 1
    
    # Inserir lotes quando atingir o batch_size
    if product_count >= batch_size:
        insere_lotes(conn)
        product_total += product_count
        product_count = 0
        if product_total % 10000 == 0:
            print(f"{product_total} produtos processados.")
    
    current_product['id'] = None
    current_product['asin'] = None
    current_product['title'] = None
    current_product['grupo'] = None
    current_product['salesrank'] = None
    current_product['ativo'] = True

def get_argumentos():
    parser = argparse.ArgumentParser(description='Processar dados do Amazon Meta')
    parser.add_argument('--db-host', required=True)
    parser.add_argument('--db-port', required=True)
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--db-user', required=True)
    parser.add_argument('--db-pass', required=True)
    parser.add_argument('--snap-input', required=True)
    return parser.parse_args()


def conectar_postgree(args, database=None):
    DB_CONFIG = {
        'host': args.db_host,
        'port': args.db_port,
        'database': database if database else args.db_name,
        'user': args.db_user,
        'password': args.db_pass,
    }
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"Banco de Dados conectado com sucesso! (Database: {DB_CONFIG['database']})")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao BD: {e}")
        exit(1)


def criar_banco_dados(conn, nome):
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (nome,))
    if cursor.fetchone():
        print(f"O banco de dados {nome} já existe!")
        return True
    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(nome)))
    print(f"Banco de dados {nome} criado com sucesso!")
    cursor.close()
    return True

def criar_tabelas(conn, esquema):
    with open(esquema, 'r') as es:
        cur = conn.cursor()
        cur.execute(es.read())
        conn.commit()

def insere_lotes(conn):
    cur = conn.cursor()

    if produtos_batch:
        execute_values(cur,"""INSERT INTO Product (id, asin, title, grupo, salesrank, active) VALUES %s""", produtos_batch)
    if categoria_batch:
        execute_values(cur,"""INSERT INTO Category (id, nome) VALUES %s ON CONFLICT DO NOTHING""", list(categoria_batch))
    if categoria_produto_batch:
        execute_values(cur,"""INSERT INTO Category_Product (id_Product, id_Category) VALUES %s""", list(categoria_produto_batch))
    if reviews_batch:
        execute_values(cur,"""INSERT INTO Review (id_Product, data, id_custumer, rating, votes, helpful) VALUES %s""", reviews_batch)
        
    conn.commit()

    produtos_batch.clear()
    reviews_batch.clear()
    categoria_batch.clear()
    categoria_produto_batch.clear()

def parse_insere(conn, path):

    with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as file:
        
        while True:
            
            linha = file.readline()
            
            # Sair se chegou ao final do arquivo
            if linha == "": break 

            match = pattern_info(linha)
            if match: 
                pass
            elif check_discontinued(linha):
                current_product['ativo'] = False
                inserir_batch()
                continue
            else: continue
            
            key = match.group(1)
            value = match.group(2)
            if key == "Id":
                current_product['id'] = int(value)
            elif key == "ASIN":
                current_product['asin'] = value
            elif key == "title":
                current_product['title'] = value
                current_product['ativo'] = True
            elif key == "group":
                current_product['grupo'] = value
            elif key == "salesrank":
                current_product['salesrank'] = int(value)
            elif key == "similar":
                similares = extract_asins(value)
                for s in similares:
                    similares_all.add((current_product['id'], s))
            elif key == "categories":
                for x in range(int(value)):
                    categorias_line = file.readline()
                    categorias = re.findall(regex_categorias, categorias_line)
                    categorias = [(int(idd), nome) for nome, idd in categorias]
                    for i in range(len(categorias)):
                        if i != 0:  # hierarquia
                            categoria_pai[int(categorias[i][0])] = int(categorias[i-1][0])
                        categoria_batch.add(categorias[i])
                        categoria_produto_batch.add((current_product['id'], categorias[i][0]))

            elif key == "reviews":
                down_reviews = extract_downloaded_reviews(value)
                current_product['total_reviews'] = extract_total_reviews(value)
                if down_reviews:
                    down_reviews = int(down_reviews.group(1))
                else:
                    down_reviews = 0
                for i in range(down_reviews):
                    line = file.readline()
                    partes = line.split()
                    if len(partes) >= 9:
                        data = partes[0]
                        id_custumer = partes[2]
                        rating = int(partes[4])
                        votes = int(partes[6])
                        helpful = int(partes[8])
                        reviews_batch.append((current_product['id'], data, id_custumer, rating, votes, helpful))
                inserir_batch()

                    
        # Inserir registros restantes
        global product_total, product_count
        insere_lotes(conn)
        product_total += product_count
        print(f"{product_total} produtos processados.")

    cur = conn.cursor()

    # Product_Similar e Category_hierarchy tratados separadamente para não violar restrições
    print("\nProcessando Similares e Hierarquia de Categorias...")
    if similares_all:
        execute_values(cur, 
            """INSERT INTO Product_Similar (id_Product, asin_similar)
            SELECT s.id_Product, s.asin_similar
            FROM (VALUES %s) AS s(id_Product, asin_similar)
            WHERE EXISTS (SELECT 1 FROM Product p1 WHERE p1.id = s.id_Product)
              AND EXISTS (SELECT 1 FROM Product p2 WHERE p2.asin = s.asin_similar)
            ON CONFLICT (id_Product, asin_similar) DO NOTHING""", 
            list(similares_all))

    if categoria_pai:
        execute_values(cur, 
            """INSERT INTO Category_hierarchy (id_Category, id_Category_pai) 
            VALUES %s ON CONFLICT DO NOTHING""", 
            [(cat_id, parent_id) for cat_id, parent_id in categoria_pai.items()])

    conn.commit()  # Commit explícito após as inserções finais


if __name__ == "__main__":
    args = get_argumentos()
    
    conn = conectar_postgree(args, database="postgres")
    criar_banco_dados(conn, args.db_name)
    conn.close()
    
    conn = conectar_postgree(args, database=args.db_name)
    criar_tabelas(conn, BASE_DIR / "sql" / "schema.sql")

    print("Executando o parsing e inserindo dados no banco...")
    inicio = time.perf_counter()
    parse_insere(conn, args.snap_input)
    fim = time.perf_counter()
    print(f"Tempo total de execução: {((fim - inicio)/60):.2f} minutos")
    print("Concluído!")
    conn.close()