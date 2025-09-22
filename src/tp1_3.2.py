import gzip
import psycopg2
import argparse
from pathlib import Path
from psycopg2 import sql
from psycopg2.extras import execute_values

BASE_DIR = Path(__file__).parent.parent

categorias_pai = {}
similares = set()

def conectar_postgree(args):
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


def criar_banco_dados(conn):
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'ecommerce'")
    if cursor.fetchone():
        print("O banco de dados ecommerce já existe!")
        return False
    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("ecommerce")))
    print("Banco de dados ecommerce criado com sucesso!")
    cursor.close()
    return True


def criar_tabelas(conn, esquema):
    with open(esquema, 'r') as es:
        cur = conn.cursor()
        cur.execute(es.read())
        conn.commit()


def parse_insere(conn, metaPath, batch_size=1000):
    cur = conn.cursor()
    produtos_batch = []
    reviews_batch = []
    categoria_batch = []
    categoria_produto_batch = []

    with gzip.open(metaPath, 'rt', encoding='utf-8', errors='replace') as meta:
        buffer = []
        for line in meta:
            if line.startswith("# Full") or line.startswith("Total"):
                continue
            if line.strip() == "":
                if buffer:
                    processa_produto(buffer, produtos_batch, reviews_batch, categoria_batch, categoria_produto_batch)
                    buffer = []

                    # Inserção em lote
                    if len(produtos_batch) >= batch_size:
                        insere_lotes(cur, produtos_batch, reviews_batch, categoria_batch, categoria_produto_batch)
                        produtos_batch.clear()
                        reviews_batch.clear()
                        categoria_batch.clear()
                        categoria_produto_batch.clear()
            else:
                buffer.append(line)
        if buffer:
            processa_produto(buffer, produtos_batch, reviews_batch, categoria_batch, categoria_produto_batch)
            insere_lotes(cur, produtos_batch, reviews_batch, categoria_batch, categoria_produto_batch)

    conn.commit()


def processa_produto(linhas, produtos_batch, reviews_batch, categoria_batch,categoria_produto_batch):
    atributos = {}
    l = linhas
    
    # id
    atributos["id"] = int(l[0].split()[1])

    # asin
    atributos["asin"] = l[1].split()[1]

    # title ou descontinuado
    if l[2].strip() == "discontinued product":
        atributos["ativo"] = False
        produtos_batch.append((atributos["id"], atributos["asin"], False, None, None, False))
        return
    else:
        atributos["ativo"] = True
        atributos["title"] = l[2].split(":", 1)[1].strip()[:255]

    # group
    atributos["group"] = l[3].split()[1][:255]

    # salesrank
    atributos["salesrank"] = int(l[4].split()[1])

    # similares
    tokens = l[5].split()
    n_similares = int(tokens[1])
    for s in tokens[2:2+n_similares]:
        similares.add((atributos["id"], s))

    # categorias
    qtd_categorias = int(l[6].split()[1])
    c_aux = set()
    for i in range(7, 7+qtd_categorias):
        partes = l[i].strip().split("|")[1:]
        prev_cid = -1
        for j, cat in enumerate(partes):
            o = cat.split("[")
            if len(o)!=2:
                continue
            nome, cid = o
            cid = cid[:-1]
            categoria_batch.append((nome[:255],cid))
            if j != 0:
                categorias_pai[cid] = prev_cid
            c_aux.add(cid)
            prev_cid = cid
    atributos["categorias"] = c_aux

    # reviews
    aux_i = 7+qtd_categorias
    k = l[aux_i]
    aux = list(" ".join(k.split()).split())
    qtd_reviews = int(aux[2])
    downloaded = int(aux[4])
    avg_rating = float(aux[7])
    atributos_reviews = {}

    aux_i+=1
    for i in range(aux_i, aux_i+downloaded):
        aux = l[i].strip().split()
        #print(aux)
        reviews_batch.append((
            atributos["id"],
            aux[0],
            aux[2],
            aux[4],
            aux[6],
            aux[8],
        ))

    # produtos batch
    produtos_batch.append((
        atributos["id"],
        atributos["asin"],
        atributos["title"],
        atributos["group"],
        atributos["salesrank"],
        True
    ))

    # categoria_produto batch
    for cid in c_aux:
        categoria_produto_batch.append((atributos["id"], cid))


def insere_lotes(cur, produtos_batch, reviews_batch, categoria_batch, categoria_produto_batch):
    try:
        if produtos_batch:
            execute_values(cur,
            """INSERT INTO Produto (id, asin, titulo, grupo, ranking_vendas, ativo)
            VALUES %s ON CONFLICT(asin) DO NOTHING""", produtos_batch)
    except psycopg2.errors.StringDataRightTruncation:
        print(produtos_batch)
    if reviews_batch:
        execute_values(cur,
            """INSERT INTO Avaliacao (id_produto, data, id_usuario, classificacao, votos, util)
               VALUES %s""", reviews_batch)
    if categoria_batch:
        execute_values(cur,
            """INSERT INTO Categoria (nome, id)
               VALUES %s ON CONFLICT DO NOTHING""", categoria_batch)
    if categoria_produto_batch:
        execute_values(cur,
            """INSERT INTO Categoria_Produto (id_produto, id_categoria)
               VALUES %s ON CONFLICT DO NOTHING""", categoria_produto_batch)

def inserir_similares(conn):
    cur = conn.cursor()
    if similares:
        execute_values(cur,
            """INSERT INTO Produto_Similar (id_produto, asin_similar)
            SELECT s.id_produto, s.asin_similar
            FROM (VALUES %s) AS s(id_produto, asin_similar)
            WHERE EXISTS (
                SELECT 1 FROM Produto p1 WHERE p1.id = s.id_produto
            ) AND EXISTS (
                SELECT 1 FROM Produto p2 WHERE p2.asin = s.asin_similar
            )
            ON CONFLICT (id_produto, asin_similar) DO NOTHING""", list(similares))
    conn.commit()


def inserir_hierarquia(conn):
    cur = conn.cursor()
    if categorias_pai:
        execute_values(cur,
            """INSERT INTO Categoria_Hierarquia (id_categoria, id_categoria_pai)
               VALUES %s ON CONFLICT DO NOTHING""", list(categorias_pai.items()))
    conn.commit()


def get_args():
    parser = argparse.ArgumentParser(description='Processar dados do Amazon Meta')
    parser.add_argument('--db-host', required=True)
    parser.add_argument('--db-port', required=True)
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--db-user', required=True)
    parser.add_argument('--db-pass', required=True)
    parser.add_argument('--snap-input', required=True)
    return parser.parse_args()


def main():
    args = get_args()
    conn = conectar_postgree(args)
    criar_banco_dados(conn)
    criar_tabelas(conn, BASE_DIR / "sql" / "schema.sql")

    print("Executando o parsing e inserindo dados no banco...")
    parse_insere(conn, args.snap_input)
    inserir_similares(conn)
    inserir_hierarquia(conn)
    print("Concluído!")


if __name__ == "__main__":
    main()
