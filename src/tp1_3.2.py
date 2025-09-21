import gzip, psycopg2, argparse
from pathlib import Path
from psycopg2 import sql

categorias_pai = {}
categorias_nome = {}
similares = set([])
BASE_DIR = Path(__file__).parent.parent

def conectar_postgree():

    parser = argparse.ArgumentParser(description='Processar dados do Amazon Meta')
    parser.add_argument('--db-host', required=True, help='Host do PostgreSQL')
    parser.add_argument('--db-port', required=True, help='Porta do PostgreSQL')
    parser.add_argument('--db-name', required=True, help='Nome do banco de dados')
    parser.add_argument('--db-user', required=True, help='Usuário do PostgreSQL')
    parser.add_argument('--db-pass', required=True, help='Senha do PostgreSQL')
    parser.add_argument('--snap-input', required=True, help='Caminho para o arquivo de entrada .gz')
    
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


def criar_banco_dados(conn):
    try:
        conn.autocommit = True  # Necessário para criar bancos de dados
        
        cursor = conn.cursor()
        
        # Verificar se o banco já existe
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'ecommerce'")
        exists = cursor.fetchone()
        
        if exists:
            print(f"O banco de dados ecommerce já existe!")
            return False
        
        # Criar o banco de dados
        create_db_query = sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier("ecommerce")
        )
        
        cursor.execute(create_db_query)
        print(f"Banco de dados ecommerce criado com sucesso!")
        
        cursor.close()        
        return True
        
    except psycopg2.Error as e:
        print(f"Erro ao criar banco de dados: {e}")
        return False

def criar_tabelas(conn, esquema):
    with open(esquema, 'r') as es:
        cur = conn.cursor()
        cur.execute(es.read())
        conn.commit()

def parse_insere(conn, metaPath):

    cur = conn.cursor()
    with gzip.open(metaPath, 'rt', encoding='utf-8', errors='replace') as meta:
        produtos = meta.read().split("\n\n")[1:]
        for p in produtos:
            atributos = {}
            reviews = []

            l = p.split("\n")

            # id
            aux = list(" ".join(l[0].split()).split())
            atributos["id"] = int(aux[1])

            # asin
            aux = list(" ".join(l[1].split()).split())
            atributos["asin"] = aux[1]

            aux = l[2].strip()
            if (aux == "discontinued product"):
                atributos["ativo"] = False

                # insere no BD
                if not atributos["ativo"]:
                    cur.execute("""INSERT INTO Produto (id, asin, ativo)
                                    VALUES (%s, %s, %s)""", (atributos["id"], atributos["asin"], atributos["ativo"]))
                    continue
            else:
                #title
                atributos["ativo"] = True
                aux = list(" ".join(aux.split()).split(":", 1))
                atributos["title"] = aux[1]
            
            # group
            aux = list(" ".join(l[3].split()).split())
            atributos["group"] = aux[1]

            # salesrank
            aux = list(" ".join(l[4].split()).split())
            atributos["salesrank"] = int(aux[1])

            # similar
            aux = list(" ".join(l[5].split()).split())
            similares = []
            for i in range(int(aux[1])):
                similares.__add__([atributos["id"],aux[i+2]])

            # categories
            qtd_categorias = int(list(" ".join(l[6].split()).split())[1])
            print(qtd_categorias)
            c_aux = set([])
            i = 7
            while(i<7+qtd_categorias):
                aux = list(l[i].strip().split("|"))[1:]
                for j in range(len(aux)):
                    aux[j] = aux[j].split("[")
                    if(j!=0):
                        categorias_pai[aux[j][1][:-1]] = aux[j-1][1][:-1]
                    categorias_nome[aux[j][1][:-1]] = aux[j][0]
                    c_aux.add(aux[j][1][:-1])
                i+=1
            atributos["categorias"] = c_aux

            # reviews
            aux = list(" ".join(l[i].split()).split())
            qtd_reviews = int(aux[2])
            downloaded = int(aux[4])
            avg_rating = float(aux[7])
            atributos_reviews = {}

            i+=1
            i_aux = i
            while(i<i_aux+qtd_reviews):
                aux = list(l[i].strip().split())
                print(aux)
                atributos_reviews["data"] = aux[0]
                atributos_reviews["customer"] = aux[2]
                atributos_reviews["rating"] = aux[4]
                atributos_reviews["votes"] = aux[6]
                atributos_reviews["hepful"] = aux[8]
                atributos_reviews["product"] = atributos["id"]
                reviews.append(atributos_reviews)
                i+=1

            # inserir produtos
            cur.execute("""INSERT INTO Produto (id, asin, titulo, grupo, ranking_vendas, ativo) VALUES (%s, %s, %s, %s, %s, %s)""", (atributos["id"], atributos["asin"], atributos["title"], atributos["group"], atributos["salesrank"], atributos["ativo"]))

            # inserir reviews
            for r in reviews:
                cur.execute("""INSERT INTO Avaliacao (id_produto, data, id_usuario, classificacao, votos, util) VALUES (%s, %s, %s, %s, %s, %s)""", (r["product"], r["data"], r["customer"], r["rating"], r["votes"], r["hepful"]))
            
            # inserir categorias, categorias_produtos e categorias_hierarquia
            for r in atributos["categorias"]:
                cur.execute("""INSERT INTO Categoria (id, nome)
                                VALUES (%s, %s)
                                ON CONFLICT (id) DO NOTHING;""", (r, categorias_nome[r]))
                cur.execute("""INSERT INTO Categoria_Produto (id_produto, id_categoria)
                                VALUES (%s, %s)""", (atributos["id"], r))

def inserir_similares(conn):

    cur = conn.cursor()
    
    for a, b in similares:
        cur.execute("""INSERT INTO Produto_Similar(id_produto, ains_similar)
                        VALUES(%s, %s)""", (a, b))
        
def inserir_hierarquia(conn):

    cur = conn.cursor()
    
    for a, b in categorias_pai.items():
        cur.execute("""INSERT INTO Categoria_Hierarquia (id_categoria, id_categoria_pai)
                                VALUES (%s, %s)""", (a, b))

def main():

   
    CONN = conectar_postgree()
    criar_banco_dados(CONN)
    criar_tabelas(CONN, BASE_DIR / "sql" / "schema.sql")
    parse_insere(CONN, BASE_DIR / "data" / "mini_teste.txt.gz")
    inserir_similares(CONN)

    print("Concluído")

if __name__ == "__main__":
    main()