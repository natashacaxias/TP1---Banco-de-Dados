import gzip
import psycopg2
import re, argparse, sys
from pathlib import Path
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# padrões (mantidos iguais)
rx_id = re.compile(r'^\s*Id:\s*(\d+)', re.IGNORECASE)
rx_asin = re.compile(r'^\s*ASIN:\s*(\S+)', re.IGNORECASE)
rx_status = re.compile(r'^\s*discontinued\s+product\s*$', re.IGNORECASE)
rx_title = re.compile(r'^\s*title:\s*(.*)', re.IGNORECASE)
rx_group = re.compile(r'^\s*group:\s*(.*)', re.IGNORECASE)
rx_salesrank = re.compile(r'^\s*salesrank:\s*(\d+)', re.IGNORECASE)
rx_similar = re.compile(r'^\s*similar:\s*(?:\d+\s+)?(.*)', re.IGNORECASE)
rx_categories = re.compile(r'^\s*categories:\s*(.*)', re.IGNORECASE)
rx_reviews_summary = re.compile(r'^\s*reviews:\s*(.*)', re.IGNORECASE)
rx_review_line = re.compile(
    r'^\s*(\d{4}-\d{1,2}-\d{1,2})\s+customer:\s*(\S+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)',
    re.IGNORECASE
)

# Criação das tabelas
def create_tables(conn, esquema):
    with open(esquema, 'r') as es:
        cur = conn.cursor()
        cur.execute(es.read())
        conn.commit()

class CategoriaManager:
    def __init__(self, conn):
        self.conn = conn
        self.name_to_id = {}
        self.parent_pairs = set()

    def load_existing(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, nome FROM Categoria")
        for cid, nome in cur.fetchall():
            self.name_to_id[nome] = cid

    def get_or_create_id(self, nome):
        nome = nome.strip()
        if not nome:
            return None
        if nome in self.name_to_id:
            return self.name_to_id[nome]
        
        cur = self.conn.cursor()
        
        try:
            cur.execute("INSERT INTO Categoria (nome) VALUES (%s) RETURNING id", (nome,))
            row = cur.fetchone()
            if row:
                cid = row[0]
                self.name_to_id[nome] = cid
                return cid
            else:
                # Se não inseriu, buscar o ID existente
                cur.execute("SELECT id FROM Categoria WHERE nome = %s", (nome,))
                row = cur.fetchone()
                if row:
                    cid = row[0]
                    self.name_to_id[nome] = cid
                    return cid
            return None
        except Exception as e:
            self.conn.rollback()
            print(f"Erro ao inserir categoria {nome}: {e}")
            return None

    def add_hierarchy(self, child_name, parent_name):
        child_id = self.get_or_create_id(child_name)
        parent_id = self.get_or_create_id(parent_name) if parent_name else None
        if child_id:
            self.parent_pairs.add((child_id, parent_id))

    def persist_hierarchy_bulk(self, cur):
        for child_id, parent_id in self.parent_pairs:
            try:
                cur.execute("""
                    INSERT INTO Categoria_Hierarquia (id_categoria, id_categoria_pai)
                    SELECT %s, %s WHERE NOT EXISTS (
                        SELECT 1 FROM Categoria_Hierarquia 
                        WHERE id_categoria = %s AND id_categoria_pai IS NOT DISTINCT FROM %s
                    )
                """, (child_id, parent_id, child_id, parent_id))
            except Exception as e:
                print(f"Erro ao inserir hierarquia {child_id}-{parent_id}: {e}")
                continue

def parse_amazon_meta(filepath, esquema, db_config=None, limite=None, flush_every=1000):
    # Configuração de conexão PostgreSQL
    if db_config is None:
        db_config = {
            'host': 'localhost',
            'database': 'amazon_db',
            'user': 'postgres',
            'password': 'password',
            'port': '5432'
        }
    
    conn = psycopg2.connect(**db_config)
    conn.autocommit = False
    
    create_tables(conn, esquema)

    cat_mgr = CategoriaManager(conn)
    cat_mgr.load_existing()

    # Buffers por tabela
    buf_produto = []          # tuples (id, asin, titulo, grupo, salesrank, ativo)
    buf_similar = []          # tuples (id_produto, asin_similar)
    buf_categoria_produto = []# tuples (id_produto, id_categoria)
    buf_avaliacao = []        # tuples (id_produto, data, user, rating, votes, helpful)
    

    produtos_processados = 0
    produto = None
    in_review_block = False

    def flush_all(commit=True):
        nonlocal conn
        cur = conn.cursor()
        try:
            if buf_produto:
                cur.executemany("""
                    INSERT INTO Produto (id, asin, titulo, grupo, ranking_vendas, ativo)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                    asin = EXCLUDED.asin,
                    titulo = EXCLUDED.titulo,
                    grupo = EXCLUDED.grupo,
                    ranking_vendas = EXCLUDED.ranking_vendas,
                    ativo = EXCLUDED.ativo
                """, buf_produto)
                buf_produto.clear()
            
            if buf_similar:
                cur.executemany("""
                    INSERT INTO Produto_Similar (id_produto, asin_similar)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, buf_similar)
                buf_similar.clear()
            
            if buf_categoria_produto:
                cur.executemany("""
                    INSERT INTO Categoria_Produto (id_produto, id_categoria)
                    SELECT %s, %s WHERE NOT EXISTS (
                        SELECT 1 FROM Categoria_Produto 
                        WHERE id_produto = %s AND id_categoria = %s
                    )
                """, [(a, b, a, b) for (a, b) in buf_categoria_produto])
                buf_categoria_produto.clear()
            
            if buf_avaliacao:
                cur.executemany("""
                    INSERT INTO Avaliacao (id_produto, data, id_usuario, classificacao, votos, util)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, buf_avaliacao)
                buf_avaliacao.clear()
            
            # persistir hierarquia em lote
            cat_mgr.persist_hierarchy_bulk(cur)
            
            if commit:
                conn.commit()
                print(f"Commit realizado para {produtos_processados} produtos")
                
        except Exception as e:
            conn.rollback()
            print(f"Erro no flush: {e}")
            raise

    # Leitura do gzip
    with gzip.open(filepath, 'rt', encoding='utf-8', errors='replace') as f:
        for linha in f:
                                
            m = rx_id.match(linha)
            if m:
                
                if produto:
                    buf_produto.append((
                        produto['id_produto'],
                        produto.get('asin'),
                        produto.get('titulo'),
                        produto.get('grupo'),
                        produto.get('salesrank'),
                        produto.get('ativo')
                    ))
                    # similars
                    for s in produto['similar_list']:
                        if s and produto.get('asin') and s != produto.get('asin'):
                            buf_similar.append((produto['id_produto'], s))
                    
                    for cat_block in produto['categories_raw']:
                        parts = [p for p in cat_block.split('|') if p.strip()]
                        names = []
                        for p in parts:
                            name = re.split(r'\[\d+\]', p)[0].strip()
                            if name:
                                names.append(name)
                        parent = None
                        for name in names:
                            cid = cat_mgr.get_or_create_id(name)
                            if parent:
                                cat_mgr.add_hierarchy(name, parent)
                            parent = name
                        if names:
                            last_name = names[-1]
                            cid_last = cat_mgr.get_or_create_id(last_name)
                            if cid_last:
                                buf_categoria_produto.append((produto['id_produto'], cid_last))
                
                    for r in produto['reviews_lines']:
                        buf_avaliacao.append((produto['id_produto'], r['data'], r['user'], r['rating'], r['votes'], r['helpful']))

                    produtos_processados += 1

                    # flush periódico
                    if produtos_processados % flush_every == 0:
                        flush_all(commit=True)
                        print(f"Flush em {produtos_processados} produtos...")

                    if limite and produtos_processados >= limite:
                        break

                # inicia novo produto
                produto = {
                    'id_produto': int(m.group(1)),
                    'asin': None,
                    'titulo': None,
                    'grupo': None,
                    'ativo': True,
                    'salesrank': None,
                    'similar_list': [],
                    'categories_raw': [],
                    'reviews_summary_raw': None,
                    'reviews_lines': []
                }
                in_review_block = False
                continue

            if produto is None:
                continue

            m = rx_asin.match(linha)
            if m:
                produto['asin'] = m.group(1).strip()
                continue

            m = rx_status.match(linha)
            if m:
                produto['ativo'] = False

            m = rx_title.match(linha)
            if m:
                produto['titulo'] = m.group(1).strip()
                continue

            m = rx_group.match(linha)
            if m:
                produto['grupo'] = m.group(1).strip()
                continue

            m = rx_salesrank.match(linha)
            if m:
                try:
                    produto['salesrank'] = int(m.group(1))
                except:
                    produto['salesrank'] = None
                continue

            m = rx_similar.match(linha)
            if m:
                block = m.group(1).strip()
                if block:
                    asins = [s for s in block.split() if s]
                    produto['similar_list'].extend(asins)
                continue

            m = rx_categories.match(linha)
            if m:
                produto['categories_raw'].append(m.group(1).strip())
                continue

            m = rx_reviews_summary.match(linha)
            if m:
                produto['reviews_summary_raw'] = m.group(1).strip()
                in_review_block = True
                continue

            if in_review_block:
                m = rx_review_line.match(linha)
                if m:
                    data = m.group(1)
                    user = m.group(2)
                    rating = int(m.group(3))
                    votes = int(m.group(4))
                    helpful = int(m.group(5))
                    produto['reviews_lines'].append({'data': data, 'user': user, 'rating': rating, 'votes': votes, 'helpful': helpful})
                    continue
                else:
                    if linha.strip() == "":
                        in_review_block = False
                    continue

        # fim do arquivo: persistir último produto
        if produto:
            buf_produto.append((
                produto['id_produto'],
                produto.get('asin'),
                produto.get('titulo'),
                produto.get('grupo'),
                produto.get('salesrank'),
                produto.get('ativo')
            ))
            for s in produto['similar_list']:
                if s and produto.get('asin') and s != produto.get('asin'):
                    buf_similar.append((produto['id_produto'], s))
            for cat_block in produto['categories_raw']:
                parts = [p for p in cat_block.split('|') if p.strip()]
                names = []
                for p in parts:
                    name = re.split(r'\[\d+\]', p)[0].strip()
                    if name:
                        names.append(name)
                parent = None
                for name in names:
                    cid = cat_mgr.get_or_create_id(name)
                    if parent:
                        cat_mgr.add_hierarchy(name, parent)
                    parent = name
                if names:
                    last_name = names[-1]
                    cid_last = cat_mgr.get_or_create_id(last_name)
                    if cid_last:
                        buf_categoria_produto.append((produto['id_produto'], cid_last))
            for r in produto['reviews_lines']:
                buf_avaliacao.append((produto['id_produto'], r['data'], r['user'], r['rating'], r['votes'], r['helpful']))
            produtos_processados += 1

    # flush final (commit)
    flush_all(commit=True)
    conn.close()
    print(f"Concluído: {produtos_processados} produtos processados.")

def create_database(db_config):
    """
    Conecta ao PostgreSQL e cria o banco de dados se ele não existir.
    """
    
    # Configuração para conectar ao banco 'postgres' padrão
    temp_config = db_config.copy()
    temp_config['database'] = 'postgres'  # CONECTAR AO BANCO 'postgres' PADRÃO
    
    try:
        conn = psycopg2.connect(**temp_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Verificar se o banco de dados já existe
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_config['database'],))
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_config['database'])))
            print(f"Banco de dados '{db_config['database']}' criado com sucesso.")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao verificar/criar banco de dados: {e}")
        sys.exit(1)


# Execução 
if __name__ == "__main__":

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

    create_database(DB_CONFIG)
    
    BASE_DIR = Path(__file__).parent.parent

    INPUT_GZ = args.snap_input
    ESQUEMA = BASE_DIR / "sql" /  "schema.sql"
    LIMITE = None     # definir para testes rápidos
    FLUSH_EVERY = 1000

    parse_amazon_meta(INPUT_GZ, ESQUEMA, db_config=DB_CONFIG, limite=LIMITE, flush_every=FLUSH_EVERY)