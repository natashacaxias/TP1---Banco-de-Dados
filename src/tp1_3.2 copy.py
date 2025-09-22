import gzip
import psycopg2
import argparse
from pathlib import Path
from psycopg2 import sql, extras
from typing import Dict, List, Set, Tuple
import time

# Constantes globais
BATCH_SIZE = 1000  # Tamanho do lote para inserções em batch

def conectar_postgresql(args):
    """Conecta ao PostgreSQL com configurações otimizadas"""
    DB_CONFIG = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_pass,
        'application_name': 'amazon_data_loader',
    }

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Configurações para melhor performance
        conn.autocommit = False
        print("Banco de Dados conectado com sucesso!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao BD: {e}")
        exit(1)

def criar_banco_dados(conn):
    """Cria o banco de dados se não existir"""
    try:
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'ecommerce'")
            exists = cursor.fetchone()
            
            if exists:
                print("O banco de dados ecommerce já existe!")
                return False
            
            cursor.execute("CREATE DATABASE ecommerce")
            print("Banco de dados ecommerce criado com sucesso!")
            
        return True
        
    except psycopg2.Error as e:
        print(f"Erro ao criar banco de dados: {e}")
        return False

def criar_tabelas(conn, esquema):
    """Cria as tabelas do esquema"""
    with open(esquema, 'r') as es:
        with conn.cursor() as cur:
            cur.execute(es.read())
        conn.commit()

def processar_arquivo(conn, meta_path: str):
    """Processa o arquivo de forma otimizada usando batches"""
    # Estruturas para armazenamento em lote
    produtos_batch: List[Tuple] = []
    avaliacoes_batch: List[Tuple] = []
    categorias_batch: Set[str] = set()
    categorias_produto_batch: List[Tuple] = []
    categorias_hierarquia_batch: Set[Tuple] = set()
    produtos_similares_batch: List[Tuple] = []
    
    # Contadores para monitoramento
    contador_produtos = 0
    contador_linhas = 0
    
    start_time = time.time()
    
    try:
        with gzip.open(meta_path, 'rt', encoding='utf-8', errors='replace') as meta:
            for linha in meta:
                contador_linhas += 1
                
                if linha.strip() == "":
                    # Processa batch quando encontrar linha vazia (separador de produtos)
                    if produtos_batch:
                        _inserir_batches(conn, produtos_batch, avaliacoes_batch, 
                                       categorias_batch, categorias_produto_batch,
                                       categorias_hierarquia_batch, produtos_similares_batch)
                        
                        # Limpa os batches
                        produtos_batch.clear()
                        avaliacoes_batch.clear()
                        categorias_batch.clear()
                        categorias_produto_batch.clear()
                        categorias_hierarquia_batch.clear()
                        produtos_similares_batch.clear()
                    
                    continue
                
                # Processa a linha atual
                _processar_linha(linha, produtos_batch, avaliacoes_batch, categorias_batch,
                               categorias_produto_batch, categorias_hierarquia_batch,
                               produtos_similares_batch)
                
                # Insere em batch quando atingir o tamanho máximo
                if len(produtos_batch) >= BATCH_SIZE:
                    _inserir_batches(conn, produtos_batch, avaliacoes_batch, 
                                   categorias_batch, categorias_produto_batch,
                                   categorias_hierarquia_batch, produtos_similares_batch)
                    
                    # Limpa os batches
                    produtos_batch.clear()
                    avaliacoes_batch.clear()
                    categorias_batch.clear()
                    categorias_produto_batch.clear()
                    categorias_hierarquia_batch.clear()
                    produtos_similares_batch.clear()
        
        # Insere os registros restantes
        if produtos_batch:
            _inserir_batches(conn, produtos_batch, avaliacoes_batch, 
                           categorias_batch, categorias_produto_batch,
                           categorias_hierarquia_batch, produtos_similares_batch)
            
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Erro durante o processamento: {e}")
        raise
    
    end_time = time.time()
    print(f"Processamento concluído em {end_time - start_time:.2f} segundos")
    print(f"Total de linhas processadas: {contador_linhas}")

def _processar_linha(linha: str, produtos_batch: List, avaliacoes_batch: List,
                    categorias_batch: Set, categorias_produto_batch: List,
                    categorias_hierarquia_batch: Set, produtos_similares_batch: List):
    """Processa uma linha individual do arquivo e atualiza os batches"""
    linha = linha.strip()
    
    # Variáveis de estado para o produto atual
    current_product = {
        'id': None,
        'asin': None,
        'title': None,
        'group': None,
        'salesrank': None,
        'ativo': True,
        'categorias': set(),
        'reviews': []
    }
    
    if linha.startswith("Id:"):
        # Processa ID do produto
        parts = linha.split()
        if len(parts) >= 2:
            try:
                current_product['id'] = int(parts[1])
            except ValueError:
                pass
    
    elif linha.startswith("ASIN:"):
        # Processa ASIN
        parts = linha.split()
        if len(parts) >= 2:
            current_product['asin'] = parts[1]
    
    elif linha.startswith("title:"):
        # Processa título
        parts = linha.split(':', 1)
        if len(parts) >= 2:
            current_product['title'] = parts[1].strip()
    
    elif linha.startswith("group:"):
        # Processa grupo
        parts = linha.split()
        if len(parts) >= 2:
            current_product['group'] = parts[1]
    
    elif linha.startswith("salesrank:"):
        # Processa salesrank
        parts = linha.split()
        if len(parts) >= 2:
            try:
                current_product['salesrank'] = int(parts[1])
            except ValueError:
                pass
    
    elif linha.startswith("similar:"):
        # Processa produtos similares
        parts = linha.split()
        if len(parts) >= 3:
            num_similar = int(parts[1])
            for i in range(2, 2 + num_similar):
                if i < len(parts):
                    produtos_similares_batch.append((current_product['id'], parts[i]))
    
    elif linha.startswith("categories:"):
        # Processa categorias
        parts = linha.split()
        if len(parts) >= 2:
            num_categories = int(parts[1])
            # As próximas linhas serão as categorias
    
    elif "|" in linha and linha.count('[') == linha.count(']'):
        # Processa linha de categoria (formato: cat_name[cat_id])
        category_parts = linha.strip().split('|')[1:]  # Ignora o primeiro elemento vazio
        previous_cat_id = None
        
        for j, cat_part in enumerate(category_parts):
            if '[' in cat_part and ']' in cat_part:
                cat_name, cat_id = cat_part.split('[', 1)
                cat_id = cat_id.rstrip(']')
                
                # Adiciona categoria ao batch
                categorias_batch.add((cat_id, cat_name))
                
                # Adiciona relação produto-categoria
                if current_product['id'] is not None:
                    categorias_produto_batch.append((current_product['id'], cat_id))
                
                # Adiciona hierarquia de categoria (se não for a primeira)
                if j > 0 and previous_cat_id:
                    categorias_hierarquia_batch.add((cat_id, previous_cat_id))
                
                previous_cat_id = cat_id
    
    elif linha.startswith("reviews:"):
        # Processa informações de reviews
        parts = linha.split()
        if len(parts) >= 8:
            # Formato: reviews: total: 5 downloaded: 5 avg rating: 4.0
            pass  # Apenas metadados, os reviews reais vêm nas linhas seguintes
    
    elif len(linha.split()) >= 5 and 'cutomer:' not in linha.lower():
        # Tenta processar como linha de review
        # Formato esperado: 2004-7-23 cutomer: A2UAK8J6GPM0A7 rating: 5 votes: 3 helpful: 2
        try:
            review_parts = linha.split()
            if len(review_parts) >= 9:
                review_data = {
                    'date': review_parts[0],
                    'customer': review_parts[2],
                    'rating': int(review_parts[4]),
                    'votes': int(review_parts[6]),
                    'helpful': int(review_parts[8]),
                    'product_id': current_product['id']
                }
                avaliacoes_batch.append((
                    review_data['product_id'],
                    review_data['date'],
                    review_data['customer'],
                    review_data['rating'],
                    review_data['votes'],
                    review_data['helpful']
                ))
        except (ValueError, IndexError):
            pass
    
    elif linha == "discontinued product":
        # Produto descontinuado
        current_product['ativo'] = False
    
    # Se temos um produto completo, adiciona ao batch de produtos
    if (current_product['id'] is not None and current_product['asin'] is not None and 
        current_product['ativo'] is not None):
        
        produtos_batch.append((
            current_product['id'],
            current_product['asin'],
            current_product['title'],
            current_product['group'],
            current_product['salesrank'],
            current_product['ativo']
        ))
        
        # Limpa o produto atual para o próximo
        current_product = {
            'id': None,
            'asin': None,
            'title': None,
            'group': None,
            'salesrank': None,
            'ativo': True,
            'categorias': set(),
            'reviews': []
        }

def _inserir_batches(conn, produtos_batch, avaliacoes_batch, categorias_batch,
                    categorias_produto_batch, categorias_hierarquia_batch,
                    produtos_similares_batch):
    """Insere todos os batches no banco de dados de forma otimizada"""
    try:
        with conn.cursor() as cur:
            # Insere produtos
            if produtos_batch:
                extras.execute_values(
                    cur,
                    """INSERT INTO Produto (id, asin, titulo, grupo, ranking_vendas, ativo) 
                       VALUES %s ON CONFLICT (asin) DO NOTHING""",
                    produtos_batch,
                    page_size=BATCH_SIZE
                )
            
            # Insere avaliações
            if avaliacoes_batch:
                extras.execute_values(
                    cur,
                    """INSERT INTO Avaliacao (id_produto, data, id_usuario, classificacao, votos, util) 
                       VALUES %s""",
                    avaliacoes_batch,
                    page_size=BATCH_SIZE
                )
            
            # Insere categorias
            if categorias_batch:
                categorias_list = [(cat_id, nome) for cat_id, nome in categorias_batch]
                extras.execute_values(
                    cur,
                    """INSERT INTO Categoria (id, nome) VALUES %s 
                       ON CONFLICT (id) DO NOTHING""",
                    categorias_list,
                    page_size=BATCH_SIZE
                )
            
            # Insere categorias_produto
            if categorias_produto_batch:
                extras.execute_values(
                    cur,
                    """INSERT INTO Categoria_Produto (id_produto, id_categoria) VALUES %s""",
                    categorias_produto_batch,
                    page_size=BATCH_SIZE
                )
            
            # Insere hierarquia de categorias
            if categorias_hierarquia_batch:
                hierarquia_list = list(categorias_hierarquia_batch)
                extras.execute_values(
                    cur,
                    """INSERT INTO Categoria_Hierarquia (id_categoria, id_categoria_pai) VALUES %s""",
                    hierarquia_list,
                    page_size=BATCH_SIZE
                )
            
            # Insere produtos similares
            if produtos_similares_batch:
                extras.execute_values(
                    cur,
                    """INSERT INTO Produto_Similar (id_produto, ains_similar) VALUES %s""",
                    produtos_similares_batch,
                    page_size=BATCH_SIZE
                )
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao inserir batch: {e}")
        raise

def criar_indices_otimizacao(conn):
    """Cria índices para melhorar performance das consultas"""
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_produto_asin ON Produto(asin)",
        "CREATE INDEX IF NOT EXISTS idx_produto_ativo ON Produto(ativo)",
        "CREATE INDEX IF NOT EXISTS idx_avaliacao_produto ON Avaliacao(id_produto)",
        "CREATE INDEX IF NOT EXISTS idx_categoria_produto_categoria ON Categoria_Produto(id_categoria)",
        "CREATE INDEX IF NOT EXISTS idx_categoria_produto_produto ON Categoria_Produto(id_produto)",
        "CREATE INDEX IF NOT EXISTS idx_produto_similar_produto ON Produto_Similar(id_produto)",
    ]
    
    try:
        with conn.cursor() as cur:
            for indice in indices:
                cur.execute(indice)
        conn.commit()
        print("Índices criados com sucesso!")
    except Exception as e:
        print(f"Erro ao criar índices: {e}")
        conn.rollback()

def get_args():
    """Parse dos argumentos de linha de comando"""
    parser = argparse.ArgumentParser(description='Processar dados do Amazon Meta')
    parser.add_argument('--db-host', required=True, help='Host do PostgreSQL')
    parser.add_argument('--db-port', required=True, help='Porta do PostgreSQL')
    parser.add_argument('--db-name', required=True, help='Nome do banco de dados')
    parser.add_argument('--db-user', required=True, help='Usuário do PostgreSQL')
    parser.add_argument('--db-pass', required=True, help='Senha do PostgreSQL')
    parser.add_argument('--snap-input', required=True, help='Caminho para o arquivo de entrada .gz')
    
    return parser.parse_args()

def main():
    """Função principal"""
    args = get_args()
    
    # Conecta e prepara o banco
    conn = conectar_postgresql(args)
    
    try:
        # Cria banco e tabelas
        criar_banco_dados(conn)
        criar_tabelas(conn, Path(__file__).parent.parent / "sql" / "schema.sql")
        
        print("Iniciando processamento do arquivo...")
        
        # Processa o arquivo
        processar_arquivo(conn, args.snap_input)
        
        # Cria índices para otimização
        criar_indices_otimizacao(conn)
        
        print("Processamento concluído com sucesso!")
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()