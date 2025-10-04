import psycopg2, argparse
from pathlib import Path
import pandas as pd
from sys import stdout

BASE_DIR = Path(__file__).parent.parent

def get_argumentos():
    parser = argparse.ArgumentParser(description='Processar dados do Amazon Meta')
    parser.add_argument('--db-host', required=True)
    parser.add_argument('--db-port', required=True)
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--db-user', required=True)
    parser.add_argument('--db-pass', required=True)
    parser.add_argument('--product-asin', required=True)
    return parser.parse_args()


def conectar_postgree(args):
    
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
        stdout.write("Banco de Dados conectado com sucesso!\n")
        return conn
    except Exception as e:
        stdout.write(f"Erro ao conectar ao BD: {e}\n")
        exit(1)

def executar_consultas(conn, asin, consultas):
    stdout.write("\nDASHBOARD | Consultas SQL\n")

    with open(consultas, 'r') as c:
        linhas = c.read().split(";")
        
        for l in linhas:
            if not l.strip():  # Pular linhas vazias
                continue
                
            aux = l.strip().split("\n")[0]
            stdout.write("\n"+ aux + "\n")
            nome_arquivo = f"{aux.split()[1][:-1]}.csv"
            
            # Usar cursor para executar a consulta
            with conn.cursor() as cur:
                # Executar a consulta
                cur.execute(l, (asin,))
                
                # Obter os resultados
                resultados = cur.fetchall()
                
                # Obter os nomes das colunas
                colunas = [desc[0] for desc in cur.description]
                
                # Converter para DataFrame para salvar em CSV
                df = pd.DataFrame(resultados, columns=colunas)
                df.to_csv(BASE_DIR / "out" / nome_arquivo, index=False, encoding="utf-8")
                
                # Formatar a saída
                stdout.write(("-" * 133) + "\n")
                stdout.write(f"Arquivo salvo: {nome_arquivo}\n")
                stdout.write(f"Total de registros: {len(df)}\n")
                stdout.write(("-" * 133) + "\n")
                
                if len(df) > 0:
                    # Calcular a largura de cada coluna
                    larguras = []
                    for i, coluna in enumerate(colunas):
                        # Largura mínima é o nome da coluna
                        largura = len(coluna)
                        # Verificar o maior valor na coluna
                        for linha in resultados:
                            valor = str(linha[i]) if linha[i] is not None else ""
                            largura = max(largura, len(valor))
                        larguras.append(largura)
                    
                    # Criar a linha de cabeçalho
                    cabecalho = ""
                    for i, coluna in enumerate(colunas):
                        cabecalho += f" {coluna:<{larguras[i]}} |"
                    cabecalho = cabecalho[:-1]  # Remover o último |
                    
                    # Criar a linha separadora
                    separador = ""
                    for largura in larguras:
                        separador += "-" * (largura + 2) + "+"
                    separador = separador[:-1]  # Remover o último +
                    
                    # Imprimir cabeçalho
                    stdout.write(cabecalho + "\n")
                    stdout.write(separador + "\n")
                    
                    # Imprimir cada linha
                    for linha in resultados:
                        linha_str = ""
                        for i, valor in enumerate(linha):
                            valor_str = str(valor) if valor is not None else ""
                            linha_str += f" {valor_str:<{larguras[i]}} |"
                        linha_str = linha_str[:-1]  # Remover o último |
                        stdout.write(linha_str + "\n")
                    
                    # Adicionar contagem de linhas
                    stdout.write(f"({len(df)} rows)\n\n")
                else:
                    stdout.write("Nenhum resultado encontrado\n\n")

def main():
    args = get_argumentos()
    CONN = conectar_postgree(args)
    executar_consultas(CONN, args.product_asin ,BASE_DIR / "sql" / "queries.sql")
    stdout.write("Concluído\n")

if __name__ == "__main__":
    main()