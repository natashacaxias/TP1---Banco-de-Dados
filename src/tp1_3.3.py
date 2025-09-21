import argparse
import psycopg
import pandas as pd
import os

def run_query(conn, query, params=None, out_file=None):
    df = pd.read_sql(query, conn, params=params)
    print(df.head(20).to_string(index=False))  # preview no terminal
    if out_file:
        df.to_csv(out_file, index=False)
    return df

def main():
    parser = argparse.ArgumentParser(description="Dashboard - TP1 Bancos de Dados")
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True, type=int)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--product-asin", help="ASIN do produto (necessário para Q1, Q2, Q3)")
    parser.add_argument("--output", default="/app/out", help="Diretório de saída para CSVs")

    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    conn = psycopg.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_pass
    )

    # Q1
    print("\n### Q1 - Top 5 comentários úteis positivos e negativos ###")
    if args.product_asin:
        q1 = """
        (SELECT a.id_usuario, a.classificacao, a.votos, a.util, a.data
         FROM Avaliacao a
         JOIN Produto p ON p.id = a.id_produto
         WHERE p.asin = %s
         ORDER BY a.util DESC, a.classificacao DESC
         LIMIT 5)
        UNION
        (SELECT a.id_usuario, a.classificacao, a.votos, a.util, a.data
         FROM Avaliacao a
         JOIN Produto p ON p.id = a.id_produto
         WHERE p.asin = %s
         ORDER BY a.util DESC, a.classificacao ASC
         LIMIT 5);
        """
        run_query(conn, q1, params=(args.product_asin, args.product_asin),
                  out_file=f"{args.output}/q1_reviews.csv")
    else:
        print("⚠️ --product-asin é necessário para Q1")

    # Q2
    print("\n### Q2 - Produtos similares com melhor ranking de vendas ###")
    if args.product_asin:
        q2 = """
        SELECT ps.asin_similar, p2.ranking_vendas
        FROM Produto_Similar ps
        JOIN Produto p1 ON p1.id = ps.id_produto
        JOIN Produto p2 ON p2.asin = ps.asin_similar
        WHERE p1.asin = %s AND p2.ranking_vendas < p1.ranking_vendas
        ORDER BY p2.ranking_vendas ASC;
        """
        run_query(conn, q2, params=(args.product_asin,),
                  out_file=f"{args.output}/q2_similares.csv")
    else:
        print("⚠️ --product-asin é necessário para Q2")

    # Q3
    print("\n### Q3 - Evolução diária das médias de avaliação ###")
    if args.product_asin:
        q3 = """
        SELECT a.data, AVG(a.classificacao) AS media
        FROM Avaliacao a
        JOIN Produto p ON p.id = a.id_produto
        WHERE p.asin = %s
        GROUP BY a.data
        ORDER BY a.data;
        """
        run_query(conn, q3, params=(args.product_asin,),
                  out_file=f"{args.output}/q3_evolucao.csv")
    else:
        print("⚠️ --product-asin é necessário para Q3")

    # Q4
    print("\n### Q4 - Top 10 produtos líderes de venda por grupo ###")
    q4 = """
    SELECT grupo, asin, titulo, ranking_vendas
    FROM (
        SELECT p.*, 
               ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY ranking_vendas ASC) as pos
        FROM Produto p
        WHERE ranking_vendas IS NOT NULL
    ) sub
    WHERE pos <= 10;
    """
    run_query(conn, q4, out_file=f"{args.output}/q4_top10_sales.csv")

    # Q5
    print("\n### Q5 - Top 10 produtos com maior média de avaliações úteis ###")
    q5 = """
    SELECT p.asin, p.titulo, AVG(a.util::float / NULLIF(a.votos,0)) AS media_util
    FROM Avaliacao a
    JOIN Produto p ON p.id = a.id_produto
    GROUP BY p.asin, p.titulo
    ORDER BY media_util DESC NULLS LAST
    LIMIT 10;
    """
    run_query(conn, q5, out_file=f"{args.output}/q5_top10_helpful.csv")

    # Q6
    print("\n### Q6 - Top 5 categorias com maior média de avaliações úteis ###")
    q6 = """
    SELECT c.nome, AVG(a.util::float / NULLIF(a.votos,0)) AS media_util
    FROM Avaliacao a
    JOIN Produto p ON p.id = a.id_produto
    JOIN Categoria_Produto cp ON cp.id_produto = p.id
    JOIN Categoria c ON c.id = cp.id_categoria
    GROUP BY c.nome
    ORDER BY media_util DESC NULLS LAST
    LIMIT 5;
    """
    run_query(conn, q6, out_file=f"{args.output}/q6_top5_categories.csv")

    # Q7
    print("\n### Q7 - Top 10 clientes que mais comentaram por grupo ###")
    q7 = """
    SELECT a.id_usuario, p.grupo, COUNT(*) AS total
    FROM Avaliacao a
    JOIN Produto p ON p.id = a.id_produto
    GROUP BY a.id_usuario, p.grupo
    ORDER BY total DESC
    LIMIT 10;
    """
    run_query(conn, q7, out_file=f"{args.output}/q7_top10_clients.csv")

    conn.close()
    print("\n✅ Dashboard executado com sucesso.")

if __name__ == "__main__":
    main()

