-- Q1: Top 5 comentários mais úteis e com maior/menor avaliação
WITH ordenado AS (
  SELECT 
    a.id_usuario, a.classificacao, a.votos, a.util, a.data,
    RANK() OVER (ORDER BY a.util DESC, a.classificacao DESC) AS rank_pos,
    RANK() OVER (ORDER BY a.util DESC, a.classificacao ASC) AS rank_neg
  FROM Avaliacao a
  JOIN Produto p ON p.id = a.id_produto
  WHERE p.asin = 'INSIRA_ASIN_AQUI'
)
SELECT id_usuario, classificacao, votos, util, data
FROM ordenado
WHERE rank_pos <= 5 OR rank_neg <= 5
ORDER BY util DESC, classificacao DESC;


-- Q2: Produtos similares com melhor ranking de vendas
SELECT ps.asin_similar, p2.titulo, p2.ranking_vendas
FROM Produto_Similar ps
JOIN Produto p1 ON p1.id = ps.id_produto
JOIN Produto p2 ON p2.asin = ps.asin_similar
WHERE p1.asin = 'INSIRA_ASIN_AQUI'
  AND p2.ranking_vendas < p1.ranking_vendas
ORDER BY p2.ranking_vendas ASC;


-- Q3: Evolução diária das médias de avaliação
SELECT a.data, AVG(a.classificacao) AS media_diaria
FROM Avaliacao a
JOIN Produto p ON p.id = a.id_produto
WHERE p.asin = 'INSIRA_ASIN_AQUI'
GROUP BY a.data
ORDER BY a.data;


-- Q4: Top 10 produtos líderes de venda por grupo
SELECT grupo, asin, titulo, ranking_vendas
FROM (
  SELECT p.*,
         ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY ranking_vendas ASC) AS pos
  FROM Produto p
  WHERE ranking_vendas IS NOT NULL
) sub
WHERE pos <= 10
ORDER BY grupo, pos;


-- Q5: Top 10 produtos com maior média de avaliações úteis positivas
SELECT p.asin, p.titulo,
       AVG(a.util::float / NULLIF(a.votos, 0)) AS media_util
FROM Avaliacao a
JOIN Produto p ON p.id = a.id_produto
GROUP BY p.asin, p.titulo
ORDER BY media_util DESC NULLS LAST
LIMIT 10;


-- Q6: Top 5 categorias com maior média de avaliações úteis por produto
SELECT c.nome,
       AVG(a.util::float / NULLIF(a.votos, 0)) AS media_util
FROM Avaliacao a
JOIN Produto p ON p.id = a.id_produto
JOIN Categoria_Produto cp ON cp.id_produto = p.id
JOIN Categoria c ON c.id = cp.id_categoria
GROUP BY c.nome
ORDER BY media_util DESC NULLS LAST
LIMIT 5;


-- Q7: Top 10 clientes que mais comentaram por grupo de produto
SELECT a.id_usuario, p.grupo, COUNT(*) AS total_comentarios
FROM Avaliacao a
JOIN Produto p ON p.id = a.id_produto
GROUP BY a.id_usuario, p.grupo
ORDER BY total_comentarios DESC
LIMIT 10;
