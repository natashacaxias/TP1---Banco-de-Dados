-- q1: Top 5 comentários mais úteis e com maior avaliação
WITH ordenado AS (
  SELECT 
    a.id_custumer, a.rating, a.votes, a.helpful, a.data,
    RANK() OVER (ORDER BY a.helpful DESC, a.rating DESC) AS rank_pos,
    RANK() OVER (ORDER BY a.helpful DESC, a.rating ASC) AS rank_neg
  FROM Review a
  JOIN Product p ON p.id = a.id_product
  WHERE p.asin = '1559362022'
)
SELECT id_custumer, rating, votes, helpful, data
FROM ordenado
WHERE rank_pos <= 5 
ORDER BY helpful DESC, rating DESC;

-- q1: Top 5 comentários mais úteis e com menor avaliação
WITH ordenado AS (
  SELECT 
    a.id_custumer, a.rating, a.votes, a.helpful, a.data,
    RANK() OVER (ORDER BY a.helpful DESC, a.rating DESC) AS rank_pos,
    RANK() OVER (ORDER BY a.helpful DESC, a.rating ASC) AS rank_neg
  FROM Review a
  JOIN Product p ON p.id = a.id_product
  WHERE p.asin = '1559362022'
)
SELECT id_custumer, rating, votes, helpful, data
FROM ordenado
WHERE rank_neg <= 5
ORDER BY helpful DESC, rating DESC;

-- q2: Produtos similares com melhor ranking de vendas
SELECT ps.asin_similar, p2.title, p2.salesrank
FROM Product_Similar ps
JOIN Product p1 ON p1.id = ps.id_product
JOIN Product p2 ON p2.asin = ps.asin_similar
WHERE p1.asin = '1559362022'
  AND p2.salesrank < p1.salesrank
ORDER BY p2.salesrank ASC;

-- q3: Evolução diária das médias de avaliação
SELECT a.data, AVG(a.rating) AS media_diaria
FROM Review a
JOIN Product p ON p.id = a.id_product
WHERE p.asin = '1559362022'
GROUP BY a.data
ORDER BY a.data;

-- q4: Top 10 produtos líderes de venda por grupo
SELECT grupo, asin, title, salesrank
FROM (
  SELECT p.*,
         ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY salesrank ASC) AS pos
  FROM Product p
  WHERE salesrank IS NOT NULL
) sub
WHERE pos <= 10
ORDER BY grupo, pos;

-- q5: Top 10 produtos com maior média de avaliações úteis positivas
SELECT p.asin, p.title,
       AVG(a.helpful::float / NULLIF(a.votes, 0)) AS media_util
FROM Review a
JOIN Product p ON p.id = a.id_product
GROUP BY p.asin, p.title
ORDER BY media_util DESC NULLS LAST
LIMIT 10;

-- q6: Top 5 categorias com maior média de avaliações úteis por produto
SELECT c.nome,
       AVG(a.helpful::float / NULLIF(a.votes, 0)) AS media_util
FROM Review a
JOIN Product p ON p.id = a.id_product
JOIN Category_Product cp ON cp.id_product = p.id
JOIN Category c ON c.id = cp.id_category
GROUP BY c.nome
ORDER BY media_util DESC NULLS LAST
LIMIT 5;

-- q7: Top 10 clientes que mais comentaram por grupo de produto
SELECT a.id_custumer, p.grupo, COUNT(*) AS total_comentarios
FROM Review a
JOIN Product p ON p.id = a.id_product
GROUP BY a.id_custumer, p.grupo
ORDER BY total_comentarios DESC

LIMIT 10;

