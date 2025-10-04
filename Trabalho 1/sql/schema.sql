CREATE TABLE IF NOT EXISTS Product (
    id INT PRIMARY KEY,
    asin VARCHAR(20) UNIQUE NOT NULL,
    title TEXT,
    grupo TEXT,
    salesrank INT,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS Category (
    id INT PRIMARY KEY,
    nome TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Review (
    id SERIAL PRIMARY KEY,
    id_Product INT NOT NULL,
    data DATE NOT NULL,
    id_custumer VARCHAR(255) NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    votes INT DEFAULT 0,
    helpful INT DEFAULT 0,

    FOREIGN KEY (id_Product) REFERENCES Product(id)
);

CREATE TABLE IF NOT EXISTS Product_Similar (
    id SERIAL PRIMARY KEY,
    id_Product INT NOT NULL,
    asin_similar VARCHAR(20) NOT NULL,

    FOREIGN KEY (id_Product) REFERENCES Product(id),
    FOREIGN KEY (asin_similar) REFERENCES Product(asin),
    CONSTRAINT par_unico_similar UNIQUE (id_Product, asin_similar) -- Evita repetições de pares ordenados
);

CREATE TABLE IF NOT EXISTS Category_hierarchy (
    id SERIAL PRIMARY KEY,
    id_Category INT NOT NULL,
    id_Category_pai INT NOT NULL,

    FOREIGN KEY (id_Category) REFERENCES Category(id),
    FOREIGN KEY (id_Category_pai) REFERENCES Category(id),

    CONSTRAINT uk_Category_hierarquia UNIQUE (id_Category, id_Category_pai), -- Evita repetições de pares ordenados
    CONSTRAINT chk_diferentes CHECK (id_Category != id_Category_pai) -- evita pai se si mesmo
);

CREATE TABLE IF NOT EXISTS Category_Product (
    id SERIAL PRIMARY KEY,
    id_Product INT NOT NULL,
    id_Category INT NOT NULL,

    FOREIGN KEY (id_Product) REFERENCES Product(id),
    FOREIGN KEY (id_Category) REFERENCES Category(id),
    CONSTRAINT par_unico_Category UNIQUE (id_Product, id_Category) -- Evita repetições de pares
);

CREATE VIEW total_avg_reviews AS
SELECT 
    id_product,
    AVG(rating) AS avg_rating,
    COUNT(*) AS total_reviews 
FROM Review
GROUP BY id_Product;

-- Índices para otimização das consultas de tp1_3.3.py

-- Product
CREATE INDEX IF NOT EXISTS idx_Product_asin ON Product(asin);
CREATE INDEX IF NOT EXISTS idx_Product_group ON Product(grupo);
CREATE INDEX IF NOT EXISTS idx_Product_ranking ON Product(salesrank);

-- Review
CREATE INDEX IF NOT EXISTS idx_Review_Product ON Review(id_Product);
CREATE INDEX IF NOT EXISTS idx_Review_data ON Review(data);
CREATE INDEX IF NOT EXISTS idx_Review_rating ON Review(rating);
CREATE INDEX IF NOT EXISTS idx_Review_customer_group ON Review(id_custumer, id_Product);

-- Category_Product
CREATE INDEX IF NOT EXISTS idx_Category_Product_Product ON Category_Product(id_Product);

-- Product_Similar
CREATE INDEX IF NOT EXISTS idx_similar_asin ON Product_Similar(asin_similar);