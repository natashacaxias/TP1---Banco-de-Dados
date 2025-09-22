CREATE TABLE IF NOT EXISTS Produto (
    id BIGINT PRIMARY KEY,
    asin TEXT UNIQUE NOT NULL,
    titulo TEXT,
    grupo TEXT,
    ranking_vendas BIGINT,
    ativo BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS Categoria (
    id BIGINT PRIMARY KEY,
    nome TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Avaliacao (
    id SERIAL PRIMARY KEY,
    id_produto BIGINT NOT NULL,
    data DATE NOT NULL,
    id_usuario TEXT NOT NULL,
    classificacao BIGINT CHECK (classificacao BETWEEN 1 AND 5),
    votos BIGINT DEFAULT 0,
    util BIGINT DEFAULT 0,

    FOREIGN KEY (id_produto) REFERENCES Produto(id)
);

CREATE TABLE IF NOT EXISTS Produto_Similar (
    id SERIAL PRIMARY KEY,
    id_produto BIGINT NOT NULL,
    asin_similar TEXT NOT NULL,

    FOREIGN KEY (id_produto) REFERENCES Produto(id),
    FOREIGN KEY (asin_similar) REFERENCES Produto(asin),
    CONSTRAINT par_unico_similar UNIQUE (id_produto, asin_similar) -- Evita repetições de pares ordenados
);

CREATE TABLE IF NOT EXISTS Categoria_Hierarquia (
    id SERIAL PRIMARY KEY,
    id_categoria BIGINT NOT NULL,
    id_categoria_pai BIGINT NOT NULL,

    FOREIGN KEY (id_categoria) REFERENCES Categoria(id),
    FOREIGN KEY (id_categoria_pai) REFERENCES Categoria(id),

    CONSTRAINT uk_categoria_hierarquia UNIQUE (id_categoria, id_categoria_pai), -- Evita repetições de pares ordenados
    CONSTRAINT chk_diferentes CHECK (id_categoria != id_categoria_pai) -- evita pai se si mesmo
);

CREATE TABLE IF NOT EXISTS Categoria_Produto (
    id SERIAL PRIMARY KEY,
    id_produto BIGINT NOT NULL,
    id_categoria BIGINT NOT NULL,

    FOREIGN KEY (id_produto) REFERENCES Produto(id),
    FOREIGN KEY (id_categoria) REFERENCES Categoria(id),
    CONSTRAINT par_unico_categoria UNIQUE (id_produto, id_categoria) -- Evita repetições de pares
);

-- Índices para Produto
CREATE INDEX IF NOT EXISTS idx_produto_asin ON Produto(asin);
CREATE INDEX IF NOT EXISTS idx_produto_grupo ON Produto(grupo);
CREATE INDEX IF NOT EXISTS idx_produto_ranking ON Produto(ranking_vendas);
CREATE INDEX IF NOT EXISTS idx_produto_ativo ON Produto(ativo);

-- Índices para Avaliacao
CREATE INDEX IF NOT EXISTS idx_avaliacao_produto ON Avaliacao(id_produto);
CREATE INDEX IF NOT EXISTS idx_avaliacao_data ON Avaliacao(data);
CREATE INDEX IF NOT EXISTS idx_avaliacao_classificacao ON Avaliacao(classificacao);
CREATE INDEX IF NOT EXISTS idx_avaliacao_usuario ON Avaliacao(id_usuario);

-- Índices para Categoria_Produto
CREATE INDEX IF NOT EXISTS idx_categoria_produto_categoria ON Categoria_Produto(id_categoria);
CREATE INDEX IF NOT EXISTS idx_categoria_produto_produto ON Categoria_Produto(id_produto);

-- Índices para Produto_Similar
CREATE INDEX IF NOT EXISTS idx_similar_asin ON Produto_Similar(asin_similar);

-- Índices para Categoria_Hierarquia
CREATE INDEX IF NOT EXISTS idx_hierarquia_pai ON Categoria_Hierarquia(id_categoria_pai);