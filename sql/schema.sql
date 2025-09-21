CREATE TABLE IF NOT EXISTS Produto (
    id INT PRIMARY KEY,
    asin VARCHAR(20) UNIQUE NOT NULL,
    titulo VARCHAR(255),
    grupo varchar(50),
    ranking_vendas INT,
    ativo BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS Categoria (
    id INT PRIMARY KEY,
    nome varchar(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Avaliacao (
    id SERIAL PRIMARY KEY,
    id_produto INT NOT NULL,
    data DATE NOT NULL,
    id_usuario INT NOT NULL,
    classificacao INT CHECK (classificacao BETWEEN 1 AND 5),
    votos INT DEFAULT 0,
    util INT DEFAULT 0,

    FOREIGN KEY (id_produto) REFERENCES Produto(id)
);

CREATE TABLE IF NOT EXISTS Produto_Similar (
    id SERIAL PRIMARY KEY,
    id_produto INT NOT NULL,
    asin_similar VARCHAR(20) NOT NULL,

    FOREIGN KEY (id_produto) REFERENCES Produto(id),
    FOREIGN KEY (asin_similar) REFERENCES Produto(asin),
    CONSTRAINT par_unico_similar UNIQUE (id_produto, asin_similar) -- Evita repetições de pares ordenados
);

CREATE TABLE IF NOT EXISTS Categoria_Hierarquia (
    id SERIAL PRIMARY KEY,
    id_categoria INT NOT NULL,
    id_categoria_pai INT NOT NULL,

    FOREIGN KEY (id_categoria) REFERENCES Categoria(id),
    FOREIGN KEY (id_categoria_pai) REFERENCES Categoria(id),

    CONSTRAINT uk_categoria_hierarquia UNIQUE (id_categoria, id_categoria_pai), -- Evita repetições de pares ordenados
    CONSTRAINT chk_diferentes CHECK (id_categoria != id_categoria_pai) -- evita pai se si mesmo
);

CREATE TABLE IF NOT EXISTS Categoria_Produto (
    id SERIAL PRIMARY KEY,
    id_produto INT NOT NULL,
    id_categoria INT NOT NULL,

    FOREIGN KEY (id_produto) REFERENCES Produto(id),
    FOREIGN KEY (id_categoria) REFERENCES Categoria(id),
    CONSTRAINT par_unico_categoria UNIQUE (id_produto, id_categoria) -- Evita repetições de pares
);