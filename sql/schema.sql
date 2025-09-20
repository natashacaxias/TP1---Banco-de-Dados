CREATE TABLE Produto (
    id INT PRIMARY KEY,
    asin VARCHAR(20) UNIQUE NOT NULL,
    titulo VARCHAR(255),
    grupo varchar(50),
    ranking_vendas INT,
    --status TINYINT(1) DEFAULT 1 -- '1=ativo, 0=descontinuado'
);

CREATE TABLE Categoria (
    id INT PRIMARY KEY,
    nome varchar(50) UNIQUE NOT NULL
);

CREATE TABLE Avaliacao (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_produto INT NOT NULL,
    data DATE NOT NULL,
    id_usuario INT NOT NULL,
    classificacao INT CHECK (classificacao BETWEEN 1 AND 5),
    votos INT DEFAULT 0,
    util INT DEFAULT 0,

    FOREIGN KEY (id_produto) REFERENCES Produto(id)
);

CREATE TABLE Similar (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_produto INT NOT NULL,
    asin_similar VARCHAR(20) NOT NULL,

    FOREIGN KEY (id_produto) REFERENCES Produto(id),
    FOREIGN KEY (asin_similar) REFERENCES Produto(asin),
    CONSTRAINT par_unico UNIQUE (id_produto, asin_similar) -- Evita repetições de pares ordenados
);

CREATE TABLE Categoria_Hierarquia (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_categoria INT NOT NULL,
    id_categoria_pai INT NOT NULL,

    FOREIGN KEY (id_categoria) REFERENCES Categoria(id),
    FOREIGN KEY (id_categoria) REFERENCES Categoria(id),

    CONSTRAINT uk_categoria_hierarquia UNIQUE (id_categoria, id_categoria_pai), -- Evita repetições de pares ordenados
    CONSTRAINT chk_diferentes CHECK (id_categoria != id_categoria_pai) -- evita pai se si mesmo
);

CREATE TABLE Categoria_Produto (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_produto INT NOT NULL,
    id_categoria INT NOT NULL,

    FOREIGN KEY (id_produto) REFERENCES Produto(id),
    FOREIGN KEY (id_categoria) REFERENCES Categoria(id),
    CONSTRAINT par_unico UNIQUE (id_produto, id_categoria) -- Evita repetições de pares
);