CREATE TABLE LargePrimaryKeyTable (
    pk_col1 VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
    pk_col2 VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
    pk_col3 VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
    value_col TEXT,
    PRIMARY KEY (pk_col1, pk_col2, pk_col3)  -- Only 3 columns allowed
);

INSERT INTO LargePrimaryKeyTable (
    pk_col1, pk_col2, pk_col3,value_col
) VALUES (
    REPEAT('A', 255),
    REPEAT('B', 255),
    REPEAT('C', 255),
    '8KB of total size of table key'
);