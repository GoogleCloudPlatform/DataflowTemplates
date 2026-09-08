CREATE TABLE LargeKey (
    pk_col1 VARCHAR(255) NOT NULL,
    pk_col2 VARCHAR(255) NOT NULL,
    pk_col3 VARCHAR(255) NOT NULL,
    col1 VARCHAR(255),
    col2 VARCHAR(255),
    col3 VARCHAR(255),
    value_col VARCHAR(MAX),
    PRIMARY KEY (pk_col1, pk_col2, pk_col3)
);

CREATE INDEX large_index ON LargeKey (col1, col2, col3);

INSERT INTO LargeKey VALUES (
    REPLICATE('A', 255),
    REPLICATE('B', 255),
    REPLICATE('C', 255),
    REPLICATE('A', 255),
    REPLICATE('B', 255),
    REPLICATE('C', 255),
    '3072 bytes of total size of table key as per limitation'
);

CREATE TABLE LargeCell (
  id INT PRIMARY KEY,
  max_string_col_to_bytes VARCHAR(MAX),
  max_string_col_to_str VARCHAR(MAX)
);

INSERT INTO LargeCell (id, max_string_col_to_bytes, max_string_col_to_str) VALUES (1, REPLICATE(CAST('b' AS VARCHAR(MAX)), 20971520), NULL);
INSERT INTO LargeCell (id, max_string_col_to_bytes, max_string_col_to_str) VALUES (2, NULL, REPLICATE(CAST('b' AS VARCHAR(MAX)), 2883584));
INSERT INTO LargeCell (id, max_string_col_to_bytes, max_string_col_to_str) VALUES (3, REPLICATE(CAST('a' AS VARCHAR(MAX)), 10485760), REPLICATE(CAST('a' AS VARCHAR(MAX)), 2621440));

CREATE TABLE WideRow (
  id INT PRIMARY KEY,
  col1 VARCHAR(MAX),
  col2 VARCHAR(MAX),
  col3 VARCHAR(MAX)
);
