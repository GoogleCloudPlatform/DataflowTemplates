CREATE TABLE large_pk_table (
    pk_col1 VARCHAR(4096) NOT NULL,
    pk_col2 VARCHAR(4096) NOT NULL,
    pk_col3 VARCHAR(4096) NOT NULL,
    value_col TEXT,
    PRIMARY KEY (pk_col1, pk_col2, pk_col3)
);

CREATE TABLE large_idx_table (
    pk_col BIGINT NOT NULL,
    idx_col1 VARCHAR(4096),
    idx_col2 VARCHAR(4096),
    value_col TEXT,
    PRIMARY KEY (pk_col)
);

CREATE INDEX large_idx_table_idx ON large_idx_table (idx_col1, idx_col2);

INSERT INTO large_pk_table (
    pk_col1, pk_col2, pk_col3, value_col
) VALUES (
    REPEAT('A', 4092),
    REPEAT('B', 4092),
    REPEAT('C', 8),
    'Primary key with size exactly equal to 8192 bytes'
),
(
    REPEAT('A', 4096),
    REPEAT('B', 4096),
    REPEAT('C', 8),
    'Primary key with size greater than 8192 bytes'
);

INSERT INTO large_idx_table (
    pk_col, idx_col1, idx_col2, value_col
) VALUES (
    1,
    REPEAT('A', 4091),
    REPEAT('B', 4091),
    'Index key with size less than or equal to 8192 bytes (including PK size)'
),
(
    2,
    REPEAT('A', 4096),
    REPEAT('B', 4096),
    'Index key with size greater than 8192 bytes (including PK size)'
);
