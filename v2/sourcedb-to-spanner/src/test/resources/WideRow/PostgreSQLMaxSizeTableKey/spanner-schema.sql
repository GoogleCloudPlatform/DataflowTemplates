CREATE TABLE large_pk_table (
    pk_col1 STRING(4096) NOT NULL,
    pk_col2 STRING(4096) NOT NULL,
    pk_col3 STRING(4096) NOT NULL,
    value_col STRING(MAX),
    PRIMARY KEY (pk_col1, pk_col2, pk_col3)
);

CREATE TABLE large_idx_table (
    pk_col INT64 NOT NULL,
    idx_col1 STRING(4096),
    idx_col2 STRING(4096),
    value_col STRING(MAX),
    PRIMARY KEY (pk_col)
);

CREATE INDEX large_idx_table_idx ON large_idx_table (idx_col1, idx_col2);
