CREATE TABLE LargePrimaryKeyTable (
    pk_col1 STRING(255) NOT NULL,
    pk_col2 STRING(255) NOT NULL,
    pk_col3 STRING(255) NOT NULL,
    value_col STRING(MAX)
)PRIMARY KEY (pk_col1, pk_col2, pk_col3);
