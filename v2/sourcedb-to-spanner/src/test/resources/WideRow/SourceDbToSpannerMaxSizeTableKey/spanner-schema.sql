CREATE TABLE test_key_size (
    id INT64 NOT NULL,
    col1 STRING(MAX) NOT NULL,
    col2 STRING(MAX) NOT NULL,
    col3 STRING(MAX) NOT NULL,
    col4 STRING(MAX) NOT NULL,
    col5 STRING(MAX) NOT NULL,
    CONSTRAINT pk PRIMARY KEY (id),
    CONSTRAINT idx_large UNIQUE (col1, col2, col3, col4, col5)
);
