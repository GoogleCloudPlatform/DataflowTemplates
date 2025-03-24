CREATE TABLE LargePrimaryKeyTable (
    pk_col1 VARCHAR(128) NOT NULL,
    pk_col2 VARCHAR(128) NOT NULL,
    pk_col3 VARCHAR(128) NOT NULL,
    pk_col4 VARCHAR(128) NOT NULL,
    pk_col5 VARCHAR(128) NOT NULL,
    pk_col6 VARCHAR(128) NOT NULL,
    pk_col7 VARCHAR(128) NOT NULL,
    pk_col8 VARCHAR(128) NOT NULL,
    pk_col9 VARCHAR(128) NOT NULL,
    pk_col10 VARCHAR(128) NOT NULL,
    pk_col11 VARCHAR(128) NOT NULL,
    pk_col12 VARCHAR(128) NOT NULL,
    pk_col13 VARCHAR(128) NOT NULL,
    pk_col14 VARCHAR(128) NOT NULL,
    pk_col15 VARCHAR(128) NOT NULL,
    pk_col16 VARCHAR(128) NOT NULL,
    value_col TEXT,
    PRIMARY KEY (
        pk_col1, pk_col2, pk_col3, pk_col4, pk_col5,
        pk_col6, pk_col7, pk_col8, pk_col9, pk_col10,
        pk_col11, pk_col12, pk_col13, pk_col14, pk_col15, pk_col16
    )
);

INSERT INTO LargePrimaryKeyTable (
    pk_col1, pk_col2, pk_col3, pk_col4, pk_col5, pk_col6, pk_col7, pk_col8,
    pk_col9, pk_col10, pk_col11, pk_col12, pk_col13, pk_col14, pk_col15, pk_col16,
    value_col
) VALUES (
    REPEAT('A', 128), REPEAT('B', 128), REPEAT('C', 128), REPEAT('D', 128),
    REPEAT('E', 128), REPEAT('F', 128), REPEAT('G', 128), REPEAT('H', 128),
    REPEAT('I', 128), REPEAT('J', 128), REPEAT('K', 128), REPEAT('L', 128),
    REPEAT('M', 128), REPEAT('N', 128), REPEAT('O', 128), REPEAT('P', 128),
    '8KB of total size of table key'
);