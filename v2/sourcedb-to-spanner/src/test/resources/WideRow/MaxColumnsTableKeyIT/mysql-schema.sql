CREATE TABLE LargePrimaryKeyTable (
    pk_col1 VARCHAR(48) NOT NULL,
    pk_col2 VARCHAR(48) NOT NULL,
    pk_col3 VARCHAR(48) NOT NULL,
    pk_col4 VARCHAR(48) NOT NULL,
    pk_col5 VARCHAR(48) NOT NULL,
    pk_col6 VARCHAR(48) NOT NULL,
    pk_col7 VARCHAR(48) NOT NULL,
    pk_col8 VARCHAR(48) NOT NULL,
    pk_col9 VARCHAR(48) NOT NULL,
    pk_col10 VARCHAR(48) NOT NULL,
    pk_col11 VARCHAR(48) NOT NULL,
    pk_col12 VARCHAR(48) NOT NULL,
    pk_col13 VARCHAR(48) NOT NULL,
    pk_col14 VARCHAR(48) NOT NULL,
    pk_col15 VARCHAR(48) NOT NULL,
    pk_col16 VARCHAR(48) NOT NULL,
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
    REPEAT('A', 48), REPEAT('B', 48), REPEAT('C', 48), REPEAT('D', 48),
    REPEAT('E', 48), REPEAT('F', 48), REPEAT('G', 48), REPEAT('H', 48),
    REPEAT('I', 48), REPEAT('J', 48), REPEAT('K', 48), REPEAT('L', 48),
    REPEAT('M', 48), REPEAT('N', 48), REPEAT('O', 48), REPEAT('P', 48),
    '3072 Bytes of total size of table key'
);