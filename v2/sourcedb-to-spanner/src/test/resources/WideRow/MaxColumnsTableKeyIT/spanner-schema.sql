CREATE TABLE LargePrimaryKeyTable (
    pk_col1 STRING(48) NOT NULL,
    pk_col2 STRING(48) NOT NULL,
    pk_col3 STRING(48) NOT NULL,
    pk_col4 STRING(48) NOT NULL,
    pk_col5 STRING(48) NOT NULL,
    pk_col6 STRING(48) NOT NULL,
    pk_col7 STRING(48) NOT NULL,
    pk_col8 STRING(48) NOT NULL,
    pk_col9 STRING(48) NOT NULL,
    pk_col10 STRING(48) NOT NULL,
    pk_col11 STRING(48) NOT NULL,
    pk_col12 STRING(48) NOT NULL,
    pk_col13 STRING(48) NOT NULL,
    pk_col14 STRING(48) NOT NULL,
    pk_col15 STRING(48) NOT NULL,
    pk_col16 STRING(48) NOT NULL,
    value_col STRING(MAX)
)PRIMARY KEY (
         pk_col1, pk_col2, pk_col3, pk_col4, pk_col5,
         pk_col6, pk_col7, pk_col8, pk_col9, pk_col10,
         pk_col11, pk_col12, pk_col13, pk_col14, pk_col15, pk_col16
     );